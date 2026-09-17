import os
import sys
import urllib.parse
from pathlib import Path

# Añadir la raíz del proyecto al PATH para permitir importaciones relativas
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, DateType, TimestampType, BooleanType
)

from config.settings import Settings, load_json_config, validate_batch_config
from src.common.spark import get_spark_session
from src.common.logger import get_logger
from src.common.audit import registrar_auditoria

logger = get_logger(__name__)


def _is_cloud() -> bool:
    """
    Determina si la ejecución actual se realiza en un entorno Cloud (Databricks) o en entorno local.

    Evaluación:
    1. Variable de entorno ENVIRONMENT == 'local'`.
    2. Ajustes globales de `Settings.ENVIRONMENT`.
    3. Existencia de una SparkSession activa propia de Databricks.
    4. Esquema de rutas de almacenamiento remoto (`abfss://` o `dbfs:/`).

    Returns:
        bool: True si el entorno es Cloud/Databricks, False en caso contrario.
    """
    if os.getenv("ENVIRONMENT", "").lower() == "local":
        return False

    if Settings.ENVIRONMENT == "databricks":
        return True

    try:
        from pyspark.sql import SparkSession
        active_session = SparkSession.getActiveSession()
        if active_session:
            return True
    except Exception:
        pass

    return Settings.BASE_BRONZE_PATH.startswith("abfss://") or Settings.BASE_BRONZE_PATH.startswith("dbfs:/")


_TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "boolean": BooleanType(),
}


def _build_schema(expected_schema: list) -> StructType:
    """
    Construye un esquema explícito de PySpark (`StructType`) a partir de una lista
    de definiciones de columnas leída desde el JSON de configuración.

    Args:
        expected_schema (list): Lista de diccionarios con la estructura
            `[{"name": str, "type": str, "nullable": bool}]`.

    Returns:
        StructType: Objeto de esquema nativo de PySpark.
    """
    fields = [
        StructField(c["name"], _TYPE_MAP[c["type"].lower()], c.get("nullable", True))
        for c in expected_schema
    ]
    return StructType(fields)


def read_stream(spark: SparkSession, ingestion_config: dict) -> DataFrame:
    """
    Inicia la lectura en modo Streaming (Auto Loader en Cloud o fuente streaming
    nativa local) para el dataset especificado.

    En Cloud utiliza la fuente `cloudFiles` con evolución de esquema. En entorno local
    aplica la lectura streaming nativa del formato configurado pasándole el esquema explícito.
    Adicionalmente, enriquece el DataFrame con columnas de auditoría técnica (`_ingested_at`,
    `_ingested_filename`).

    Args:
        spark (SparkSession): Sesión de Spark activa.
        ingestion_config (dict): Diccionario con los parámetros de ingesta leídos del JSON.

    Returns:
        DataFrame: DataFrame streaming de PySpark proyectado con metadatos y datos.
    """
    dataset = ingestion_config.get("dataset") or ingestion_config.get("dataset_name")
    source = ingestion_config.get("source", {})
    fmt = source.get("format", ingestion_config.get("format", "cloudFiles"))
    opts = source.get("options", ingestion_config.get("reader_options", {})).copy()

    landing_base = Settings.BASE_LANDING_PATH.rstrip("/").replace("\\", "/")
    raw_base = Settings.BASE_RAW_PATH.rstrip("/").replace("\\", "/")
    checkpoint_base = Settings.BASE_CHECKPOINT_PATH.rstrip("/").replace("\\", "/")

    landing_subdir = ingestion_config.get("landing_subdir", dataset)
    landing_path = f"{landing_base}/{landing_subdir}"
    schema_path = f"{checkpoint_base}/{dataset}_schema"

    expected_schema = ingestion_config.get("expected_schema")
    schema = _build_schema(expected_schema) if expected_schema else None

    if _is_cloud():
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            dbutils.fs.mkdirs(schema_path)
        except Exception:
            pass

    if _is_cloud():
        reader = (
            spark.readStream.format("cloudFiles")
            .options(**opts)
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        )
        df = reader.load(landing_path)
    else:
        file_fmt = opts.get("cloudFiles.format", fmt if fmt != "cloudFiles" else "csv")
        clean_opts = {k: v for k, v in opts.items() if not k.startswith("cloudFiles.")}
        reader = spark.readStream.format(file_fmt).options(**clean_opts)
        if schema:
            reader = reader.schema(schema)
        df = reader.load(landing_path)

    metadata_cols = ["_ingested_at", "_ingested_filename"]
    data_cols = [c for c in df.columns if c not in metadata_cols]

    df = (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "_ingested_filename",
            F.replace(F.input_file_name(), F.lit(landing_base), F.lit(raw_base))
        )
    )

    return df.select(metadata_cols + data_cols)


def write_stream(spark: SparkSession, ingestion_config: dict, df: DataFrame) -> StreamingQuery:
    """
    Configura y ejecuta el proceso de escritura streaming hacia la capa Bronze.

    Utiliza `foreachBatch` para escribir el microbatch actual en la tabla Delta de la capa
    Bronze, registrar opcionalmente la tabla externa en `hive_metastore` (en Cloud) y
    archivar/mover los ficheros procesados desde `Landing` hacia la zona `Raw`.

    Args:
        spark (SparkSession): Sesión de Spark activa.
        ingestion_config (dict): Diccionario con los parámetros de ingesta leídos del JSON.
        df (DataFrame): DataFrame streaming devuelto por `read_stream`.

    Returns:
        StreamingQuery: Objeto de consulta streaming iniciado con el disparador `availableNow=True`.
    """
    datasource = ingestion_config.get("datasource", "farmia")
    dataset = ingestion_config.get("dataset") or ingestion_config.get("dataset_name")
    sink = ingestion_config.get("sink", {})

    bronze_base = Settings.BASE_BRONZE_PATH.rstrip("/").replace("\\", "/")
    checkpoint_base = Settings.BASE_CHECKPOINT_PATH.rstrip("/").replace("\\", "/")
    landing_base = Settings.BASE_LANDING_PATH.rstrip("/").replace("\\", "/")
    raw_base = Settings.BASE_RAW_PATH.rstrip("/").replace("\\", "/")

    bronze_subdir = ingestion_config.get("bronze_subdir", dataset)
    checkpoint_path = f"{checkpoint_base}/{dataset}_checkpoint"
    partition_cols = sink.get("partition_columns", ingestion_config.get("partition_columns", []))
    writer_options = sink.get("options", {"mergeSchema": "true"}).copy()

    dbutils = None
    if _is_cloud():
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            dbutils.fs.mkdirs(checkpoint_path)
            dbutils.fs.mkdirs(f"{raw_base}/{ingestion_config.get('raw_subdir', dataset)}")
        except Exception:
            pass

    def _archive_raw_files(batch_df: DataFrame):
        """Mueve los ficheros procesados de la zona Landing a Raw (DBFS/ADLS o Local)."""
        if "_ingested_filename" in batch_df.columns:
            files = [row["_ingested_filename"] for row in batch_df.select("_ingested_filename").distinct().collect()]
            for raw_file in files:
                if not raw_file:
                    continue
                raw_file_norm = raw_file.replace("\\", "/")
                landing_file = raw_file_norm.replace(raw_base, landing_base)

                if dbutils:
                    try:
                        folder_path = raw_file_norm[0:raw_file_norm.rfind("/") + 1]
                        dbutils.fs.mkdirs(folder_path)
                        dbutils.fs.mv(landing_file, raw_file_norm)
                    except Exception as e:
                        logger.warning(f"No se pudo mover {landing_file} -> {raw_file_norm}: {e}")
                else:
                    import shutil
                    try:
                        clean_landing = urllib.parse.unquote(landing_file).replace("file:///", "").replace("file:/", "").replace("file:", "")
                        clean_raw = urllib.parse.unquote(raw_file_norm).replace("file:///", "").replace("file:/", "").replace("file:", "")

                        path_landing = Path(clean_landing)
                        path_raw = Path(clean_raw)

                        path_raw.parent.mkdir(parents=True, exist_ok=True)
                        if path_landing.exists():
                            shutil.move(str(path_landing), str(path_raw))
                    except Exception as e:
                        logger.warning(f"Error al mover localmente {landing_file}: {e}")

    def _append_2_bronze(batch_df: DataFrame, batch_id: int):
        """Escribe cada microbatch en la capa Bronze Delta y ejecuta el archivado."""
        batch_df.persist()
        try:
            current_bronze_path = f"{bronze_base}/{bronze_subdir}"

            # 1. Escritura en almacenamiento Delta
            writer = batch_df.write.format("delta").mode("append").options(**writer_options)
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(current_bronze_path)

            # 2. Registro en Hive Metastore (solo Cloud)
            if _is_cloud():
                table_simple_name = dataset.replace("-", "_")
                full_hive_table_name = f"hive_metastore.farmia_bronze.{datasource}_{table_simple_name}"

                spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.farmia_bronze")
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {full_hive_table_name}
                    USING DELTA
                    LOCATION '{current_bronze_path}'
                """)

            _archive_raw_files(batch_df)
        finally:
            batch_df.unpersist()

    return (
        df.writeStream
        .foreachBatch(_append_2_bronze)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .queryName(f"bronze-{datasource}-{dataset}")
        .start()
    )


def run_batch_ingestion(config_filename: str):
    """
    Orquesta el flujo completo de ingesta batch para un dataset dado.

    Carga la configuración JSON especificada, valida sus parámetros, ejecuta el
    pipeline streaming/batch con Auto Loader hasta su finalización y registra
    la métrica de la ingesta en el log de auditoría.

    Args:
        config_filename (str): Nombre del archivo de configuración (ej. 'ecommerce_ventas.json').
    """
    config_path = ROOT_DIR / "config" / "batch" / config_filename
    config = load_json_config(config_path)
    validate_batch_config(config)

    dataset_name = config.get("dataset") or config.get("dataset_name")
    spark = get_spark_session()
    logger.info(f"[{dataset_name}] Iniciando ingesta batch con Auto Loader...")

    df = read_stream(spark, config)
    query = write_stream(spark, config, df)
    query.awaitTermination()

    progress = query.lastProgress
    rows = progress["numInputRows"] if progress else 0

    registrar_auditoria(spark, dataset_name, "BATCH", rows, "SUCCESS")
    logger.info(f"[{dataset_name}] Ingesta batch completada con éxito. Registros procesados: {rows}")


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else "ecommerce_ventas.json"
    run_batch_ingestion(config_file)