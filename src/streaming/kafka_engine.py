import os
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from config.settings import Settings, load_json_config, validate_streaming_config
from src.common.spark import get_spark_session
from src.common.logger import get_logger
from src.common.audit import registrar_auditoria

logger = get_logger(__name__)


def _is_cloud() -> bool:
    """Detecta si la ejecución corre sobre un entorno Databricks."""
    if os.getenv("ENVIRONMENT", "").lower() == "local":
        return False
    if Settings.ENVIRONMENT == "databricks":
        return True
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession() is not None
    except Exception:
        return False


def read_stream(spark: SparkSession, ingestion_config: dict) -> DataFrame:
    """
    Lee streaming desde Apache Kafka (Confluent Cloud), deserializa el payload JSON
    y expande el esquema en columnas raíz.
    """
    source = ingestion_config.get("source", {})
    opts = source.get("options", {}).copy()

    # 1. Recuperar credenciales con fallback prioritario a spark.conf (Databricks)
    bootstrap_server = os.getenv("CONFLUENT_BOOTSTRAP_SERVER")
    api_key = os.getenv("CONFLUENT_API_KEY")
    api_secret = os.getenv("CONFLUENT_API_SECRET")

    if not bootstrap_server and spark:
        try:
            bootstrap_server = spark.conf.get("spark.env.CONFLUENT_BOOTSTRAP_SERVER", None)
            api_key = spark.conf.get("spark.env.CONFLUENT_API_KEY", None)
            api_secret = spark.conf.get("spark.env.CONFLUENT_API_SECRET", None)
        except Exception:
            pass

    # 2. Inyectar en las opciones del conector de Kafka
    opts["kafka.bootstrap.servers"] = bootstrap_server or Settings.KAFKA_BOOTSTRAP_SERVERS
    opts["kafka.security.protocol"] = "SASL_SSL"
    opts["kafka.sasl.mechanism"] = "PLAIN"
    
    # Detección dinámica de la clase LoginModule para Databricks (shading)
    try:
        spark._jvm.java.lang.Class.forName("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule")
        login_module_class = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
    except Exception:
        login_module_class = "org.apache.kafka.common.security.plain.PlainLoginModule"

    opts["kafka.sasl.jaas.config"] = (
        f'{login_module_class} required '
        f'username="{api_key}" password="{api_secret}";'
    )

    if "subscribePattern" not in opts and "subscribe" not in opts:
        topic_pattern = ingestion_config.get("topic_pattern")
        if topic_pattern:
            opts["subscribePattern"] = topic_pattern

    value_format = source.get("value_format", ingestion_config.get("message_format", "json"))

    df_raw = (
        spark.readStream
        .format("kafka")
        .options(**opts)
        .load()
    )

    if value_format == "json":
        json_schema = source.get("json_schema")
        if json_schema:
            df_parsed = df_raw.withColumn("parsed_value", F.from_json(F.col("value").cast("string"), json_schema))
            df_body = df_parsed.select("parsed_value.*", "topic", "partition", "offset")
        else:
            df_body = df_raw.select(F.col("value").cast("string").alias("value"), "topic", "partition", "offset")
    else:
        df_body = df_raw.select(F.col("value").cast("string").alias("value"), "topic", "partition", "offset")

    return (
        df_body
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_kafka_topic", F.col("topic"))
        .withColumn("_kafka_partition", F.col("partition"))
        .withColumn("_kafka_offset", F.col("offset"))
        .drop("topic", "partition", "offset")
    )


def write_stream(spark: SparkSession, ingestion_config: dict, df: DataFrame) -> StreamingQuery:
    """Escribe los datos streaming a la capa Bronze en Delta Lake."""
    datasource = ingestion_config.get("datasource", "farmia")
    dataset = ingestion_config.get("dataset") or ingestion_config.get("dataset_name")
    sink = ingestion_config.get("sink", {})

    # Recuperar rutas base (spark.conf para Databricks / Settings para local)
    if _is_cloud():
        try:
            base_bronze = spark.conf.get("spark.farmia.base_bronze_path")
            base_checkpoint = spark.conf.get("spark.farmia.base_checkpoint_path")
        except Exception:
            base_bronze = Settings.BASE_BRONZE_PATH
            base_checkpoint = Settings.BASE_CHECKPOINT_PATH
    else:
        base_bronze = Settings.BASE_BRONZE_PATH
        base_checkpoint = Settings.BASE_CHECKPOINT_PATH

    bronze_subdir = ingestion_config.get("bronze_subdir", dataset)
    bronze_path = sink.get("path") or f"{base_bronze.rstrip('/')}/{bronze_subdir}"
    checkpoint_path = f"{base_checkpoint.rstrip('/')}/{dataset}_checkpoint"
    partition_cols = sink.get("partition_columns", ingestion_config.get("partition_columns", []))

    opts = sink.get("options", {"mergeSchema": "true"}).copy()

    writer = (
        df.writeStream
        .format("delta")
        .options(**opts)
        .option("checkpointLocation", checkpoint_path)
        .queryName(f"{datasource}_{dataset}_streaming")
    )

    trigger_config = ingestion_config.get("trigger", {})
    if trigger_config.get("processingTime"):
        writer = writer.trigger(processingTime=trigger_config["processingTime"])
    else:
        writer = writer.trigger(availableNow=True)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    return writer.start(bronze_path)

def run_streaming_ingestion(config_filename: str):
    """Carga configuración, inicializa el flujo streaming y procesa de forma incremental o continua."""
    config_path = ROOT_DIR / "config" / "streaming" / config_filename
    config = load_json_config(config_path)
    validate_streaming_config(config)

    dataset_name = config.get("dataset") or config.get("dataset_name")
    spark = get_spark_session()
    logger.info(f"[{dataset_name}] Iniciando ingesta streaming desde Confluent Cloud...")

    try:
        df = read_stream(spark, config)
        query = write_stream(spark, config, df)
        logger.info(f"[{dataset_name}] Consulta Streaming activa ('{query.name}'). Procesando...")
        
        # 1. Esperar la finalización del lote incremental
        query.awaitTermination()
        
        # 2. Obtener métricas del micro-lote
        last_progress = query.lastProgress
        rows_read = last_progress.get("numInputRows", 0) if last_progress else 0

        # 3. Registrar el estado SUCCESS
        registrar_auditoria(
            spark=spark,
            dataset_name=dataset_name,
            execution_type="streaming",
            records_processed=rows_read,
            status="SUCCESS",
            error_message=f"Query completed successfully: {query.id}",
            batch_id="batch_0"
        )

        logger.info(f"[{dataset_name}] Control de ingesta registrado (SUCCESS). Filas procesadas: {rows_read}.")
        logger.info(f"[{dataset_name}] Ingesta incremental completada con éxito.")

        time.sleep(1)

    except KeyboardInterrupt:
        logger.info(f"[{dataset_name}] Deteniendo el streaming...")
        if 'query' in locals() and query.isActive:
            query.stop()
    except Exception as e:
        logger.error(f"[{dataset_name}] Error en streaming de Kafka: {e}")
        raise


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else "eventos_clientes.json"
    run_streaming_ingestion(config_file)