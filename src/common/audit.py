from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from config.settings import Settings
from src.common.logger import get_logger

logger = get_logger(__name__)

INGESTION_CONTROL_SCHEMA = StructType([
    StructField("pipeline_name", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("batch_id", StringType(), True),
    StructField("ingest_ts", TimestampType(), False),
    StructField("rows_read", LongType(), True),
    StructField("rows_written", LongType(), True),
    StructField("status", StringType(), False),
    StructField("notes", StringType(), True),
])


def registrar_auditoria(spark: SparkSession, dataset_name: str, execution_type: str,
                        records_processed: int, status: str, error_message: str = None,
                        batch_id: str = "batch_0"):
    """Registra el control de ingesta en tabla Delta Lake"""
    control_path = f"{Settings.BASE_BRONZE_PATH}/ingestion_control"
    data = [(
        f"farmia_{execution_type.lower()}_pipeline",
        dataset_name,
        str(batch_id),
        datetime.now(),
        int(records_processed),
        int(records_processed) if status.upper() == "SUCCESS" else 0,
        status.upper(),
        error_message or "OK"
    )]

    try:
        df_control = spark.createDataFrame(data, schema=INGESTION_CONTROL_SCHEMA)
        df_control.write.format("delta").mode("append").save(control_path)
        logger.info(f"Control de ingesta registrado para '{dataset_name}' ({status.upper()})")
    except Exception as e:
        logger.error(f"Error al registrar control de ingesta de '{dataset_name}': {e}")