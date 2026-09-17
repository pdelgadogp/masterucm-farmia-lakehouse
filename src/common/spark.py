import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from src.common.logger import get_logger

# Importación global para que esté disponible tanto en Local como en Databricks
from src.common.audit_listener import AuditStreamingListener

logger = get_logger(__name__)


def get_spark_session() -> SparkSession:
    """
    Obtiene la sesión activa de Spark o inicializa una nueva adaptada
    al entorno (local con soporte Delta Lake + Kafka, o Databricks).
    """
    try:
        active_session = SparkSession.getActiveSession()
        if active_session:
            env = os.getenv("ENVIRONMENT", "").lower()
            if env == "local":
                logger.info("Reutilizando sesión activa de PySpark local.")
            else:
                logger.info("Sesión de Spark activa detectada. Ejecutando en entorno Databricks.")
            return active_session
    except Exception:
        pass

    env = os.getenv("ENVIRONMENT", "").lower()

    # ==========================================
    # 1. ENTORNO LOCAL
    # ==========================================
    if env == "local":
        logger.info("Levantando entorno local. Inicializando PySpark con Delta Lake y Kafka...")
        
        builder = (
            SparkSession.builder.appName("FarmIA-Local-Streaming")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.driver.host", "127.0.0.1")
        )
        
        try:
            from delta import configure_spark_with_delta_pip
            spark = configure_spark_with_delta_pip(
                builder, 
                extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"]
            ).getOrCreate()
        except ImportError:
            builder = builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            spark = builder.getOrCreate()

        # Registrar el listener en el entorno local
        spark.streams.addListener(AuditStreamingListener(spark))
        return spark

    # ==========================================
    # 2. ENTORNO CLOUD (DATABRICKS)
    # ==========================================
    logger.info("Inicializando SparkSession para entorno Databricks...")
    spark = SparkSession.builder.getOrCreate()
    
    # Registrar el listener de auditoría en la nube
    try:
        spark.streams.addListener(AuditStreamingListener(spark))
    except Exception as e:
        logger.warning(f"No se pudo registrar el listener en Databricks: {e}")

    return spark