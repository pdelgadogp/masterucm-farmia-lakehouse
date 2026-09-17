import os
import json
from pathlib import Path
from dotenv import load_dotenv
from src.common.logger import get_logger

logger = get_logger(__name__)

# Localizar la raíz del proyecto para cargar el .env
ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env"

if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
else:
    # En Databricks, las variables de entorno o spark.conf se leen desde la configuración del clúster
    logger.info("Fichero .env no encontrado. Utilizando variables del entorno del sistema / SparkConf.")


class Settings:
    # Entorno de ejecución
    ENVIRONMENT = os.getenv("ENVIRONMENT", "local").lower()
    
    # Credenciales de Azure Storage Account
    AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    AZURE_STORAGE_CLIENT_ID = os.getenv("AZURE_STORAGE_CLIENT_ID")
    AZURE_STORAGE_CLIENT_SECRET = os.getenv("AZURE_STORAGE_CLIENT_SECRET")
    AZURE_STORAGE_TENANT_ID = os.getenv("AZURE_STORAGE_TENANT_ID")
    AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "farmia-data")
    
    # Apache Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    @classmethod
    def _get_path(cls, env_var: str, spark_conf_key: str, default_relative: str) -> str:
        """
        Lee una ruta intentando primero de la configuración activa de Spark (SparkConf),
        después de variables de entorno y finalmente usando la ruta relativa del proyecto.
        """
        try:
            from pyspark.sql import SparkSession
            active_session = SparkSession.getActiveSession()
            if active_session:
                val = active_session.conf.get(spark_conf_key, None)
                if val:
                    return val
        except Exception:
            pass

        return os.getenv(env_var, str(ROOT_DIR / default_relative))

    @classmethod
    def get_azure_credentials(cls) -> dict:
        """Devuelve un diccionario con las credenciales de Azure de forma estructurada."""
        return {
            "account_name": cls.AZURE_STORAGE_ACCOUNT_NAME,
            "client_id": cls.AZURE_STORAGE_CLIENT_ID,
            "client_secret": cls.AZURE_STORAGE_CLIENT_SECRET,
            "tenant_id": cls.AZURE_STORAGE_TENANT_ID,
            "container": cls.AZURE_CONTAINER_NAME
        }

    @property
    def BASE_LANDING_PATH(self) -> str:
        return self._get_path("BASE_LANDING_PATH", "spark.farmia.base_landing_path", "data/landing")

    @property
    def BASE_RAW_PATH(self) -> str:
        return self._get_path("BASE_RAW_PATH", "spark.farmia.base_raw_path", "data/raw")

    @property
    def BASE_BRONZE_PATH(self) -> str:
        return self._get_path("BASE_BRONZE_PATH", "spark.farmia.base_bronze_path", "data/bronze")

    @property
    def BASE_CHECKPOINT_PATH(self) -> str:
        return self._get_path("BASE_CHECKPOINT_PATH", "spark.farmia.base_checkpoint_path", "data/checkpoints")


# Instancia de la clase Settings
Settings = Settings()


def load_json_config(file_path: Path) -> dict:
    """Carga y valida sintácticamente un archivo de configuración JSON."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Fichero de configuración corrupto o inválido: {file_path} - {e}")
    except FileNotFoundError:
        raise FileNotFoundError(f"Fichero de configuración no encontrado: {file_path}")


def validate_config(config: dict, required_keys: list) -> bool:
    """Valida los campos obligatorios en un diccionario de configuración."""
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise KeyError(f"Campos faltantes en el archivo de configuración '{config.get('dataset_name', '(desconocido)')}': {missing}")
    return True


BATCH_REQUIRED_KEYS = [
    "dataset_name", "format", "expected_schema",
    "landing_subdir", "bronze_table", "partition_columns"
]

STREAMING_REQUIRED_KEYS = [
    "dataset_name", "message_format", "topic_pattern",
    "bronze_table", "partition_columns"
]


def validate_batch_config(config: dict) -> bool:
    """Valida la configuración para un dataset batch de Autoloader."""
    return validate_config(config, BATCH_REQUIRED_KEYS)


def validate_streaming_config(config: dict) -> bool:
    """Valida la configuración para un dataset en streaming de Kafka."""
    return validate_config(config, STREAMING_REQUIRED_KEYS)