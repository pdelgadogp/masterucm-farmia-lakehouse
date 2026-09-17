"""Fixtures y datos de prueba compartidos por toda la suite."""

from types import SimpleNamespace

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config.settings import Settings

# Datos del dataset ecommerce_ventas, compartidos por los tests de
# integración del motor batch y del orquestador.
ORDERS_CSV = (
    "order_id,cliente_id,producto_id,cantidad,precio_unitario,total_pedido,fecha_pedido,canal_venta\n"
    "ORD-001,CLI-001,PROD-001,2,10.5,21.0,2026-09-10,web\n"
)

ECOMMERCE_VENTAS_SCHEMA = [
    {"name": "order_id", "type": "string", "nullable": False},
    {"name": "cliente_id", "type": "string", "nullable": False},
    {"name": "producto_id", "type": "string", "nullable": False},
    {"name": "cantidad", "type": "integer", "nullable": True},
    {"name": "precio_unitario", "type": "double", "nullable": True},
    {"name": "total_pedido", "type": "double", "nullable": True},
    {"name": "fecha_pedido", "type": "date", "nullable": False},
    {"name": "canal_venta", "type": "string", "nullable": True},
]


@pytest.fixture
def local_paths(tmp_path, monkeypatch):
    """Redirige las rutas base de Settings a directorios temporales
    y fuerza el entorno local."""
    paths = SimpleNamespace(
        landing=tmp_path / "landing",
        raw=tmp_path / "raw",
        bronze=tmp_path / "bronze",
        checkpoints=tmp_path / "checkpoints",
    )
    monkeypatch.setattr(Settings, "ENVIRONMENT", "local")
    monkeypatch.setattr(type(Settings), "BASE_LANDING_PATH", property(lambda self: str(paths.landing)))
    monkeypatch.setattr(type(Settings), "BASE_RAW_PATH", property(lambda self: str(paths.raw)))
    monkeypatch.setattr(type(Settings), "BASE_BRONZE_PATH", property(lambda self: str(paths.bronze)))
    monkeypatch.setattr(type(Settings), "BASE_CHECKPOINT_PATH", property(lambda self: str(paths.checkpoints)))
    return paths


@pytest.fixture
def ecommerce_ventas_layout(local_paths):
    """Crea la estructura de directorios de ecommerce_ventas y deja
    el CSV de landing listo para procesar."""
    dirs = SimpleNamespace(
        landing=local_paths.landing / "ecommerce_ventas",
        raw=local_paths.raw / "ecommerce_ventas",
        bronze=local_paths.bronze / "ecommerce_ventas",
        checkpoints=local_paths.checkpoints / "ecommerce_ventas_checkpoint",
    )
    for directory in (dirs.landing, dirs.raw, dirs.bronze, dirs.checkpoints):
        directory.mkdir(parents=True, exist_ok=True)
    (dirs.landing / "orders.csv").write_text(ORDERS_CSV, encoding="utf-8")
    return dirs


@pytest.fixture
def ecommerce_ventas_config(local_paths):
    """Configuración batch estándar del dataset ecommerce_ventas."""
    return {
        "datasource": "farmia",
        "dataset": "ecommerce_ventas",
        "dataset_name": "ecommerce_ventas",
        "source": {
            "format": "csv",
            "options": {"header": "true", "sep": ","},
            "path": str(local_paths.landing / "ecommerce_ventas"),
        },
        "sink": {
            "path": str(local_paths.bronze / "ecommerce_ventas"),
            "options": {"mergeSchema": "true"},
            "partition_columns": ["fecha_pedido"],
        },
        "landing_subdir": "ecommerce_ventas",
        "raw_subdir": "ecommerce_ventas",
        "bronze_subdir": "ecommerce_ventas",
        "partition_columns": ["fecha_pedido"],
        "expected_schema": ECOMMERCE_VENTAS_SCHEMA,
    }


@pytest.fixture(scope="session")
def spark_session():
    """SparkSession local con Delta, compartida por toda la suite.

    Se crea una única vez y vive hasta el final de la ejecución:
    - Los tests de integración la usan como sesión real.
    - Los tests unitarios con mocks la solicitan únicamente para que
      exista un SparkContext activo: las funciones de
      pyspark.sql.functions (F.col, F.current_timestamp, F.from_json...)
      delegan en la JVM y fallan sin contexto activo.

    Ningún test debe pararla: detenerla a mitad de ejecución rompería
    los tests posteriores que dependen del contexto.
    """
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("test-suite")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()