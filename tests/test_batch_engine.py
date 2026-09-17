"""Tests unitarios de src/batch/autoloader_engine.py.

El motor recibe mocks de SparkSession/DataFrame. La fixture spark_session
no se usa directamente en estos tests: pyspark.sql.functions
(F.current_timestamp, F.col...) delega en la JVM y necesita un
SparkContext activo para no lanzar AssertionError.
"""

import importlib
import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import config.settings as settings_module
import src.batch.autoloader_engine as engine
import src.common.audit as audit_module
import src.common.spark as spark_module
from config.settings import Settings


# ---------------------------------------------------------------------------
# Detección de entorno (_is_cloud)
# ---------------------------------------------------------------------------

def test_is_cloud_false_when_environment_variable_is_local(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setattr(Settings, "ENVIRONMENT", "databricks")
    assert engine._is_cloud() is False


def test_is_cloud_true_when_settings_is_databricks(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setattr(Settings, "ENVIRONMENT", "databricks")
    with patch("pyspark.sql.SparkSession.getActiveSession", return_value=None):
        assert engine._is_cloud() is True


def test_is_cloud_detects_abfss_paths_when_no_active_session(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    with patch("pyspark.sql.SparkSession.getActiveSession", side_effect=Exception("No session")):
        class FakeSettingsCloud:
            ENVIRONMENT = "databricks"
            BASE_BRONZE_PATH = "abfss://lake/bronze"
            KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

        monkeypatch.setattr(engine, "Settings", FakeSettingsCloud)
        assert engine._is_cloud() is True

        class FakeSettingsOnPremise:
            ENVIRONMENT = "produccion"
            BASE_BRONZE_PATH = "C:/lake/bronze"

        monkeypatch.setattr(engine, "Settings", FakeSettingsOnPremise)
        assert engine._is_cloud() is False


def test_is_cloud_detects_active_spark_session(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setattr(engine.Settings, "ENVIRONMENT", "produccion")
    with patch("pyspark.sql.SparkSession.getActiveSession", return_value=MagicMock()):
        assert engine._is_cloud() is True


# ---------------------------------------------------------------------------
# Import del módulo
# ---------------------------------------------------------------------------

def test_module_import_adds_root_dir_to_sys_path():
    root_dir = str(engine.ROOT_DIR)
    original_path = sys.path[:]
    if root_dir in sys.path:
        sys.path.remove(root_dir)
    try:
        importlib.reload(engine)
        assert root_dir in sys.path
    finally:
        sys.path[:] = original_path


# ---------------------------------------------------------------------------
# read_stream / write_stream
# ---------------------------------------------------------------------------

def test_read_and_write_stream_in_cloud_mode(spark_session, monkeypatch):
    """Flujo cloud: cloudFiles, dbutils virtualizado y Hive Metastore."""
    monkeypatch.setattr(engine, "_is_cloud", lambda: True)

    dbutils_module = ModuleType("pyspark.dbutils")
    dbutils_cls = MagicMock()
    dbutils_module.DBUtils = dbutils_cls

    with patch.dict(sys.modules, {"pyspark.dbutils": dbutils_module}):
        spark = MagicMock()
        reader = MagicMock()
        spark.readStream.format.return_value = reader
        reader.options.return_value = reader
        reader.option.return_value = reader

        df = MagicMock()
        reader.load.return_value = df
        df.columns = ["col1", "_ingested_at"]
        df.withColumn.return_value = df
        df.select.return_value = df

        config = {
            "dataset": "ecommerce_ventas",
            "source": {"format": "csv", "options": {}},
            "sink": {"partition_columns": ["fecha"]},
            "raw_subdir": "ecommerce",
        }

        assert engine.read_stream(spark, config) is not None

        query = MagicMock()
        writer = MagicMock()
        spark.sql = MagicMock()
        df.writeStream.foreachBatch.return_value = writer
        writer.option.return_value = writer
        writer.trigger.return_value = writer
        writer.queryName.return_value = writer
        writer.start.return_value = query

        def fake_foreach_batch(fn):
            batch_df = MagicMock()
            batch_df.columns = ["_ingested_filename"]
            batch_df.select.return_value.distinct.return_value.collect.return_value = [
                {"_ingested_filename": "/path/raw/file.csv"}
            ]
            fn(batch_df, 0)
            return writer

        df.writeStream.foreachBatch.side_effect = fake_foreach_batch

        assert engine.write_stream(spark, config, df) is not None


def test_read_and_write_stream_survive_missing_dbutils(spark_session, monkeypatch):
    """Sin dbutils disponible, los errores de import se toleran."""
    monkeypatch.setattr(engine, "_is_cloud", lambda: True)

    with patch.dict(sys.modules):
        sys.modules.pop("pyspark.dbutils", None)

        spark = MagicMock()
        config = {"dataset": "test_dbutils", "source": {}, "sink": {}}

        engine.read_stream(spark, config)

        df = MagicMock()
        df.writeStream.foreachBatch.return_value.option.return_value.trigger.return_value \
            .queryName.return_value.start.return_value = MagicMock()
        engine.write_stream(spark, config, df)


def test_write_stream_survives_local_archive_failures(spark_session, monkeypatch):
    """El archivado Landing -> Raw en local no rompe el stream aunque
    shutil.move falle o el nombre de fichero esté vacío."""
    monkeypatch.setattr(engine, "_is_cloud", lambda: False)

    spark = MagicMock()
    df = MagicMock()
    writer = MagicMock()
    df.writeStream.foreachBatch.return_value = writer

    config = {"dataset": "test_exceptions", "source": {}, "sink": {}}

    def fake_foreach_batch(fn):
        batch_df = MagicMock()
        batch_df.columns = ["_ingested_filename"]
        batch_df.select.return_value.distinct.return_value.collect.return_value = [
            {"_ingested_filename": ""},  # nombre vacío: se ignora
            {"_ingested_filename": "file:///C:/fake/landing/file.csv"},
        ]
        with patch("src.batch.autoloader_engine.Path.exists", return_value=True), \
             patch("shutil.move", side_effect=Exception("Local move error")):
            fn(batch_df, 0)
        return writer

    df.writeStream.foreachBatch.side_effect = fake_foreach_batch
    writer.option.return_value.trigger.return_value.queryName.return_value.start.return_value = MagicMock()

    engine.write_stream(spark, config, df)


def test_write_stream_survives_cloud_archive_failures(spark_session, monkeypatch):
    """Un fallo de dbutils.fs.mv no debe tumbar el stream en cloud."""
    monkeypatch.setattr(engine, "_is_cloud", lambda: True)

    dbutils_module = ModuleType("pyspark.dbutils")
    dbutils_cls = MagicMock()
    dbutils_instance = MagicMock()
    dbutils_instance.fs.mv.side_effect = Exception("Fallo forzado en cloud")
    dbutils_cls.return_value = dbutils_instance
    dbutils_module.DBUtils = dbutils_cls

    with patch.dict(sys.modules, {"pyspark.dbutils": dbutils_module}):
        spark = MagicMock()
        df = MagicMock()
        writer = MagicMock()

        config = {"dataset": "test", "source": {}, "sink": {}}

        def fake_foreach_batch(fn):
            batch_df = MagicMock()
            batch_df.columns = ["_ingested_filename"]
            batch_df.select.return_value.distinct.return_value.collect.return_value = [
                {"_ingested_filename": "abfss://lake/landing/file.csv"}
            ]
            fn(batch_df, 0)
            return writer

        df.writeStream.foreachBatch.side_effect = fake_foreach_batch

        engine.write_stream(spark, config, df)


# ---------------------------------------------------------------------------
# Bloque __main__
# ---------------------------------------------------------------------------

def test_main_block_runs_batch_ingestion(spark_session, monkeypatch):
    """Ejecuta el bloque __main__ del módulo con las dependencias mockeadas."""
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setattr(settings_module, "load_json_config",
                        lambda path: {"dataset": "test", "source": {}, "sink": {}})
    monkeypatch.setattr(settings_module, "validate_batch_config", lambda config: True)
    monkeypatch.setattr(spark_module, "get_spark_session", lambda: MagicMock())
    monkeypatch.setattr(audit_module, "registrar_auditoria", MagicMock())

    with patch.object(sys, "argv", ["autoloader_engine.py", "mock_config.json"]):
        with open(engine.__file__, "r", encoding="utf-8") as f:
            code = f.read()

        namespace = engine.__dict__.copy()
        namespace["__name__"] = "__main__"
        exec(compile(code, engine.__file__, "exec"), namespace)