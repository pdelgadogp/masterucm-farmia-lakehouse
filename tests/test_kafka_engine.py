"""Tests unitarios de src/streaming/kafka_engine.py.

El motor recibe mocks de SparkSession/DataFrame. La fixture spark_session
solo garantiza un SparkContext activo para que pyspark.sql.functions
(F.col, F.from_json...) pueda ejecutarse.
"""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

import src.streaming.kafka_engine as engine
from config.settings import Settings


# ---------------------------------------------------------------------------
# Detección de entorno (_is_cloud)
# ---------------------------------------------------------------------------

def test_is_cloud_false_when_environment_variable_is_local(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setattr(Settings, "ENVIRONMENT", "databricks")
    assert engine._is_cloud() is False


def test_is_cloud_detects_abfss_paths_when_no_active_session(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    class FakeSettingsCloud:
        ENVIRONMENT = "databricks"
        BASE_BRONZE_PATH = "abfss://lake/bronze"
        BASE_CHECKPOINT_PATH = "abfss://lake/checkpoints"
        KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

    monkeypatch.setattr(engine, "Settings", FakeSettingsCloud)
    with patch("pyspark.sql.SparkSession.getActiveSession", side_effect=Exception("no active session")):
        assert engine._is_cloud() is True


# ---------------------------------------------------------------------------
# read_stream / write_stream
# ---------------------------------------------------------------------------

def test_read_stream_with_json_schema(spark_session, monkeypatch):
    monkeypatch.setenv("CONFLUENT_BOOTSTRAP_SERVER", "localhost:9092")
    monkeypatch.setenv("CONFLUENT_API_KEY", "dummy_key")
    monkeypatch.setenv("CONFLUENT_API_SECRET", "dummy_secret")

    spark = MagicMock()
    raw_df = MagicMock()
    spark.readStream.format.return_value.options.return_value.load.return_value = raw_df
    parsed_df = MagicMock()
    raw_df.withColumn.return_value = parsed_df
    parsed_df.select.return_value = parsed_df
    parsed_df.drop.return_value = parsed_df

    schema = StructType([StructField("id", StringType(), True)])
    config = {
        "source": {
            "format": "kafka",
            "value_format": "json",
            "json_schema": schema,
            "options": {"subscribe": "test_topic"},
        }
    }

    assert engine.read_stream(spark, config) is not None


def test_read_stream_falls_back_to_spark_conf(spark_session, monkeypatch):
    monkeypatch.delenv("CONFLUENT_BOOTSTRAP_SERVER", raising=False)
    monkeypatch.delenv("CONFLUENT_API_KEY", raising=False)
    monkeypatch.delenv("CONFLUENT_API_SECRET", raising=False)

    spark = MagicMock()
    spark.conf.get.side_effect = lambda key, default: "cloud_conf_value"

    config = {
        "source": {
            "format": "kafka",
            "value_format": "raw",
            "options": {"subscribe": "test_topic"},
        }
    }

    assert engine.read_stream(spark, config) is not None


def test_read_stream_survives_missing_conf_and_uses_topic_pattern(spark_session, monkeypatch):
    monkeypatch.delenv("CONFLUENT_BOOTSTRAP_SERVER", raising=False)
    monkeypatch.delenv("CONFLUENT_API_KEY", raising=False)
    monkeypatch.delenv("CONFLUENT_API_SECRET", raising=False)

    spark = MagicMock()
    spark.conf.get.side_effect = Exception("no conf available")
    spark._jvm.java.lang.Class.forName.side_effect = Exception("no shaded class")

    raw_df = MagicMock()
    spark.readStream.format.return_value.options.return_value.load.return_value = raw_df
    raw_df.select.return_value = raw_df
    raw_df.withColumn.return_value = raw_df
    raw_df.drop.return_value = raw_df

    config = {
        "source": {"format": "kafka", "value_format": "raw", "options": {}},
        "topic_pattern": "topic_test_.*",
    }

    assert engine.read_stream(spark, config) is not None


def test_read_and_write_stream_in_cloud_mode(spark_session, monkeypatch):
    monkeypatch.setattr(engine, "_is_cloud", lambda: True)

    spark = MagicMock()
    spark.conf.get.side_effect = Exception("Conf not found")

    config = {
        "dataset": "sensores_iot",
        "source": {"format": "kafka", "value_format": "json", "options": {}},
        "sink": {},
    }
    assert engine.read_stream(spark, config) is not None

    df = MagicMock()
    writer = MagicMock()
    df.writeStream.format.return_value = writer
    writer.options.return_value = writer
    writer.option.return_value = writer
    writer.queryName.return_value = writer
    writer.trigger.return_value = writer
    writer.start.return_value = MagicMock()

    assert engine.write_stream(spark, config, df) is not None


def test_write_stream_in_cloud_mode_without_spark_conf(spark_session, monkeypatch):
    monkeypatch.setattr(engine, "_is_cloud", lambda: True)

    spark = MagicMock()
    spark.conf.get.side_effect = Exception("cloud conf error")

    df = MagicMock()
    writer = MagicMock()
    df.writeStream.format.return_value = writer
    writer.options.return_value = writer
    writer.option.return_value = writer
    writer.queryName.return_value = writer
    writer.trigger.return_value = writer
    writer.partitionBy.return_value = writer
    writer.start.return_value = MagicMock()

    config = {
        "dataset": "stream_iot",
        "sink": {"path": "/tmp/custom_bronze_kafka", "partition_columns": ["date"]},
    }

    assert engine.write_stream(spark, config, df) is not None


# ---------------------------------------------------------------------------
# run_streaming_ingestion
# ---------------------------------------------------------------------------

def test_run_streaming_ingestion_registers_audit(tmp_path, monkeypatch):
    config = {
        "dataset": "sensores_iot",
        "dataset_name": "sensores_iot",
        "source": {"format": "kafka", "options": {}},
        "sink": {"path": str(tmp_path / "bronze")},
    }
    monkeypatch.setattr(engine, "load_json_config", lambda path: config)
    monkeypatch.setattr(engine, "validate_streaming_config", lambda cfg: True)
    monkeypatch.setattr(engine, "get_spark_session", lambda: MagicMock())
    monkeypatch.setattr(engine, "read_stream", lambda spark, cfg: MagicMock())

    query = MagicMock()
    query.name = "test_query"
    query.id = "test_id"
    query.lastProgress = {"numInputRows": 42}
    monkeypatch.setattr(engine, "write_stream", lambda spark, cfg, df: query)

    with patch("src.streaming.kafka_engine.registrar_auditoria") as mock_audit:
        engine.run_streaming_ingestion("sensores_iot.json")

    mock_audit.assert_called_once()


def test_run_streaming_ingestion_stops_query_on_keyboard_interrupt(monkeypatch):
    monkeypatch.setattr(engine, "load_json_config", lambda path: {"dataset_name": "sensores_iot"})
    monkeypatch.setattr(engine, "validate_streaming_config", lambda cfg: True)
    monkeypatch.setattr(engine, "get_spark_session", lambda: MagicMock())
    monkeypatch.setattr(engine, "read_stream", lambda spark, cfg: MagicMock())

    query = MagicMock()
    query.awaitTermination.side_effect = KeyboardInterrupt()
    query.isActive = True
    monkeypatch.setattr(engine, "write_stream", lambda spark, cfg, df: query)

    engine.run_streaming_ingestion("sensores_iot.json")

    query.stop.assert_called_once()


def test_run_streaming_ingestion_propagates_read_errors(monkeypatch):
    monkeypatch.setattr(engine, "load_json_config", lambda path: {"dataset_name": "sensores_iot"})
    monkeypatch.setattr(engine, "validate_streaming_config", lambda cfg: True)
    monkeypatch.setattr(engine, "get_spark_session", lambda: MagicMock())

    def raise_runtime_error(spark, config):
        raise RuntimeError("Kafka crash")

    monkeypatch.setattr(engine, "read_stream", raise_runtime_error)

    with pytest.raises(RuntimeError):
        engine.run_streaming_ingestion("sensores_iot.json")