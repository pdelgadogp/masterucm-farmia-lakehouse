"""Tests unitarios de src/common/spark.py (gestión de la sesión Spark)."""

from unittest.mock import MagicMock, patch

from src.common.spark import get_spark_session


def test_returns_active_session_in_local_environment(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "local")
    active = MagicMock()
    with patch("pyspark.sql.SparkSession.getActiveSession", return_value=active):
        assert get_spark_session() is active


def test_returns_active_session_in_non_local_environment(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "databricks")
    active = MagicMock()
    with patch("pyspark.sql.SparkSession.getActiveSession", return_value=active):
        assert get_spark_session() is active


def test_creates_new_session_in_databricks_when_none_is_active(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "databricks")
    new_session = MagicMock()
    with patch("pyspark.sql.SparkSession.getActiveSession", return_value=None), \
         patch("pyspark.sql.SparkSession.builder") as mock_builder:
        mock_builder.getOrCreate.return_value = new_session
        assert get_spark_session() is new_session


def test_creates_session_without_delta_package_in_local(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "local")
    new_session = MagicMock()
    with patch.dict("sys.modules", {"delta": None}), \
         patch("pyspark.sql.SparkSession.getActiveSession", return_value=None), \
         patch("pyspark.sql.SparkSession.builder") as mock_builder:
        # get_spark_session encadena llamadas sobre el builder:
        # appName().master().config()...getOrCreate(). Sin estos
        # return_value el mock devolvería hijos nuevos en cada eslabón.
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = new_session
        assert get_spark_session() is new_session


def test_listener_registration_errors_do_not_break_session_creation(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "databricks")
    new_session = MagicMock()
    new_session.streams.addListener.side_effect = Exception("Forced listener error")
    with patch("pyspark.sql.SparkSession.getActiveSession", return_value=None), \
         patch("pyspark.sql.SparkSession.builder") as mock_builder:
        mock_builder.getOrCreate.return_value = new_session
        assert get_spark_session() is new_session