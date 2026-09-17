"""Tests unitarios de src/common/logger.py."""

import logging
from unittest.mock import patch

from src.common.logger import get_logger


def test_get_logger_returns_logger_with_expected_name():
    logger = get_logger("test_simple_logger")
    logger.info("Message info")
    logger.debug("Message debug")
    assert logger.name == "test_simple_logger"


def test_get_logger_with_mocked_file_handler():
    with patch("src.common.logger.logging.FileHandler"):
        logger = get_logger("test_file_handler_logger")
        logger.info("Forzando escritura en el handler")
        assert logger is not None


def test_log_level_depends_on_environment(monkeypatch):
    # Entorno local: nivel DEBUG
    monkeypatch.setenv("ENVIRONMENT", "local")
    with patch.object(logging.Logger, "hasHandlers", return_value=False):
        local_logger = get_logger("test_logger_local")
        assert local_logger.level == logging.DEBUG

    # Segunda llamada: el logger ya existe y se devuelve de caché
    assert get_logger("test_logger_local") is not None

    # Entorno cloud: nivel INFO
    monkeypatch.setenv("ENVIRONMENT", "databricks")
    with patch.object(logging.Logger, "hasHandlers", return_value=False):
        cloud_logger = get_logger("test_logger_databricks")
        assert cloud_logger.level == logging.INFO