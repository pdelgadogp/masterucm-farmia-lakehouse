"""Tests unitarios de la validación de configuraciones (config/settings.py)."""

import pytest

from config.settings import validate_batch_config, validate_streaming_config


def test_batch_config_without_required_keys_raises_key_error():
    with pytest.raises(KeyError):
        validate_batch_config({"dataset_name": "incomplete_ds"})


def test_streaming_config_without_required_keys_raises_key_error():
    with pytest.raises(KeyError):
        validate_streaming_config({"dataset_name": "incomplete_ds"})