"""Tests unitarios de src/common/audit_listener.py."""

from types import SimpleNamespace
from unittest.mock import patch

from src.common.audit_listener import AuditStreamingListener


def test_clean_dataset_name_returns_placeholder_for_empty_values():
    listener = AuditStreamingListener(None)
    assert listener._clean_dataset_name(None) == "unknown_source"
    assert listener._clean_dataset_name("") == "unknown_source"


def test_on_query_progress_registers_untracked_query():
    listener = AuditStreamingListener(None)

    event = SimpleNamespace(
        progress=SimpleNamespace(
            id="uuid-fantasma-123",
            name="bronze-farmia-ventas_streaming",
            numInputRows=42,
        )
    )

    # Progreso de una query que no pasó por onQueryStarted:
    # se limpia el nombre y se registra igualmente.
    listener.onQueryProgress(event)

    assert "uuid-fantasma-123" in listener._active_queries
    assert listener._active_queries["uuid-fantasma-123"]["dataset_name"] == "ventas"
    assert listener._active_queries["uuid-fantasma-123"]["rows"] == 42


def test_on_query_idle_is_a_noop():
    listener = AuditStreamingListener(None)
    listener.onQueryIdle(SimpleNamespace())


def test_on_query_terminated_registers_failure_with_exception():
    listener = AuditStreamingListener(None)
    listener._active_queries["query_fail_id"] = {"dataset_name": "test_ds"}

    event = SimpleNamespace(
        id="query_fail_id",
        name="farmia_streaming_test",
        exception="AnalysisException: schema mismatch",
    )

    with patch("src.common.audit_listener.registrar_auditoria") as mock_registrar:
        listener.onQueryTerminated(event)

    mock_registrar.assert_called_once()
    _, kwargs = mock_registrar.call_args
    assert kwargs.get("status") == "FAILURE"