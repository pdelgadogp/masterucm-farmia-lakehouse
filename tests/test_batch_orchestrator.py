"""Test de integración del orquestador batch (run_batch_ingestion) sobre Spark real."""

import src.batch.autoloader_engine as engine


def test_run_batch_ingestion_end_to_end(
    ecommerce_ventas_layout, ecommerce_ventas_config, spark_session, monkeypatch
):
    # El orquestador exige claves adicionales frente al config base
    config = {
        **ecommerce_ventas_config,
        "format": "csv",
        "bronze_table": "ecommerce_ventas",
    }
    monkeypatch.setattr(engine, "load_json_config", lambda path: config)
    monkeypatch.setattr(engine, "_is_cloud", lambda: False)

    engine.run_batch_ingestion("ecommerce_ventas.json")

    # run_batch_ingestion reutiliza la sesión activa (la compartida)
    result = spark_session.read.format("delta").load(str(ecommerce_ventas_layout.bronze))
    assert result.count() == 1