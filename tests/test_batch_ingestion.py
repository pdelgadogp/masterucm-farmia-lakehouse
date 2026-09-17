"""Test de integración del motor batch (AutoLoader) sobre Spark real."""

import src.batch.autoloader_engine as engine


def test_batch_ingestion_reads_and_writes_a_small_csv(
    ecommerce_ventas_layout, ecommerce_ventas_config, spark_session, monkeypatch
):
    monkeypatch.setattr(engine, "_is_cloud", lambda: False)

    df = engine.read_stream(spark_session, ecommerce_ventas_config)
    assert "_ingested_filename" in df.columns
    assert "_ingested_at" in df.columns

    query = engine.write_stream(spark_session, ecommerce_ventas_config, df)
    query.processAllAvailable()
    query.stop()

    rows = spark_session.read.format("delta").load(str(ecommerce_ventas_layout.bronze)).collect()

    assert len(rows) == 1
    assert rows[0]["order_id"] == "ORD-001"
    assert rows[0]["canal_venta"] == "web"

    # En local, el fichero se archiva de Landing a Raw
    assert not (ecommerce_ventas_layout.landing / "orders.csv").exists()
    assert (ecommerce_ventas_layout.raw / "orders.csv").exists()