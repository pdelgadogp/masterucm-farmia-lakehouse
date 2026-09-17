"""Test de integración del motor streaming sobre Spark real."""

import time

from pyspark.sql import functions as F

import src.streaming.kafka_engine as engine


def test_streaming_ingestion_writes_to_bronze(local_paths, spark_session, monkeypatch):
    bronze_dir = local_paths.bronze / "sensores_iot"
    checkpoint_dir = local_paths.checkpoints / "sensores_iot_checkpoint"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(engine, "_is_cloud", lambda: False)

    config = {
        "datasource": "farmia",
        "dataset": "sensores_iot",
        "dataset_name": "sensores_iot",
        "sink": {
            "path": str(bronze_dir),
            "options": {"mergeSchema": "true"},
            "partition_columns": ["sensor_id"],
        },
        "bronze_subdir": "sensores_iot",
        "partition_columns": ["sensor_id"],
        "trigger": {"processingTime": "1 second"},
    }

    # Se simula el DataFrame que devolvería read_stream usando la fuente 'rate'
    df = (
        spark_session.readStream.format("rate").option("rowsPerSecond", 5).load()
        .withColumn("sensor_id", F.lit("SENSOR_TEST_001"))
        .withColumn("temperature", F.col("value").cast("double") + 20.0)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_kafka_topic", F.lit("farmia.iot.sensors"))
        .withColumn("_kafka_partition", F.lit(0))
        .withColumn("_kafka_offset", F.col("value"))
        .drop("timestamp")
    )

    query = engine.write_stream(spark_session, config, df)

    # Espera activa hasta que la tabla tenga datos (máx. ~10 segundos)
    rows = []
    result = None
    for _ in range(20):
        time.sleep(0.5)
        try:
            result = spark_session.read.format("delta").load(str(bronze_dir))
            rows = result.collect()
            if rows:
                break
        except Exception:
            pass

    query.stop()

    assert len(rows) > 0, "El stream no ha escrito ningún registro en Delta."
    assert "_kafka_topic" in result.columns
    assert "_ingested_at" in result.columns
    assert rows[0]["sensor_id"] == "SENSOR_TEST_001"
    assert list(checkpoint_dir.iterdir()), "El directorio de checkpoints está vacío."