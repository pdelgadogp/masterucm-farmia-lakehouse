import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.spark import get_spark_session
from config.settings import Settings

def show_latest_audit(limit: int = 20) -> None:
    spark = get_spark_session()
    df = spark.read.format("delta").load(f"{Settings.BASE_BRONZE_PATH}/ingestion_control")
    df.orderBy(df.ingest_ts.desc()).show(limit, truncate=False)


def main() -> None:
    show_latest_audit()


if __name__ == "__main__":
    main()