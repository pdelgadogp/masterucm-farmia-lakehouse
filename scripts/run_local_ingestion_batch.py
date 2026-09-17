import os
import sys
import shutil
import time
from pathlib import Path

# FORZAR ENTORNO LOCAL
os.environ["ENVIRONMENT"] = "local"

# Asegurar la raíz del proyecto en sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.generate_synthetic_data import main as generate_synthetic_data
from scripts.query_audit import show_latest_audit
from src.batch.autoloader_engine import run_batch_ingestion
from src.common.logger import get_logger

logger = get_logger("run_local_batch")

BATCH_CONFIGS = [
    "ecommerce_ventas.json",
    "inventario_local.json",
    "logistica_envios.json",
    "meteorologia.json",
]


def clear_local_data_dir():
    data_dir = ROOT_DIR / "data"
    if data_dir.exists():
        logger.info(f"Limpiando directorio local de datos: {data_dir}")
        for item in data_dir.iterdir():
            try:
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
            except Exception as e:
                logger.warning(f"No se pudo eliminar {item}: {e}")
        logger.info("Directorio data/ limpiado correctamente.\n")
    else:
        data_dir.mkdir(parents=True, exist_ok=True)


def main():
    logger.info("=== 0. Limpiando datos anteriores ===")
    clear_local_data_dir()

    logger.info("=== 1. Generando datos sintéticos batch en landing local ===")
    try:
        generate_synthetic_data(mode="batch")
        logger.info("Datos sintéticos batch generados correctamente en data/landing/\n")
    except Exception as e:
        logger.error(f"ERROR durante la generación de datos sintéticos: {e}", exc_info=True)
        sys.exit(1)

    logger.info("=== 2. Iniciando ingesta batch local ===")
    total = len(BATCH_CONFIGS)
    ok = 0
    ko = 0

    for idx, config_file in enumerate(BATCH_CONFIGS, 1):
        logger.info(f"[{idx}/{total}] Procesando dataset: {config_file}")
        try:
            run_batch_ingestion(config_file)
            ok += 1
            logger.info(f"Dataset {config_file} procesado con éxito.\n")
        except Exception as e:
            ko += 1
            logger.error(f"ERROR al procesar {config_file}: {e}\n", exc_info=True)

    logger.info("==========================================")
    logger.info("Resumen de Ingesta Local Batch:")
    logger.info(f"  - OK: {ok}/{total}")
    logger.info(f"  - KO: {ko}/{total}")
    logger.info("==========================================\n\n")

    try:
        logger.info("=== 3. Auditoría de ingesta ===")
        time.sleep(20)
        show_latest_audit()
    except Exception as e:
        logger.warning(f"No se pudo mostrar la auditoría final: {e}")


if __name__ == "__main__":
    main()