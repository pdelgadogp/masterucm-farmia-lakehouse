import logging
import os
import sys

def get_logger(module_name: str) -> logging.Logger:
    """
    Configura y devuelve un logger unificado.
    
    - En local: Muestra trazas en nivel DEBUG con formato de consola legible.
    - En Databricks: Muestra trazas en nivel INFO, ideal para integrarse con Databricks.
    """
    logger = logging.getLogger(module_name)
    
    # Evitar duplicar si el logger ya ha sido inicializado en otra importación
    if logger.hasHandlers():
        return logger

    # Configurar el nivel de log según el entorno
    env = os.getenv("ENVIRONMENT", "local").lower()
    if env == "local":
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Formato homogéneo para todas las trazas del motor de ingesta
    log_format = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Configurar la salida estándar por consola (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # Evitar la propagación hacia el logger raíz de Spark/Java (Log4j), evitando duplicidad de logs en Databricks
    logger.propagate = False

    return logger