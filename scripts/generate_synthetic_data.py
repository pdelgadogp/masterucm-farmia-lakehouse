import os
import sys
import csv
import json
import io
import random
import uuid
from datetime import datetime
from pathlib import Path

# Inyectar la ruta raíz del proyecto al PATH
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv
load_dotenv()
from confluent_kafka import Producer
from src.common.logger import get_logger
from config.settings import Settings

logger = get_logger(__name__)

PRODUCTOS = [
    {"id": "PROD_001", "nombre": "Semillas de Tomate Híbrido", "precio": 12.50},
    {"id": "PROD_002", "nombre": "Fertilizante Orgánico NPK", "precio": 45.00},
    {"id": "PROD_003", "nombre": "Sensor de Humedad Suelo v2", "precio": 89.99},
    {"id": "PROD_004", "nombre": "Sistema de Riego por Goteo", "precio": 150.00},
    {"id": "PROD_005", "nombre": "Pala Ergonómica de Cultivo", "precio": 19.95}
]

REGIONES = ["Murcia", "Andalucia", "Castilla-La Mancha", "Aragon", "Comunidad Valenciana"]
CARRIERS = ["DHL-Agro", "Correos Express", "Seur-Campo"]
EVENTOS_WEB_ACTIONS = ["click_producto", "busqueda_semillas", "anadir_carrito", "ver_comentarios"]


# ==========================================
# CONFIGURACIÓN DEL PRODUCTOR KAFKA
# ==========================================
def get_kafka_producer():
    """Inicializa y devuelve el cliente Producer de Confluent Kafka leyendo de os.environ o spark.conf."""
    bootstrap = os.getenv("CONFLUENT_BOOTSTRAP_SERVER")
    api_key = os.getenv("CONFLUENT_API_KEY")
    api_secret = os.getenv("CONFLUENT_API_SECRET")

    # Si estamos en Databricks y las env vars de OS no existen, leemos de spark.conf
    if not (bootstrap and api_key and api_secret):
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                bootstrap = bootstrap or spark.conf.get("spark.env.CONFLUENT_BOOTSTRAP_SERVER", None)
                api_key = api_key or spark.conf.get("spark.env.CONFLUENT_API_KEY", None)
                api_secret = api_secret or spark.conf.get("spark.env.CONFLUENT_API_SECRET", None)
        except Exception as e:
            logger.warning(f"No se pudieron recuperar las credenciales desde SparkConf: {e}")

    if not bootstrap or not api_key or not api_secret:
        raise ValueError(
            "Faltan las credenciales de Kafka. Asegúrate de configurar "
            "CONFLUENT_BOOTSTRAP_SERVER, CONFLUENT_API_KEY y CONFLUENT_API_SECRET "
            "en las variables de entorno del clúster o en spark.conf."
        )

    conf = {
        'bootstrap.servers': bootstrap,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
        'client.id': 'farmia-local-generator'
    }
    return Producer(conf)

def delivery_report(err, msg):
    """Callback que se ejecuta cuando Kafka confirma la recepción del mensaje."""
    if err is not None:
        logger.error(f"Fallo al entregar mensaje a Kafka: {err}")


# ==========================================
# FUNCIONES BATCH
# ==========================================
def _write_file(file_path_str: str, content: str):
    if file_path_str.startswith("abfss://") or file_path_str.startswith("dbfs:/"):
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark:
            try:
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                dbutils.fs.put(file_path_str, content, overwrite=True)
                return
            except Exception as e:
                logger.error(f"Error escribiendo con dbutils en {file_path_str}: {e}")

    path_obj = Path(file_path_str)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    with open(path_obj, "w", encoding="utf-8") as f:
        f.write(content)

def generate_batch_ventas(date_str: str, file_path_str: str, num_records: int = 100):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["order_id", "cliente_id", "producto_id", "cantidad", "precio_unitario", "total_pedido", "fecha_pedido", "canal_venta"])
    canales = ["web", "app_movil", "marketplace"]
    for _ in range(num_records):
        prod = random.choice(PRODUCTOS)
        qty = random.randint(1, 5)
        writer.writerow([
            str(uuid.uuid4())[:8], f"CUST_{random.randint(1000, 9999)}", prod["id"],
            qty, prod["precio"], round(prod["precio"] * qty, 2), date_str, random.choice(canales)
        ])
    _write_file(file_path_str, output.getvalue())

def generate_batch_inventario(date_str: str, file_path_str: str):
    data = []
    for tienda_id in range(1, 11):
        for prod in PRODUCTOS:
            data.append({
                "store_id": f"TIENDA_{tienda_id:03d}", "product_id": prod["id"],
                "stock_level": random.randint(10, 500), "last_updated": f"{date_str}T08:00:00Z"
            })
    _write_file(file_path_str, json.dumps(data, indent=4))

def generate_batch_logistica(date_str: str, file_path_str: str, num_records: int = 50):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["shipment_id", "order_id", "carrier", "status", "departure_date", "delivery_date_estimated"])
    for _ in range(num_records):
        writer.writerow([
            f"SH_{random.randint(10000, 99999)}", str(uuid.uuid4())[:8], random.choice(CARRIERS),
            random.choice(["Delivered", "In Transit", "Shipped"]), f"{date_str}T09:00:00Z", f"{date_str}T18:00:00Z"
        ])
    _write_file(file_path_str, output.getvalue())

def generate_batch_meteorologia(date_str: str, file_path_str: str):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["station_id", "region", "temperature", "precipitation_mm", "wind_speed_kmh", "forecast_date"])
    for idx, region in enumerate(REGIONES):
        writer.writerow([
            f"STATION_{idx:03d}", region, round(random.uniform(12.0, 38.0), 1),
            round(random.uniform(0.0, 15.0), 2), round(random.uniform(5.0, 45.0), 1), date_str
        ])
    _write_file(file_path_str, output.getvalue())


# ==========================================
# FUNCIONES STREAMING
# ==========================================
def generate_streaming_sensors(producer: Producer, date_str: str, topic: str = "farmia.iot.sensors", num_records: int = 1000):
    logger.info(f"Enviando {num_records} registros al topic {topic}...")
    for _ in range(num_records):
        sensor_data = {
            "sensor_id": f"SENSOR_{random.randint(1, 15):03d}",
            "temperature": round(random.uniform(15.0, 35.0), 2),
            "humidity_soil": round(random.uniform(20.0, 80.0), 2),
            "soil_ph": round(random.uniform(5.5, 7.5), 1),
            "timestamp": f"{date_str}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}Z"
        }
        producer.produce(
            topic=topic,
            key=sensor_data["sensor_id"].encode('utf-8'),
            value=json.dumps(sensor_data).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
    
    producer.flush()

def generate_streaming_eventos_clientes(producer: Producer, date_str: str, topic: str = "farmia.events.customers", num_records: int = 1000):
    logger.info(f"Enviando {num_records} registros al topic {topic}...")
    for _ in range(num_records):
        event_data = {
            "event_id": str(uuid.uuid4()),
            "user_id": f"USER_{random.randint(10000, 99999)}",
            "action": random.choice(EVENTOS_WEB_ACTIONS),
            "device": random.choice(["Android", "iOS", "Web"]),
            "timestamp": f"{date_str}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}Z"
        }
        producer.produce(
            topic=topic,
            key=event_data["user_id"].encode('utf-8'),
            value=json.dumps(event_data).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
    
    producer.flush()


# ==========================================
# MAIN PRINCIPAL
# ==========================================
def main(mode: str = "all"):
    """
    Genera datos sintéticos para el proyecto FarmIA.
    :param mode: 'all' (por defecto), 'batch' (solo archivos Landing), o 'streaming' (solo eventos Confluent Kafka)
    """
    mode = mode.lower()
    landing_base = Settings.BASE_LANDING_PATH.rstrip("/")
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    file_suffix = today.strftime("%Y%m%d")

    # 1. Ejecución Batch
    if mode in ["all", "batch"]:
        logger.info(f"Iniciando generación de datos BATCH en: {landing_base}")
        generate_batch_ventas(today_str, f"{landing_base}/ecommerce_ventas/farmia-ventas-{file_suffix}-v1.csv")
        generate_batch_inventario(today_str, f"{landing_base}/inventario_local/farmia-inventario-{file_suffix}-v1.json")
        generate_batch_logistica(today_str, f"{landing_base}/logistica_envios/farmia-envios-{file_suffix}-v1.csv")
        generate_batch_meteorologia(today_str, f"{landing_base}/meteorologia/farmia-meteorologia-{file_suffix}-v1.csv")
        logger.info("Archivos Batch generados con éxito.")

    # 2. Ejecución Streaming
    if mode in ["all", "streaming"]:
        logger.info("Iniciando generación de datos STREAMING hacia Confluent Cloud...")
        try:
            producer = get_kafka_producer()
            generate_streaming_sensors(producer, today_str, topic="farmia.iot.sensors", num_records=1000)
            generate_streaming_eventos_clientes(producer, today_str, topic="farmia.events.customers", num_records=1000)
            logger.info("Eventos Kafka generados con éxito.")
        except Exception as e:
            logger.error(f"Error conectando con Confluent Cloud: {e}")

if __name__ == "__main__":
    # Permite pasar el modo por terminal: python generate_synthetic_data.py batch
    selected_mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    main(mode=selected_mode)