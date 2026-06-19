import logging
from pyspark.sql import SparkSession, DataFrame

# Configuramos el logger para este módulo
logger = logging.getLogger(__name__)


def read_from_kafka(spark: SparkSession, kafka_config: dict) -> DataFrame:
    """
    Establece la conexión de streaming con Apache Kafka.

    Args:
        spark (SparkSession): La sesión activa de PySpark.
        kafka_config (dict): Diccionario con la configuración de Kafka (extraído del YAML).

    Returns:
        DataFrame: Un PySpark DataFrame en streaming con los datos crudos (binarios).
    """
    try:
        bootstrap_servers = kafka_config["bootstrap_servers"]
        topic = kafka_config["topic"]
        # Usamos .get() con un valor por defecto por si olvidaste ponerlo en el YAML
        starting_offsets = kafka_config.get("starting_offsets", "latest")

        logger.info(f"Conectando a Kafka: {bootstrap_servers} | Tópico: {topic} | Offsets: {starting_offsets}")

        df_raw = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", starting_offsets) \
            .load()

        logger.info("¡Conexión a Kafka establecida con éxito!")
        return df_raw

    except KeyError as missing_key:
        logger.error(f"CRÍTICO: Falta el parámetro obligatorio {missing_key} en la config de Kafka.")
        raise  # Lanzamos el error hacia arriba para que el main.py lo gestione

    except Exception as e:
        logger.error(f"CRÍTICO: Error inesperado al inicializar la lectura de Kafka. Detalles: {e}")
        raise