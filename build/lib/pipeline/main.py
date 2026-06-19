import logging
import argparse
from pyspark.sql import SparkSession

# Importamos nuestros módulos
from pipeline.config.loader import load_config
from pipeline.ingestion.reader import read_from_kafka
from pipeline.processing.parser import parse_kafka_payload
from pipeline.inference.predictor import apply_ml_model
from pipeline.storage.writer import write_stream

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    # =======================================================
    # CAPTURA DE PARÁMETROS DESDE LA CONSOLA
    # =======================================================
    parser = argparse.ArgumentParser(description="Pipeline Antifraude en Streaming")
    parser.add_argument(
        "--config",
        required=True,
        help="Ruta absoluta o relativa al archivo config.yaml"
    )
    args = parser.parse_args()

    logger.info("=== Iniciando Pipeline de Detección de Fraude en Streaming ===")
    logger.info(f"Leyendo configuración desde: {args.config}")

    # 1. Cargar configuración dinámica
    config = load_config(args.config)

    # 2. Inicializar la sesión de Apache Spark
    logger.info("Levantando motor de Apache Spark...")
    spark = SparkSession.builder \
        .appName("FraudDetectionPipeline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Ejecución de la pipeline
        df_raw = read_from_kafka(spark, config["kafka"])
        df_parsed = parse_kafka_payload(df_raw, config["schema"])
        df_predictions = apply_ml_model(df_parsed, config["model"])
        query = write_stream(df_predictions, config["sink"])

        logger.info("Pipeline desplegado. Esperando eventos de Kafka...")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Apagando el pipeline manualmente...")
        query.stop()
    except Exception as e:
        logger.error(f"Fallo crítico en el pipeline: {e}")
        spark.stop()


if __name__ == "__main__":
    main()