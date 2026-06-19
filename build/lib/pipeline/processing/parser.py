import logging
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, from_json

# Configuramos el logger para este módulo
logger = logging.getLogger(__name__)

# Diccionario central para traducir lo que pones en el YAML a objetos reales de PySpark
TYPE_MAPPING = {
    "double": DoubleType(),
    "string": StringType(),
    "integer": IntegerType(),
    "float": FloatType()
}


def _build_dynamic_schema(schema_config: dict) -> StructType:
    """
    Función interna (privada) que construye el esquema de PySpark iterando sobre el diccionario.
    """
    fields = []
    for column_name, data_type_str in schema_config.items():
        # Si pones un tipo raro en el YAML que no existe aquí, usa StringType por defecto para no romper el programa
        spark_type = TYPE_MAPPING.get(data_type_str.lower(), StringType())
        fields.append(StructField(column_name, spark_type, True))

    return StructType(fields)


def parse_kafka_payload(df_raw: DataFrame, schema_config: dict) -> DataFrame:
    """
    Toma el DataFrame binario de Kafka y lo desempaqueta en múltiples columnas tabulares.

    Args:
        df_raw (DataFrame): El DataFrame que escupe 'read_from_kafka'.
        schema_config (dict): La sección 'schema' de tu config.yaml.

    Returns:
        DataFrame: Un DataFrame estructurado y listo para el modelo de Machine Learning.
    """
    try:
        logger.info("Construyendo el esquema dinámico a partir de la configuración...")
        dynamic_schema = _build_dynamic_schema(schema_config)

        logger.info("Aplicando from_json para desempaquetar el payload de Kafka...")
        # 1. Convertimos el binario 'value' a un String legible
        # 2. Usamos from_json aplicando nuestro esquema dinámico
        # 3. Expandimos el struct resultante en columnas individuales (data.*)
        df_parsed = df_raw \
            .selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), dynamic_schema).alias("data")) \
            .select("data.*")

        logger.info("¡Datos parseados y estructurados con éxito!")
        return df_parsed

    except Exception as e:
        logger.error(f"CRÍTICO: Error inesperado al parsear el payload. Detalles: {e}")
        raise