import unittest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, DoubleType, StringType, IntegerType

# Importamos las funciones pública y privada de tu parser
from src.pipeline.parser import _build_dynamic_schema, parse_kafka_payload


class TestParser(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        A diferencia del setUp normal (que corre antes de CADA test),
        setUpClass corre UNA SOLA VEZ para toda la clase.
        Levantamos un mini-Spark local con 1 solo núcleo ("local[1]") para que sea muy rápido.
        """
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestParserLocal") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """
        Al terminar todos los tests, apagamos la sesión para liberar memoria RAM.
        """
        cls.spark.stop()

    def test_build_dynamic_schema(self):
        """
        Prueba la función interna que lee el YAML y construye el esquema de PySpark.
        """
        schema_config = {
            "Time": "double",
            "UserId": "integer",
            "Action": "string",
            "VariableRara": "tipo_inventado"  # Simulamos un error de tipografía en el YAML
        }

        # Ejecutamos la función privada
        schema = _build_dynamic_schema(schema_config)

        # 1. Comprobamos que devuelve un objeto StructType
        self.assertIsInstance(schema, StructType)

        # 2. Comprobamos que generó exactamente 4 columnas
        self.assertEqual(len(schema.fields), 4)

        # 3. Comprobamos que el mapeo fue correcto
        self.assertIsInstance(schema["Time"].dataType, DoubleType)
        self.assertIsInstance(schema["UserId"].dataType, IntegerType)

        # 4. Comprobamos la tolerancia a fallos: el tipo inventado debe recaer por defecto en StringType
        self.assertIsInstance(schema["VariableRara"].dataType, StringType)

    def test_parse_kafka_payload(self):
        """
        Prueba la transformación real del DataFrame simulando la llegada de un evento de Kafka.
        """
        # 1. Simulamos el JSON exacto que enviaría tu productor
        json_payload = '{"Time": 0.0, "V1": -1.359, "Amount": 149.62}'

        # 2. Kafka entrega los mensajes en formato binario (bytearray) dentro de una columna llamada 'value'.
        # Creamos un DataFrame falso con esa estructura exacta.
        df_raw = self.spark.createDataFrame([
            Row(value=bytearray(json_payload, 'utf-8'))
        ])

        # 3. Definimos la configuración del YAML que le pasaremos al parser
        schema_config = {
            "Time": "double",
            "V1": "double",
            "Amount": "double"
        }

        # 4. Ejecutamos la función principal
        df_parsed = parse_kafka_payload(df_raw, schema_config)

        # 5. Comprobación A: ¿Se desempaquetó el JSON en columnas separadas?
        expected_columns = ["Time", "V1", "Amount"]
        self.assertEqual(df_parsed.columns, expected_columns)

        # 6. Comprobación B: ¿Extrajo los valores matemáticos correctamente?
        # collect()[0] toma la primera fila del DataFrame para poder inspeccionarla en Python puro
        result_row = df_parsed.collect()[0]

        self.assertEqual(result_row["Time"], 0.0)
        self.assertEqual(result_row["V1"], -1.359)
        self.assertEqual(result_row["Amount"], 149.62)


if __name__ == '__main__':
    unittest.main()