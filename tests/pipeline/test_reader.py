import unittest
from unittest.mock import MagicMock
from src.pipeline.reader import read_from_kafka


class TestReader(unittest.TestCase):

    def setUp(self):
        """
        Preparamos un 'doble de acción' (Mock) para la sesión de Spark.
        Como Spark encadena métodos (ej. spark.readStream.format().option().load()),
        necesitamos que nuestro mock se devuelva a sí mismo en cada paso hasta el final.
        """
        # 1. Creamos la sesión falsa
        self.mock_spark = MagicMock()

        # 2. Creamos un objeto falso que representará la cadena de configuraciones
        self.mock_chain = MagicMock()

        # 3. Le decimos a la sesión falsa que cuando llamen a .readStream.format(), devuelva nuestra cadena
        self.mock_spark.readStream.format.return_value = self.mock_chain

        # 4. Le decimos a la cadena que cuando llamen a .option(), se devuelva a sí misma (para poder encadenar más)
        self.mock_chain.option.return_value = self.mock_chain

        # 5. Finalmente, definimos el DataFrame falso que devolverá el método .load()
        self.fake_df = MagicMock()
        self.mock_chain.load.return_value = self.fake_df

    def test_read_from_kafka_success(self):
        """Prueba el camino feliz: los parámetros correctos generan un DataFrame."""
        # Diccionario simulando lo que devolvería tu YAML
        valid_config = {
            "bootstrap_servers": "localhost:9092",
            "topic": "test_topic",
            "starting_offsets": "earliest"
        }

        # Ejecutamos nuestra función con el Spark falso y la configuración válida
        result_df = read_from_kafka(self.mock_spark, valid_config)

        # 1. Comprobamos que devuelve el DataFrame que configuramos en el setUp
        self.assertEqual(result_df, self.fake_df)

        # 2. Verificamos que Spark fue llamado con "kafka"
        self.mock_spark.readStream.format.assert_called_once_with("kafka")

        # 3. Verificamos que se pasaron las opciones correctas.
        # MagicMock.mock_calls guarda un historial de todas las llamadas que recibió.
        # Comprobamos que el método 'option' fue llamado con los parámetros esperados.
        self.mock_chain.option.assert_any_call("kafka.bootstrap.servers", "localhost:9092")
        self.mock_chain.option.assert_any_call("subscribe", "test_topic")
        self.mock_chain.option.assert_any_call("startingOffsets", "earliest")

    def test_read_from_kafka_missing_required_key(self):
        """Prueba que lance un KeyError si falta configuración obligatoria (ej. topic)."""
        invalid_config = {
            "bootstrap_servers": "localhost:9092"
            # Falta 'topic' intencionalmente
        }

        # Le decimos a unittest que esperamos que la siguiente línea provoque una explosión controlada (KeyError)
        with self.assertRaises(KeyError):
            read_from_kafka(self.mock_spark, invalid_config)

    def test_read_from_kafka_default_offset(self):
        """Prueba que si no pasamos starting_offsets, usa 'latest' por defecto."""
        config_without_offsets = {
            "bootstrap_servers": "localhost:9092",
            "topic": "test_topic"
        }

        read_from_kafka(self.mock_spark, config_without_offsets)

        # Verificamos que aplicó el valor por defecto "latest"
        self.mock_chain.option.assert_any_call("startingOffsets", "latest")


if __name__ == '__main__':
    unittest.main()