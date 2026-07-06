import unittest
from unittest.mock import MagicMock
from pipeline.storage.writer import write_stream, _write_to_jdbc


class TestWriter(unittest.TestCase):

    def setUp(self):
        # 1. Simulamos el DataFrame que llega desde el predictor
        self.mock_df = MagicMock()

        # 2. Simulamos el objeto DataStreamWriter de Spark
        self.mock_writer = MagicMock()
        self.mock_df.writeStream = self.mock_writer

        # 3. Configuramos el "Method Chaining" para que devuelva el mismo objeto
        # al encadenar métodos (.outputMode().option().format()...)
        self.mock_writer.outputMode.return_value = self.mock_writer
        self.mock_writer.option.return_value = self.mock_writer
        self.mock_writer.format.return_value = self.mock_writer
        self.mock_writer.options.return_value = self.mock_writer
        self.mock_writer.trigger.return_value = self.mock_writer
        self.mock_writer.foreachBatch.return_value = self.mock_writer

        # 4. Simulamos el objeto StreamingQuery resultante al hacer .start()
        self.mock_query = MagicMock()
        self.mock_writer.start.return_value = self.mock_query

    def test_write_stream_standard_format(self):
        """Prueba la ruta estándar (ej. guardar en consola o parquet)."""
        sink_config = {
            "format": "console",
            "mode": "append",
            "checkpointLocation": "/tmp/checkpoints",
            "options": {"truncate": "false"}
        }

        query = write_stream(self.mock_df, sink_config)

        # Verificamos que se inyectaron correctamente los parámetros de Spark
        self.mock_writer.outputMode.assert_called_once_with("append")
        self.mock_writer.option.assert_called_once_with("checkpointLocation", "/tmp/checkpoints")
        self.mock_writer.format.assert_called_once_with("console")
        self.mock_writer.options.assert_called_once_with(truncate="false")

        # Verificamos que el streaming se arrancó
        self.mock_writer.start.assert_called_once()
        self.assertEqual(query, self.mock_query)

    def test_write_stream_jdbc_format(self):
        """Prueba la ruta condicional específica para bases de datos (foreachBatch)."""
        sink_config = {
            "format": "jdbc",
            "checkpointLocation": "/tmp/jdbc_checkpoints",
            "options": {"url": "jdbc:postgresql://localhost", "dbtable": "alertas"}
        }

        write_stream(self.mock_df, sink_config)

        # Al ser JDBC, no debe llamar a .format() sino a .foreachBatch()
        self.mock_writer.format.assert_not_called()
        self.mock_writer.foreachBatch.assert_called_once()
        self.mock_writer.start.assert_called_once()

    def test_write_to_jdbc_internal_logic(self):
        """Prueba la función interna que procesa el micro-lote estático."""
        # Simulamos un micro-lote (un DataFrame tradicional, no de streaming)
        mock_batch_df = MagicMock()
        mock_batch_writer = MagicMock()
        mock_batch_df.write = mock_batch_writer

        mock_batch_writer.format.return_value = mock_batch_writer
        mock_batch_writer.options.return_value = mock_batch_writer
        mock_batch_writer.mode.return_value = mock_batch_writer

        jdbc_options = {
            "url": "jdbc:postgresql://localhost",
            "dbtable": "alertas",
            "mode": "overwrite"  # Forzamos un modo distinto para probar
        }

        # Ejecutamos la función pasándole el lote número 1
        _write_to_jdbc(mock_batch_df, 1, jdbc_options)

        # Verificamos la inyección en el DataFrame estático
        mock_batch_writer.format.assert_called_once_with("jdbc")
        mock_batch_writer.options.assert_called_once_with(**jdbc_options)
        mock_batch_writer.mode.assert_called_once_with("overwrite")
        mock_batch_writer.save.assert_called_once()


if __name__ == '__main__':
    unittest.main()