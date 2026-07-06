import unittest
import tempfile
import os
from unittest.mock import patch

# Importamos la función que vamos a testear
from pipeline.config.loader import load_config


class TestConfigLoader(unittest.TestCase):

    def setUp(self):
        """
        Se ejecuta ANTES de cada test.
        Aquí creamos archivos temporales para no depender de archivos reales del sistema.
        """
        # 1. Crear un YAML válido temporal
        self.valid_yaml_content = """
        app_name: "TestApp"
        kafka:
          topic: "test_transactions"
        """
        self.temp_valid_file = tempfile.NamedTemporaryFile(delete=False, suffix=".yaml")
        self.temp_valid_file.write(self.valid_yaml_content.encode('utf-8'))
        self.temp_valid_file.close()

        # 2. Crear un YAML inválido (mala indentación) temporal
        self.invalid_yaml_content = """
        app_name: "TestApp"
        kafka:
         topic: "test_transactions"
          bad_indent: "yes"
        """
        self.temp_invalid_file = tempfile.NamedTemporaryFile(delete=False, suffix=".yaml")
        self.temp_invalid_file.write(self.invalid_yaml_content.encode('utf-8'))
        self.temp_invalid_file.close()

    def tearDown(self):
        """
        Se ejecuta DESPUÉS de cada test.
        Limpiamos la basura borrando los archivos temporales.
        """
        os.remove(self.temp_valid_file.name)
        os.remove(self.temp_invalid_file.name)

    def test_load_valid_local_config(self):
        """Prueba que un YAML local bien formado se carga correctamente en un diccionario."""
        config = load_config(self.temp_valid_file.name)

        self.assertIsInstance(config, dict)
        self.assertEqual(config['app_name'], "TestApp")
        self.assertEqual(config['kafka']['topic'], "test_transactions")

    def test_file_not_found_exits(self):
        """Prueba que si el archivo no existe, la aplicación hace sys.exit(1)."""
        with self.assertRaises(SystemExit) as cm:
            load_config("ruta_inventada/no_existe.yaml")

        self.assertEqual(cm.exception.code, 1)

    def test_invalid_yaml_exits(self):
        """Prueba que si el YAML está mal formateado, la aplicación hace sys.exit(1)."""
        with self.assertRaises(SystemExit) as cm:
            load_config(self.temp_invalid_file.name)

        self.assertEqual(cm.exception.code, 1)

    @patch('src.pipeline.config_loader._read_local_file')
    def test_dbfs_path_resolution(self, mock_read_local):
        """
        Prueba que las rutas dbfs:/ se transforman correctamente a /dbfs/.
        Usamos 'patch' (mocking) para fingir la lectura y no necesitar un clúster Databricks real.
        """
        # Le decimos al mock qué devolver cuando sea llamado
        mock_read_local.return_value = "app_name: 'DatabricksApp'"

        # Ejecutamos la función con una ruta tipo Databricks
        config = load_config("dbfs:/mi_carpeta/config.yaml")

        # Verificamos que por debajo intentó leer la ruta local correcta
        mock_read_local.assert_called_with("/dbfs/mi_carpeta/config.yaml")
        self.assertEqual(config['app_name'], 'DatabricksApp')


if __name__ == '__main__':
    unittest.main()