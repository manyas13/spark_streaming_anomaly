import unittest
import tempfile
import os
import joblib
import pandas as pd
from pyspark.sql import SparkSession, Row

from pipeline.inference.predictor import apply_ml_model


# Creamos un modelo "falso" pero funcional para el test
class DummyModel:
    def predict(self, pdf: pd.DataFrame):
        """
        Lógica simple: Si la columna V1 es mayor que 0, es Fraude (1).
        Si es menor o igual a 0, es Normal (0).
        Devolvemos un array de enteros.
        """
        return (pdf['V1'] > 0).astype(int).values


class TestPredictor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Inicializamos Spark y creamos nuestro modelo físico temporal."""
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestPredictor") \
            .getOrCreate()

        # 1. Instanciamos nuestro modelo de juguete
        dummy_model = DummyModel()

        # 2. Creamos un archivo temporal físico en el sistema
        cls.temp_model_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pkl")
        cls.temp_model_path = cls.temp_model_file.name
        cls.temp_model_file.close()

        # 3. Guardamos el modelo usando joblib (exactamente como lo haría sklearn o xgboost)
        joblib.dump(dummy_model, cls.temp_model_path)

    @classmethod
    def tearDownClass(cls):
        """Apagamos Spark y borramos el modelo del disco duro."""
        cls.spark.stop()
        if os.path.exists(cls.temp_model_path):
            os.remove(cls.temp_model_path)

    def test_apply_ml_model(self):
        """Verifica que la UDF de Pandas procesa el DataFrame correctamente."""

        # 1. Creamos un DataFrame de prueba simulando la salida del parser
        # Fila 1: V1 es 1.5 (Debería predecir 1)
        # Fila 2: V1 es -0.5 (Debería predecir 0)
        df_parsed = self.spark.createDataFrame([
            Row(Time=100.0, V1=1.5, V2=-2.0, Amount=50.0),
            Row(Time=101.0, V1=-0.5, V2=1.0, Amount=10.0)
        ])

        # 2. Configuramos el diccionario apuntando a nuestro modelo temporal
        model_config = {
            "path": self.temp_model_path,
            "features": ["V1", "V2", "Amount"]  # Ignoramos 'Time' a propósito
        }

        # 3. Ejecutamos la función
        df_predictions = apply_ml_model(df_parsed, model_config)

        # 4. Comprobación A: Verificar que existe la nueva columna 'prediction'
        self.assertIn("prediction", df_predictions.columns)

        # 5. Comprobación B: Verificar que las predicciones son matemáticamente correctas
        results = df_predictions.select("V1", "prediction").collect()

        for row in results:
            if row["V1"] > 0:
                self.assertEqual(row["prediction"], 1, "V1 > 0 debería ser clasificado como 1")
            else:
                self.assertEqual(row["prediction"], 0, "V1 <= 0 debería ser clasificado como 0")


if __name__ == '__main__':
    unittest.main()