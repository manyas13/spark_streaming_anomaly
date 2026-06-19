import logging
import joblib
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, col, array
from pyspark.sql.types import IntegerType

# Configuramos el logger
logger = logging.getLogger(__name__)

# Variable global en el Worker para cachear el modelo y no saturar el disco
_model_cache = None


def load_model_cached(path):
    global _model_cache
    if _model_cache is None:
        _model_cache = joblib.load(path)
    return _model_cache


def apply_ml_model(df_parsed: DataFrame, model_config: dict) -> DataFrame:
    """
    Aplica un modelo de Machine Learning preentrenado (Scikit-Learn/XGBoost) al flujo de streaming.
    """
    model_path = model_config["path"]
    feature_cols = model_config["features"]

    logger.info(f"Preparando la Pandas UDF con el modelo ubicado en: {model_path}")

    # ==========================================
    # LA MAGIA: PANDAS VECTORIZED UDF (PATRÓN ARRAY PYSPARK 3+)
    # ==========================================
    # Le decimos a Arrow explícitamente: "Vas a recibir UNA Serie de Pandas y devolver UNA Serie de Pandas"
    @pandas_udf(IntegerType())
    def predict_anomaly(features_series: pd.Series) -> pd.Series:
        # 1. Cargamos el modelo (usando la caché para que vuele)
        model = load_model_cached(model_path)

        # 2. Reconstruimos el DataFrame 2D matricial que Scikit-Learn necesita
        # features_series llega como una Serie donde cada celda es una lista: [V1, V2, ... Amount]
        pdf = pd.DataFrame(features_series.tolist(), columns=feature_cols)

        # 3. Hacemos la predicción vectorizada
        predictions = model.predict(pdf)

        # 4. Devolvemos el resultado tipado correctamente
        return pd.Series(predictions)

    # ==========================================
    # APLICACIÓN AL DATAFRAME
    # ==========================================
    try:
        logger.info("Aplicando el modelo a los datos en streaming...")

        # Seleccionamos las columnas dinámicas
        selected_columns = [col(c) for c in feature_cols]

        # LA CLAVE: Empaquetamos todas las columnas en un array de Spark antes de cruzar la frontera de Python
        df_predictions = df_parsed.withColumn(
            "prediction",
            predict_anomaly(array(*selected_columns))
        )

        logger.info("¡Estructura de predicción generada con éxito!")
        return df_predictions

    except Exception as e:
        logger.error(f"CRÍTICO: Error al aplicar el modelo de Machine Learning. Detalles: {e}")
        raise