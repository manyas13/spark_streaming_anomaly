import logging
import joblib
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import IntegerType

# Configuramos el logger
logger = logging.getLogger(__name__)


def apply_ml_model(df_parsed: DataFrame, model_config: dict) -> DataFrame:
    """
    Aplica un modelo de Machine Learning preentrenado (Scikit-Learn/XGBoost) al flujo de streaming.

    Args:
        df_parsed (DataFrame): El DataFrame con las columnas limpias (salida de parser.py).
        model_config (dict): La sección 'model' de tu config.yaml.

    Returns:
        DataFrame: El mismo DataFrame pero con una nueva columna 'prediction' (0 = Normal, 1 = Fraude).
    """
    model_path = model_config["path"]
    feature_cols = model_config["features"]

    logger.info(f"Preparando la Pandas UDF con el modelo ubicado en: {model_path}")

    # ==========================================
    # LA MAGIA: PANDAS VECTORIZED UDF
    # ==========================================
    # Le decimos a Spark que esta función devolverá un número entero (0 o 1)
    @pandas_udf(IntegerType())
    def predict_anomaly(*columns_series) -> pd.Series:
        """
        Esta función no se ejecuta en el Master, se empaqueta y se envía a cada Worker.
        Recibe las columnas de Spark transformadas temporalmente en Series de Pandas.
        """
        # 1. El worker carga el modelo desde el disco local a su propia memoria RAM
        # (En Docker local esto funciona perfecto porque todos comparten el mismo volumen ./src)
        model = joblib.load(model_path)

        # 2. Reconstruimos el DataFrame de Pandas concatenando las Series recibidas
        pdf = pd.concat(columns_series, axis=1)

        # 3. Forzamos los nombres de las columnas para que el modelo de sklearn no se queje
        pdf.columns = feature_cols

        # 4. Hacemos la predicción en bloque (vectorizada) y devolvemos una Serie de Pandas
        predictions = model.predict(pdf)
        return pd.Series(predictions)

    # ==========================================
    # APLICACIÓN AL DATAFRAME
    # ==========================================
    try:
        logger.info("Aplicando el modelo a los datos en streaming...")

        # Seleccionamos dinámicamente solo las columnas que el modelo necesita
        selected_columns = [col(c) for c in feature_cols]

        # Inyectamos una nueva columna llamando a nuestra UDF
        df_predictions = df_parsed.withColumn(
            "prediction",
            predict_anomaly(*selected_columns)
        )

        logger.info("¡Predicciones generadas con éxito!")
        return df_predictions

    except Exception as e:
        logger.error(f"CRÍTICO: Error al aplicar el modelo de Machine Learning. Detalles: {e}")
        raise