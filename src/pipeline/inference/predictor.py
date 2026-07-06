import logging
import joblib
import pandas as pd
from typing import Iterator
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, col, array
from pyspark.sql.types import IntegerType
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler

logger = logging.getLogger(__name__)


def apply_ml_model(df_parsed: DataFrame, model_config: dict) -> DataFrame:
    """
    Aplica un modelo predictivo al flujo de streaming de forma agnóstica.
    Soporta ejecuciones distribuidas nativas (Spark MLlib) o externas (Scikit-Learn, etc.)
    mediante el patrón optimizado de Iteradores para Pandas UDF (Spark 3.0+).
    """
    model_type = model_config.get("type", "sklearn")
    model_path = model_config["path"]
    feature_cols = model_config["features"]

    logger.info(f"Iniciando servicio de inferencia. Tipo de modelo detectado: '{model_type}'")

    # =========================================================
    # ENFOQUE A: MODELO NATIVO DE SPARK MLLIB
    # =========================================================
    if model_type == "spark_mllib":
        try:
            logger.info("Cargando Pipeline de Spark MLlib distribuido...")
            spark_model = PipelineModel.load(model_path)

            # Spark ML nativo requiere empaquetar los features en una sola columna Vector
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_vector")
            df_assembled = assembler.transform(df_parsed)

            # Predicción nativa sobre la estructura DataFrame de Spark
            df_predictions = spark_model.transform(df_assembled)

            logger.info("Predicción nativa de Spark MLlib completada.")
            return df_predictions

        except Exception as e:
            logger.error(f"CRÍTICO: Error en ejecución nativa de Spark MLlib: {e}")
            raise

    # =========================================================
    # ENFOQUE B: MODELOS PYTHON (Scikit-Learn, XGBoost, TensorFlow, etc.)
    # Utiliza el patrón avanzado Iterator[Series] -> Iterator[Series]
    # =========================================================
    else:
        @pandas_udf(IntegerType())
        def predict_anomaly(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
            # 1. INICIALIZACIÓN POR PARTICIÓN: Se ejecuta una única vez al levantar la tarea en el Worker
            logger.info(f"Levantando entorno de aislamiento y cargando modelo {model_type} en memoria...")

            if model_type in ["sklearn", "joblib"]:
                model = joblib.load(model_path)
            elif model_type == "xgboost_native":
                import xgboost as xgb
                model = xgb.Booster()
                model.load_model(model_path)
            elif model_type == "tensorflow":
                from tensorflow.keras.models import load_model
                model = load_model(model_path)
            else:
                model = joblib.load(model_path)  # Fallback por defecto

            # 2. PROCESAMIENTO EN BATCHES (Vía Apache Arrow)
            for features_series in iterator:
                # Reconstruimos eficientemente la matriz bidimensional (DataFrame de Pandas) para el predictor
                pdf = pd.DataFrame(features_series.tolist(), columns=feature_cols)

                # Inferencia vectorizada adaptando la API si es necesario (ej: XGBoost DMatrix)
                if model_type == "xgboost_native":
                    import xgboost as xgb
                    dtrain = xgb.DMatrix(pdf)
                    predictions = model.predict(dtrain)
                else:
                    predictions = model.predict(pdf)

                # 3. YIELD: Retorna el lote de predicciones de forma inmediata y mantiene el modelo vivo en RAM
                yield pd.Series(predictions)

        try:
            logger.info("Inyectando Pandas UDF vectorizada con iteradores en el grafo de Spark...")

            # Mapeamos las columnas dinámicas indicadas por configuración
            selected_columns = [col(c) for c in feature_cols]

            # Clave de rendimiento: Consolidamos las columnas en un array de Spark antes de cruzar la frontera de Python
            df_predictions = df_parsed.withColumn(
                "prediction",
                predict_anomaly(array(*selected_columns))
            )

            logger.info("Pipeline de inferencia con soporte de Iteradores generado con éxito.")
            return df_predictions

        except Exception as e:
            logger.error(f"CRÍTICO: Error en el motor de la Pandas UDF con iteradores. Detalles: {e}")
            raise