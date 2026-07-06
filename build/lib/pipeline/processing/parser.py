import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)

def parse_incoming_stream(df_raw: DataFrame, schema_config: dict) -> DataFrame:
    """
    Parsea el flujo binario bruto de Kafka, aplica transformaciones SQL optimizadas
    y limpia variables temporales utilizando una arquitectura declarativa.
    """
    try:
        logger.info("Iniciando Pipeline de Ingesta y Transformación Unificada...")

        # Fase 0: Conversión del payload binario de Kafka a texto (String)
        df_string = df_raw.withColumn("json_str", col("value").cast(StringType()))

        base_exprs = []
        transform_configs = {}

        # =========================================================
        # FASE 1: EXTRACOCCIÓN BASE (Schema-on-Read en JVM)
        # =========================================================
        for final_col_name, props in schema_config.items():
            if "path" in props:
                json_path = f"$.{props['path']}"
                data_type = props["type"]

                # 'get_json_object' evita cruzar a Python y extrae en Java de forma ultra eficiente
                extraction_expr = expr(f"get_json_object(json_str, '{json_path}')").cast(data_type).alias(final_col_name)
                base_exprs.append(extraction_expr)

            elif "transform" in props:
                transform_configs[final_col_name] = props
            else:
                raise ValueError(
                    f"Configuración inválida en la columna '{final_col_name}'. "
                    f"Debe declarar obligatoriamente la clave 'path' o 'transform'."
                )

        # Aplicamos la extracción masiva de variables de una sola pasada
        df_parsed = df_string.select(*base_exprs)
        logger.info(f"Fase 1 Completada: Se han extraído {len(base_exprs)} variables base del JSON.")

        # =========================================================
        # FASE 2: INGENIERÍA DE CARACTERÍSTICAS (Catalyst Optimizer)
        # =========================================================
        if transform_configs:
            logger.info(f"Fase 2 Iniciada: Aplicando {len(transform_configs)} transformaciones declarativas...")
            for final_col_name, props in transform_configs.items():
                sql_expression = props["transform"]
                data_type = props["type"]

                # Inyectamos la nueva columna ejecutando la fórmula de forma nativa en Spark
                df_parsed = df_parsed.withColumn(final_col_name, expr(sql_expression).cast(data_type))
                logger.debug(f"Transformación inyectada con éxito: {final_col_name} -> ({sql_expression})")

        # =========================================================
        # FASE 3: PODADO Y LIMPIEZA DE ATRIBUTOS (Feature Pruning)
        # =========================================================
        cols_to_drop = [
            col_name for col_name, props in schema_config.items()
            if props.get("keep", True) is False
        ]

        if cols_to_drop:
            df_parsed = df_parsed.drop(*cols_to_drop)
            logger.info(f"Fase 3 Completada: Eliminadas {len(cols_to_drop)} variables temporales/ocultas.")

        logger.info(f"¡Procesamiento finalizado! Matriz preparada con {len(df_parsed.columns)} columnas.")
        return df_parsed

    except Exception as e:
        logger.error(f"CRÍTICO: Fallo en el motor del parser dinámico unificado. Detalles: {e}")
        raise