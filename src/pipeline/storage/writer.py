import logging
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


def _write_to_jdbc(df_batch: DataFrame, batch_id: int, jdbc_options: dict):
    """
    Función interna que procesa cada micro-lote y lo escribe en una base de datos relacional.
    """
    try:
        # Se extrae el modo de escritura (por defecto 'append') y se aplica al micro-lote
        mode = jdbc_options.get("mode", "append")

        # Guardamos en base de datos usando las opciones dinámicas (URL, tabla, usuario, etc.)
        df_batch.write \
            .format("jdbc") \
            .options(**jdbc_options) \
            .mode(mode) \
            .save()

    except Exception as e:
        logger.error(f"Error al escribir el lote {batch_id} mediante JDBC. Detalles: {e}")
        raise


def write_stream(df: DataFrame, sink_config: dict) -> StreamingQuery:
    """
    Escribe el DataFrame en streaming hacia el destino configurado de forma agnóstica.

    Args:
        df (DataFrame): El DataFrame resultante del pipeline (con las predicciones).
        sink_config (dict): La sección 'sink' del archivo config.yaml.

    Returns:
        StreamingQuery: El objeto de control de la ejecución en streaming.
    """
    try:
        # 1. Extraemos los metadatos principales del diccionario
        output_format = sink_config.get("format", "console")
        options = sink_config.get("options", {})
        output_mode = sink_config.get("mode", "append")

        # El checkpoint es VITAL en streaming para la tolerancia a fallos
        checkpoint_location = sink_config.get("checkpointLocation", "/tmp/checkpoints")

        logger.info(f"Configurando el escritor de streaming con formato: {output_format}")

        # 2. Inicializamos el escritor base
        writer = df.writeStream \
            .outputMode(output_mode) \
            .option("checkpointLocation", checkpoint_location)

        # 3. Bifurcación agnóstica: Bases de Datos vs Destinos Nativos
        if output_format.lower() == "jdbc":
            logger.info("Usando patrón foreachBatch para destino relacional (JDBC)...")
            # Inyectamos las opciones de JDBC en la función auxiliar
            writer = writer.foreachBatch(lambda batch_df, batch_id: _write_to_jdbc(batch_df, batch_id, options))
        else:
            # Para formatos nativos como 'console', 'parquet', 'delta', 'kafka'
            writer = writer.format(output_format).options(**options)

        # 4. Manejo del Trigger (intervalo de procesamiento) si existe en la configuración
        trigger_config = sink_config.get("trigger", {})
        if trigger_config:
            writer = writer.trigger(**trigger_config)

        # 5. Arrancamos el motor y devolvemos el objeto Query
        logger.info("Arrancando la consulta de streaming...")
        query = writer.start()

        return query

    except Exception as e:
        logger.error(f"CRÍTICO: Fallo al iniciar la escritura del flujo. Detalles: {e}")
        raise