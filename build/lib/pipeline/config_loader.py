import yaml
import sys
import logging
from urllib.parse import urlparse

# Configuración básica de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _read_local_file(path: str) -> str:
    """Lee un archivo desde el sistema de ficheros local del nodo/contenedor."""
    with open(path, 'r', encoding='utf-8') as file:
        return file.read()


def _read_dbfs_file(path: str) -> str:
    """
    Lee un archivo desde Databricks File System (DBFS).
    Databricks monta nativamente 'dbfs:/' en el sistema de archivos local bajo '/dbfs/'.
    """
    # Transformamos la URI de dbfs a su ruta local montada
    local_dbfs_path = path.replace("dbfs:/", "/dbfs/", 1)
    return _read_local_file(local_dbfs_path)


def _read_s3_file(path: str) -> str:
    """Lee un archivo directamente desde un bucket de AWS S3."""
    try:
        import boto3
    except ImportError:
        logger.error("CRÍTICO: La librería 'boto3' no está instalada. No se puede leer de S3.")
        sys.exit(1)

    # Extraemos el bucket y la clave (ruta) del archivo a partir de la URI
    parsed_uri = urlparse(path)
    bucket_name = parsed_uri.netloc
    object_key = parsed_uri.path.lstrip('/')  # Quitamos el '/' inicial

    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

    # Decodificamos el binario a texto
    return response['Body'].read().decode('utf-8')


def load_config(config_uri: str) -> dict:
    """
    Carga y parsea el archivo YAML desde múltiples orígenes (Local, DBFS, S3).

    Args:
        config_uri (str): La ruta o URI del archivo (ej. 's3://mi-bucket/config.yaml', 'dbfs:/tfm/config.yaml', './config.yaml')

    Returns:
        dict: Diccionario de Python con la configuración.
    """
    try:
        logger.info(f"Intentando cargar configuración desde: {config_uri}")
        yaml_content = ""

        # Estrategia de enrutamiento basada en el prefijo
        if config_uri.startswith("s3://"):
            yaml_content = _read_s3_file(config_uri)

        elif config_uri.startswith("dbfs:/"):
            yaml_content = _read_dbfs_file(config_uri)

        else:
            yaml_content = _read_local_file(config_uri)

        # Parseamos el string obtenido a un diccionario de forma segura
        config_dict = yaml.safe_load(yaml_content)
        logger.info("¡Configuración cargada e interpretada correctamente!")
        return config_dict

    except FileNotFoundError:
        logger.error(f"CRÍTICO: El archivo no existe en la ruta especificada: {config_uri}")
        sys.exit(1)

    except yaml.YAMLError as exc:
        logger.error(f"CRÍTICO: Error de sintaxis en el archivo YAML. Detalles: {exc}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"CRÍTICO: Error inesperado al leer la configuración desde {config_uri}. Detalles: {e}")
        sys.exit(1)