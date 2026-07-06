FROM apache/spark:3.5.0

# 1. Cambiamos al usuario root para tener permisos de instalación a nivel de sistema
USER root

# 2. Copiamos el archivo de requerimientos dentro de la imagen
COPY requirements.txt /tmp/requirements.txt

# 3. Instalamos las dependencias
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# 4. Volvemos al usuario seguro por defecto en las imágenes oficiales de Apache (UID 185)
USER 185