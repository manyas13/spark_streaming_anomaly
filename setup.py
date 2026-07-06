from setuptools import setup, find_packages

setup(
    name="fraud_pipeline",
    version="1.0.0",
    description="Pipeline de Detección de Fraude con Apache Spark",
    author="Tu Nombre",
    # Le decimos a Python que busque los paquetes en la carpeta src
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    # Incluimos explícitamente main.py ya que no está dentro de una subcarpeta
    py_modules=["main"],

    # Dependencias mínimas para que el paquete funcione (el requirements.txt integrado)
    install_requires=[
        "pyspark>=3.5.0",
        "pyyaml>=6.0",
        "pandas",
        "scikit-learn",
        "pyarrow>=10.0.1"
    ],

    # LA MAGIA: Creamos un comando de terminal personalizado
    entry_points={
        "console_scripts": [
            "run-fraud-pipeline=main:main"
        ]
    }
)