# 1. Instalar el cliente de Kafka dentro de Jupyter
!pip install kafka-python-ng pandas

import time
import json
import pandas as pd
from kafka import KafkaProducer

# 2. Configurar la conexión. 
# ATENCIÓN: Como estamos dentro de la red de Docker, usamos el listener INTERNAL (puerto 9093)
BOOTSTRAP_SERVERS = 'kafka:9093'
TOPIC_NAME = 'transactions'
CSV_PATH = 'data/creditcard.csv'

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Cargando dataset...")
df = pd.read_csv(CSV_PATH)
print(f"Dataset cargado. Total de filas a simular: {len(df)}")

print("Enviando datos en streaming (Presiona Stop en Jupyter para parar)...")
try:
    for index, row in df.iterrows():
        transaction = row.to_dict()
        producer.send(TOPIC_NAME, value=transaction)
        
        # Enviamos 10 transacciones por segundo para probar
        time.sleep(0.1) 
        
        if index % 50 == 0 and index > 0:
            print(f">> Enviados {index} eventos a Kafka.")
except KeyboardInterrupt:
    print("\nSimulación detenida.")
finally:
    producer.flush()
    producer.close()