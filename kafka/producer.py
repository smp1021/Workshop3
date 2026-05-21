import pandas as pd
from kafka import KafkaProducer
import json
import time

# 1. Configurar el Productor 
# Le decimos que convierta todo a formato JSON automáticamente
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Leer los datos que vamos a "simular" que llegan en vivo
df = pd.read_csv('data/processed/clean_happiness_data.csv')

print(" Iniciando transmisión de datos en vivo a Kafka")

# 3. Enviar fila por fila a Kafka
for index, row in df.iterrows():
    # Convertimos la fila entera a un diccionario
    mensaje = row.to_dict()
    
    #envia al "topic" que llamaremos 'happiness_events'
    producer.send('happiness_events', value=mensaje)
    
    print(f" Enviado país: {mensaje['country']} (Año: {mensaje['year']})")
    
    # Pausamos 2 segundos entre cada envío para que parezca tiempo real
    time.sleep(2)