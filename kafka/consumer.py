import json
import pickle
import pandas as pd
from kafka import KafkaConsumer
import psycopg2

print(" Cargando el modelo de Inteligencia Artificial")
with open('models/model.pkl', 'rb') as f:
    modelo = pickle.load(f)

print("Conectando a PostgreSQL")
conexion = psycopg2.connect(
    host="localhost",
    database="happiness_db",
    user="admin",
    password="adminpassword"
)
cursor = conexion.cursor()

# Creamos la V2 de la tabla con los campos requeridos 
cursor.execute("""
    CREATE TABLE IF NOT EXISTS predicciones_felicidad_v2 (
        id SERIAL PRIMARY KEY,
        pais VARCHAR(100),
        anio INT,
        score_real FLOAT,
        prediccion_score FLOAT,
        error_prediccion FLOAT,
        gdp FLOAT,
        social_support FLOAT,
        health FLOAT,
        freedom FLOAT,
        generosity FLOAT,
        corruption FLOAT,
        fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")
conexion.commit()

consumer = KafkaConsumer(
    'happiness_events',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

print("Consumidor V2 activo. Escuchando y guardando datos en tiempo real")

for mensaje in consumer:
    datos = mensaje.value
    
    variables = [[
        datos['gdp'], datos['social_support'], datos['health'], 
        datos['freedom'], datos['generosity'], datos['corruption']
    ]]
    df_pred = pd.DataFrame(variables, columns=['gdp', 'social_support', 'health', 'freedom', 'generosity', 'corruption'])
    
    # Cálculos para la rúbrica
    prediccion = float(modelo.predict(df_pred)[0])
    score_real = float(datos['score'])
    
    # Calculamos el error absoluto 
    error = abs(score_real - prediccion)
    
    insert_query = """
        INSERT INTO predicciones_felicidad_v2 
        (pais, anio, score_real, prediccion_score, error_prediccion, gdp, social_support, health, freedom, generosity, corruption)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        datos['country'], datos['year'], score_real, prediccion, error,
        datos['gdp'], datos['social_support'], datos['health'], 
        datos['freedom'], datos['generosity'], datos['corruption']
    ))
    conexion.commit()
    
    print(f" DB -> País: {datos['country']} | Real: {score_real:.2f} | IA: {prediccion:.2f} | Error: {error:.2f}")