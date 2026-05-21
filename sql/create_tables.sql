CREATE TABLE IF NOT EXISTS predicciones_felicidad (
    id SERIAL PRIMARY KEY,
    pais VARCHAR(100),
    anio INT,
    prediccion_score FLOAT,
    gdp FLOAT,
    social_support FLOAT,
    health FLOAT,
    freedom FLOAT,
    generosity FLOAT,
    corruption FLOAT,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);