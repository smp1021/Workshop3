-- KPI 1: Average prediction error (Promedio del error de predicción)
SELECT 
    ROUND(AVG(error_prediccion)::numeric, 4) AS average_prediction_error
FROM 
    predicciones_felicidad_v2;


-- KPI 2: Predictions by country (Predicciones por país)

SELECT 
    pais, 
    ROUND(AVG(prediccion_score)::numeric, 2) AS average_predicted_score,
    COUNT(id) AS total_events_processed
FROM 
    predicciones_felicidad_v2
GROUP BY 
    pais
ORDER BY 
    average_predicted_score DESC
LIMIT 10;


-
SELECT 
    pais, 
    ROUND(AVG(score_real)::numeric, 2) AS average_actual_score,
    ROUND(AVG(prediccion_score)::numeric, 2) AS average_predicted_score,
    ROUND(AVG(error_prediccion)::numeric, 2) AS average_absolute_error
FROM 
    predicciones_felicidad_v2
GROUP BY 
    pais
ORDER BY 
    average_actual_score DESC;


-- KPI 4: Prediction trends over time (Tendencias de predicción a lo largo del tiempo)

SELECT 
    fecha_registro, 
    pais, 
    anio,
    ROUND(prediccion_score::numeric, 2) AS predicted_score
FROM 
    predicciones_felicidad_v2
ORDER BY 
    fecha_registro ASC;