# Workshop 3: Streaming ETL with Apache Kafka and Machine Learning

## 1. Project Description
This project transitions a traditional batch ETL pipeline into an event-driven streaming architecture. It processes historical World Happiness Report data (2015-2019), trains a machine learning regression model, and simulates a real-time data stream using Apache Kafka. The system performs live ML inference on the incoming data, stores the results in a relational database, and visualizes the streaming metrics in a live analytical dashboard.

## 2. Architecture Explanation
The architecture relies on a microservices approach deployed via Docker Compose to ensure isolation and reproducibility:
* **Apache Zookeeper & Kafka:** Act as the message broker and event streaming platform.
* **Producer (Python):** Simulates real-time data ingestion by reading processed CSV data and publishing JSON events to the Kafka topic.
* **Consumer (Python):** Subscribes to the Kafka topic, loads a serialized ML model, performs live inference on incoming events, calculates the prediction error, and persists the payload into the database.
* **PostgreSQL:** The persistent storage layer that holds the real-time predictions and actual scores.
* **Power BI:** Connects directly to PostgreSQL to query the live data and display streaming metrics.

## 3. Data Cleaning Decisions
The raw data consisted of yearly CSV files with inconsistent schemas. The following cleaning decisions were implemented:
* **Column Standardization:** Created translation dictionaries to map varying column names (e.g., 'Economy (GDP per Capita)' and 'GDP per capita') to a single, lowercase standard (e.g., 'gdp').
* **Dimensional Reduction:** Discarded non-essential and inconsistent columns (such as Confidence Intervals, Dystopia Residual, and Region) that were not present across all years or were irrelevant for the ML model.
* **Consolidation:** Appended all yearly data into a single `clean_happiness_data.csv` file, adding a 'year' column to track the origin of each record.

## 4. Feature Engineering Decisions
To prepare the dataset for the Random Forest regression model:
* **Feature Selection:** Selected strictly numerical features that represent the core metrics of the report: `gdp`, `social_support`, `health`, `freedom`, `generosity`, and `corruption`.
* **Handling Missing Values:** Implemented basic imputation (`fillna(0)`) to ensure the machine learning algorithm does not fail during matrix operations due to null values in specific years.
* **Target Variable:** Set the actual happiness `score` as the target variable (y) for the training phase.

## 5. Kafka Pipeline Explanation
The streaming pipeline uses the `kafka-python` library:
* **Topic:** `happiness_events`.
* **Producer:** Iterates over the cleaned dataset row by row, converting them to dictionaries and sending them to the broker every 2 seconds to simulate a live data feed.
* **Consumer:** Runs an infinite loop listening to the topic (`auto_offset_reset='latest'`). Upon receiving a JSON payload, it extracts the features, converts them to a Pandas DataFrame matching the model's expected format, executes `model.predict()`, and computes the absolute error before inserting the data into the database.

## 6. Database Schema
The database (`happiness_db` in PostgreSQL) uses a single wide table named `predicciones_felicidad_v2` to support the analytical queries:
* `id` (SERIAL PRIMARY KEY)
* `pais` (VARCHAR)
* `anio` (INT)
* `score_real` (FLOAT) - The actual ground truth score.
* `prediccion_score` (FLOAT) - The ML model's inference.
* `error_prediccion` (FLOAT) - Absolute difference between actual and predicted scores.
* `gdp`, `social_support`, `health`, `freedom`, `generosity`, `corruption` (FLOAT) - Input features.
* `fecha_registro` (TIMESTAMP) - Defaults to CURRENT_TIMESTAMP to track the exact streaming event time.

## 7. Dashboard Explanation
The Power BI dashboard connects directly to PostgreSQL (no CSVs are queried) and tracks the following KPIs as defined in the project requirements:
1.  **Average Prediction Error:** A card visual displaying the overall Mean Absolute Error (MAE) of the live inferences.
2.  **Predictions by Country:** A clustered bar chart ranking the top countries based on their average predicted score.
3.  **Predicted vs Actual Score:** A line/column contrast chart (or scatter plot) that plots the model's inference against the ground truth per country, evaluating the model's bias and accuracy.
4.  **Prediction Trends Over Time:** A line chart plotting the predicted scores against `fecha_registro` (filtered by relative time) to demonstrate the live, sequential ingestion of the streaming pipeline.

## 8. Execution Instructions
To run this project locally, ensure Docker and Python are installed, then execute the following steps in order:

1.  **Start the Infrastructure:**
    ```bash
    docker-compose up -d
    ```
    Wait for Zookeeper, Kafka, and PostgreSQL to initialize.

2.  **Set up the Virtual Environment & Dependencies:**
    ```bash
    python -m venv .venv
    source .venv/Scripts/activate  # Or .venv/bin/activate on Mac/Linux
    pip install -r requirements.txt
    ```

3.  **Run the ETL and ML Training (If not already processed):**
    Execute the cells in `notebooks/eda.ipynb` and `notebooks/model_training.ipynb` to generate the clean data and the `model.pkl` file.

4.  **Start the Streaming Pipeline:**
    Open two separate terminals (ensure the virtual environment is active in both).
    * Terminal 1 (Start the Producer):
        ```bash
        python kafka/producer.py
        ```
    * Terminal 2 (Start the Consumer):
        ```bash
        python kafka/consumer.py
        ```

5.  **Monitor the Dashboard:**
    Open the Power BI file and click "Refresh" to see the live data flowing into the visualizations.