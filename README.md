# Zameen.pk ETL Pipeline

An end-to-end data engineering project for collecting real-estate listings from Zameen.pk, streaming them through Kafka, processing them with Apache Spark, and exposing the processed data through a Streamlit dashboard. Apache Airflow is included for workflow orchestration.

## Architecture

```text
Zameen.pk
   |
   v
Python / BeautifulSoup Scraper
   |
   v
Apache Kafka
   |
   v
Spark Structured Streaming
   |
   v
Parquet Data Store
   |
   v
Streamlit Dashboard

Airflow orchestrates the scheduled pipeline tasks.
```

## Tech Stack

- Python
- BeautifulSoup + Requests
- Apache Kafka
- Apache Spark / PySpark
- Apache Airflow
- Docker Compose
- Parquet
- Streamlit

## Project Structure

```text
.
├── dags/
│   └── etl_dag.py
├── dashboard/
│   └── app.py
├── producer/
│   └── kafka_producer.py
├── spark/
│   └── spark_streamer.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Pipeline Flow

1. The producer requests a Zameen.pk listings page and extracts property information.
2. Each listing is serialized as JSON and published to the `zameen_property_listings` Kafka topic.
3. Spark Structured Streaming consumes the topic using a predefined schema.
4. The processed stream is written to Parquet files for analytics and downstream use.
5. Streamlit loads the latest Parquet output and displays listing, price, location, bedroom, bathroom, and area insights.
6. Airflow provides a DAG for scheduling the producer and Spark processing stages.

## Run Locally

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Start the infrastructure:

```bash
docker compose up -d
```

Run the producer:

```bash
python producer/kafka_producer.py
```

Run Spark with the Kafka connector:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark/spark_streamer.py
```

Launch the dashboard after Parquet output is available:

```bash
streamlit run dashboard/app.py
```

Airflow is available at `http://localhost:8080` when the Docker services are running.

## Notes

- Website HTML can change over time, so scraper selectors may need maintenance.
- The Airflow DAG assumes `spark-submit` is available in the Airflow worker/runtime where the DAG is executed.
- Runtime-generated logs, checkpoints, Parquet output, local environments, and secrets are excluded from Git.
