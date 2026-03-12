# Ro Lakehouse – End-to-End Data Engineering Pipeline (GCP + Spark)

## Project Overview

This project implements a production-style Lakehouse architecture on Google Cloud Platform using:

- Google Cloud Storage (GCS) – Data Lake
- Apache Spark (Dockerized) – Data Processing Engine
- BigQuery – Analytics Warehouse
- Parquet – Columnar optimized storage
- Partitioning & Clustering – Warehouse optimization

It demonstrates a full Bronze → Silver → Gold → BigQuery pipeline with parameterized daily ingestion.

---

# Architecture

Bronze (Raw JSON in GCS)
        ↓
Spark (Dockerized)
        ↓
Silver (Cleaned Parquet in GCS)
        ↓
Gold (Aggregated Metrics in GCS)
        ↓
BigQuery (Partitioned + Clustered Table)
        ↓
Analytics / BI / SQL Queries

---

# Data Layers

## Bronze Layer (Raw Data)

- Raw JSONL files
- Stored in GCS
- Partitioned by ingest_date
- Immutable raw storage
- No transformations applied

Data Sources:
- products (10,000 rows)
- reviews (20,000 rows)
- events (50,000 rows, nested metadata)

---

## Silver Layer (Cleaned Data)

Transformations applied using Spark:

- Schema enforcement
- Type casting
- Deduplication
- Timestamp normalization
- Nested JSON flattening (events.metadata)
- Null handling
- Partitioned by ingest_date
- Written in Parquet (Snappy compression)

Silver Tables:
- silver/products
- silver/reviews
- silver/events

---

## Gold Layer (Business Metrics)

Aggregated product-level metrics:

- total_views
- total_orders
- total_revenue
- avg_rating
- review_count

Stored in:

gs://<bucket>/gold/product_metrics/ingest_date=YYYY-MM-DD/

Gold is computed by joining:
- Silver products
- Silver events
- Silver reviews

---

# BigQuery Warehouse Layer

Table:
lakehouse.product_metrics_p

Features:
- Partitioned by ingest_date (DATE)
- Clustered by product_id
- Optimized for analytical queries

View:
lakehouse.v_product_metrics_latest

This view always points to the latest ingest_date.

---

# Running the Pipeline

## 1️Start Spark Container

docker compose -f docker/docker-compose.yml up -d

## 2️Run Full Daily Pipeline

./run_daily_pipeline.sh 2026-03-11

This single command runs:

1. Silver (products)
2. Silver (events)
3. Silver (reviews)
4. Gold aggregation
5. BigQuery load
6. Validation query

---

# Example BigQuery Validation Query

SELECT
  COUNT(*) AS total_rows,
  SUM(total_views) AS total_views,
  SUM(total_orders) AS total_orders,
  ROUND(SUM(total_revenue), 2) AS total_revenue
FROM lakehouse.product_metrics_p;

Example Output:

total_rows: 10000  
total_views: 35054  
total_orders: 1468  
total_revenue: 227823.37  

---

# Parameterized Pipeline

All Spark jobs are date-driven.

Example:

./spark_jobs/run_spark_gcs.sh silver_products.py 2026-03-11

The master script:

./run_daily_pipeline.sh YYYY-MM-DD

This makes the pipeline production-style and reusable.

---

# Engineering Concepts Demonstrated

- Lakehouse architecture
- Schema drift handling
- Nested JSON flattening
- Spark + Docker integration
- GCS Hadoop connector configuration
- Parquet optimization
- Partitioned & clustered BigQuery tables
- Date-driven batch pipelines
- Automated daily execution
- Version control with Git
- Cloud-native data engineering workflow

---

# Why This Project Matters

This project simulates a real-world production data engineering pipeline, including:

- End-to-end data flow
- Warehouse modeling
- Performance optimization
- Cloud-native architecture
- Analytics-ready datasets

It demonstrates practical skills expected from a mid-level Data Engineer.

---

# Tech Stack

- Python
- PySpark
- Docker
- Google Cloud Storage
- BigQuery
- Parquet
- Git / GitHub

---

End-to-End Lakehouse Data Engineering Project  
Built as a production-style learning system.
