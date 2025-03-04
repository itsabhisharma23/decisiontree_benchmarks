# TPC-DS Benchmarking on BigQuery and Spark

This repository provides a comprehensive framework for performing TPC-DS benchmarking on Google BigQuery and Apache Spark. It includes scripts and instructions for generating TPC-DS data, converting it to Parquet format, loading it into BigQuery, and executing the standard TPC-DS queries.

## Table of Contents

1.  **Generating TPC-DS Data**
2.  **Creating BigQuery Tables (DDLs)**
3.  **Converting Data to Parquet (GCS)**
4.  **Loading Data into BigQuery**
5.  **Running TPC-DS Queries on BigQuery**
6.  **Running TPC-DS Queries on Spark**
7.  **Prerequisites**
8.  **Usage Instructions**

## 1. Generating TPC-DS Data

Before you can run the benchmarks, you need to generate the TPC-DS data. Follow the instructions in this blog post:

[Link to your blog post on generating TPC-DS data]

This blog will guide you through using the `dsdgen` tool to generate the data files.

**Key Points:**

* Ensure you generate the data at your desired scale factor (e.g., 100GB, 1TB).
* Store the generated data files in a local directory.

## 2. Creating BigQuery Tables (DDLs)

The `bigquery_ddl` directory contains the Data Definition Language (DDL) scripts for creating the TPC-DS tables in BigQuery.

**How to Use:**

1.  Open the BigQuery console.
2.  Navigate to your desired dataset.
3.  Execute each DDL script (`.sql` file) from the `bigquery_ddl` directory to create the corresponding table.

## 3. Converting Data to Parquet (GCS)

The `data_conversion` directory contains scripts to convert the generated TPC-DS data files to Parquet format and upload them to a Google Cloud Storage (GCS) bucket.

**How to Use:**

1.  **Configure GCS Bucket:**
    * Create a GCS bucket to store the Parquet files.
    * Update the script(s) in `data_conversion` with your bucket name.
2.  **Run Conversion Script:**
    * Execute the python or shell script that performs the conversion. This script should read the flat files generated by dsdgen and convert them to parquet files, then upload them to your configured GCS bucket.
    * Ensure you have the Google Cloud SDK and necessary libraries installed.
3.  **Verify Upload:**
    * Check your GCS bucket to ensure the Parquet files have been uploaded successfully.

## 4. Loading Data into BigQuery

The `data_loading` directory contains scripts to load the Parquet data from GCS into the BigQuery tables.

**How to Use:**

1.  **Configure BigQuery Load Jobs:**
    * Update the scripts in `data_loading` with your project ID, dataset ID, and GCS bucket location.
2.  **Run Load Scripts:**
    * Execute the scripts to initiate the BigQuery load jobs. This will load the Parquet data into the corresponding BigQuery tables.
3.  **Verify Data Load:**
    * Query the BigQuery tables to ensure the data has been loaded correctly.

## 5. Running TPC-DS Queries on BigQuery

The `bigquery_queries` directory contains the standard TPC-DS queries for BigQuery.

**How to Use:**

1.  Open the BigQuery console.
2.  Navigate to your dataset.
3.  Execute the desired query (`.sql` file) from the `bigquery_queries` directory.
4.  Record the query execution time and results.

## 6. Running TPC-DS Queries on Spark

The `spark_queries` directory contains the standard TPC-DS queries for Apache Spark.

**How to Use:**

1.  **Configure Spark Environment:**
    * Set up a Spark cluster (e.g., Dataproc, local Spark).
    * Ensure Spark has access to your GCS bucket where the Parquet data is stored.
    * Ensure spark has the proper GCS connector configured.
2.  **Run Spark Queries:**
    * Use `spark-sql` or a Spark application to execute the queries from the `spark_queries` directory.
    * Ensure you set the correct path to your parquet files in GCS within your spark queries.
3.  **Record Execution Time:**
    * Record the query execution time and results.

## 7. Prerequisites

* **Google Cloud Platform (GCP) Account:**
    * Access to BigQuery and Google Cloud Storage.
* **Google Cloud SDK:**
    * Installed and configured.
* **Apache Spark:**
    * Installed and configured.
* **`dsdgen` Tool:**
    * Installed and configured for TPC-DS data generation.
* **Python (for data conversion/loading scripts):**
    * Necessary libraries installed (e.g., `google-cloud-storage`, `pandas`, `pyarrow`).
* **Java (for Spark):**
    * Java 8 or higher.

## 8. Usage Instructions

1.  **Generate TPC-DS Data:** Follow the instructions in the linked blog post.
2.  **Create BigQuery Tables:** Execute the DDL scripts in `bigquery_ddl`.
3.  **Convert Data to Parquet:** Run the scripts in `data_conversion` to convert and upload data to GCS.
4.  **Load Data into BigQuery:** Run the scripts in `data_loading` to load data from GCS to BigQuery.
5.  **Run BigQuery Queries:** Execute the queries in `bigquery_queries` and record performance.
6.  **Run Spark Queries:** Configure your Spark environment and execute the queries in `spark_queries`, recording performance.
7.  **Analyze Results:** Compare the performance of BigQuery and Spark for the TPC-DS queries.

**Note:** Remember to replace placeholder values (e.g., bucket names, project IDs) with your actual values.
