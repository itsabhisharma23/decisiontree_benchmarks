# TPC-DS Benchmarking on BigQuery and Spark

This repository provides a comprehensive guide and scripts for performing TPC-DS benchmarking on Google BigQuery and Apache Spark. It includes scripts and instructions for generating TPC-DS data, converting it to Parquet format, loading it into BigQuery, and executing the standard TPC-DS queries.

## Table of Contents

1.  **Generating TPC-DS Data**
2.  **Creating BigQuery Tables (DDLs)**
3.  **Loading Data into BigQuery**
4.  **Download Data to Parquet format (GCS)**
5.  **Running TPC-DS Queries on BigQuery**
6.  **Running TPC-DS Queries on Spark**
7.  **Prerequisites**
8.  **Usage Instructions**

## 1. Generating TPC-DS Data

Before you can run the benchmarks, you need to generate the TPC-DS data. Follow the instructions in this blog post:

[Generate TPCDS data on GCP](https://medium.com/google-cloud/how-to-generate-tpc-ds-data-in-gcp-b8e134b6ced9)

This blog will guide you through using the `dsdgen` tool to generate the data files.

**Key Points:**

* Ensure you generate the data at your desired scale factor (e.g., 100GB, 1TB).
* Store the generated data files in a local directory.

## 2. Creating BigQuery Tables (DDLs)

The `BQ_create_table_commands.sql` contains the Data Definition Language (DDL) scripts for creating the TPC-DS tables in BigQuery.

**How to Use:**

1.  Open the BigQuery console.
2.  Navigate to your desired dataset.
3.  Execute the DDL script (`BQ_create_table_commands.sql` file) to create the corresponding tables.

## 3. Loading Data into BigQuery

The `move_gcs_to_bq.sh` script can load the data flat files from GCS into the BigQuery tables.

**How to Use:**

1.  **Run Script:**
    * Execute the script to initiate the BigQuery load jobs. This will load the flat files data into the corresponding BigQuery tables.
3.  **Verify Data Load:**
    * Query the BigQuery tables to ensure the data has been loaded correctly.

## 4. Download Data to Parquet format (GCS)

The `download_tpcds_tables_as_parquet_from_bq.py` script converts download TPCDS data from BigQuery to Parquet format and upload them to a Google Cloud Storage (GCS) bucket.

**How to Use:**

1.  **Configure GCS Bucket:**
    * Create a GCS bucket to store the Parquet files.
    * Update the GCS bucket name in `download_tpcds_tables_as_parquet_from_bq.py`.
2.  **Run Conversion Script:**
    * Execute the python code that performs the conversion. 
3.  **Verify Upload:**
    * Check your GCS bucket to ensure the Parquet files have been uploaded successfully.


## 5. Running TPC-DS Queries on BigQuery

The `tpcds_queries_bq.sql` file contains the standard TPC-DS queries for BigQuery.

**How to Use:**

1.  Open the BigQuery console.
2.  Navigate to your dataset.
3.  Execute the desired query from the `tpcds_queries_bq.sql` file.
4.  Record the query execution time and results.

## 6. Running TPC-DS Queries on Spark

The `tpcds_queries_spark.py` script contains the standard TPC-DS queries for Apache Spark.

**How to Use:**

1.  **Configure Spark Environment:**
    * Set up a Spark cluster (e.g., Dataproc).
    * Ensure Spark has access to your GCS bucket where the Parquet data is stored.
    * Ensure spark has the proper GCS connector configured.
2.  **Run Spark Queries:**
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


**Note:** Remember to replace placeholder values (e.g., bucket names, project IDs) with your actual values.
