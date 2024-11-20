import os
from google.cloud import bigquery
import datetime


project_id = 'hadoop-benchmarking01'
dataset_id = 'benchmarkingv1'
res_table_id = 'benchmark_results'


# Construct a BigQuery client object.
client = bigquery.Client()

# Specify the folder containing your SQL files
sql_files_folder = 'bigquery/'  

# Get a list of SQL files in the folder
sql_files = [f for f in os.listdir(sql_files_folder) if f.endswith('.sql')]

# Loop through the SQL files and execute them
for file_name in sql_files:
    res_data=[]
    # Construct the full file path
    file_path = os.path.join(sql_files_folder, file_name)
    # Read the SQL query from the file
    with open(file_path, 'r') as f:
        query = f.read()

    # Generate a unique job ID (you can customize this as needed)
    job_id = f"TPCDS_{file_name.split('.')[0]}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"

    # Configure the query job with the job ID
    job_config = bigquery.QueryJobConfig(
        use_query_cache=False,
        use_legacy_sql=False
    )
    print(f"Submitting {file_name.split('.')[0]}")
    # Start the query
    query_job = client.query(query, job_config=job_config,job_id=job_id)

    # Wait for the query to complete
    query_job.result()

    # Get execution time and slot milliseconds
    runtime_secs = (query_job.ended - query_job.started).total_seconds()
    slot_milliseconds = query_job.slot_millis
    res_data.append({
        'benchmark_id': 1,
        'service_id': 1,
        'configuration_id': None,
        'date_id': int(datetime.date.today().strftime("%Y%m%d")),
        'runtime_seconds': runtime_secs,
        'cost_measure': slot_milliseconds,
        'cost_metric': 'slotms',
        'query_no': int(file_name.split('.')[0].replace("query","")),
    })

    table_ref = client.dataset(dataset_id, project=project_id).table(res_table_id)


    job_config = bigquery.LoadJobConfig(
        schema=[  # If the table doesn't exist, you can define the schema here
            bigquery.SchemaField("benchmark_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("service_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("configuration_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("date_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("runtime_seconds", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("cost_measure", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("cost_metric", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("query_no", "INTEGER", mode="NULLABLE"),
        ],
    )

    job = client.load_table_from_json(res_data, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded data into BigQuery table {project_id}:{dataset_id}.{res_table_id} for {file_name.split('.')[0]}")

