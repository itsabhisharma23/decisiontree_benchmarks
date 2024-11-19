import os
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Specify the folder containing your SQL files
sql_files_folder = './tpcds/bigquery/'  # Replace with the actual path

# Get a list of SQL files in the folder
sql_files = [f for f in os.listdir(sql_files_folder) if f.endswith('.sql')]

# Loop through the SQL files and execute them
for file_name in sql_files:
    # Construct the full file path
    file_path = os.path.join(sql_files_folder, file_name)

    # Read the SQL query from the file
    with open(file_path, 'r') as f:
        query = f.read()

    # Configure the query job
    job_config = bigquery.QueryJobConfig(
        use_query_cache=False,  # Disable cache for accurate timing
        use_legacy_sql=False,  # Use Standard SQL
    )

    # Start the query
    query_job = client.query(query, job_config=job_config)

    # Wait for the query to complete
    query_job.result()

    # Get execution time and slot milliseconds
    execution_time = query_job.ended - query_job.started
    slot_milliseconds = query_job.slot_millis

    print(f"Query from file: {file_name}")
    print(f"Execution time: {execution_time}")
    print(f"Slot milliseconds: {slot_milliseconds}")