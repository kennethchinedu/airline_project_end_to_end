from airflow import DAG
import pandas as pd
from datetime import timedelta, datetime
import json, requests, os
from airflow.operators.python import PythonOperator 
from airflow.models import Variable 
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from dotenv import load_dotenv

from airflow.models import TaskInstance


load_dotenv()

start_year = '2024'
end_year = '2024'
order_by = 'date'
api_key_env_variable = Variable.get("netflix_api")  # New environment variable for API key
limit = 100   #The limit for each request

#This function extracts the titles data
def extract_titles():
    all_data = []
    offset = 0

    # Continue fetching data until there are no more results left
    while True:
        url = f"https://api.apilayer.com/unogs/search/titles?start_year={start_year}&order_by={order_by}&end_year={end_year}&limit={limit}&offset={offset}"
        payload = {}
        headers = {"apikey": api_key_env_variable}

        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()

        # Check if there are no more results left
        if not data["results"]:
            break

        # Extract relevant fields from each result and append to all_data
        for result in data["results"]:
            all_data.append({
                "imdb_id": result["imdb_id"],
                "title": result["title"],
                "rating": result["rating"],
                "year": result["year"],
                "runtime": result["runtime"],
                "top250": result["top250"],
                "top250tv": result["top250tv"],
                "title_date": result["title_date"]
            })

        # Increment the offset for the next request
        offset += limit
        print(f'loaded {offset} data')

    # Create a DataFrame from all_data
    df = pd.DataFrame(all_data)


    # # Saving the DataFrame as a new CSV file with timestamp
    # timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    csv_file_name = 'Netflix2024.csv'
    df.to_csv(csv_file_name, index=False)
    print(f"CSV file {csv_file_name} saved successfully.")

    return csv_file_name
  

#This function extracts the peoples data
def extract_people():
    all_data = []
    offset = 500

    # Continue fetching data until there are no more results left
    while True:
        url = f"https://api.apilayer.com/unogs/search/people?person_type=Director"
        payload = {}
        headers = {"apikey": api_key_env_variable}

        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()

        # Check if there are no more results left
        if not data["results"]:
            break

        # Extract relevant fields from each result and append to all_data
        for result in data["results"]:
            all_data.append({
                "netflix_id": result["netflix_id"],
                "full_name": result["full_name"],
                "person_type": result["person_type"],
                "title": result["title"]
            })
        
        # Increment the offset for the next request
        offset += limit
        print(f'loaded {offset} data')

    # Create a DataFrame from all_data
    df = pd.DataFrame(all_data)

    # Save the DataFrame as a new CSV file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    csv_file_name = f'Netflix_people_{timestamp}.csv'
    df.to_csv(csv_file_name, index=False)
    print(f"CSV file {csv_file_name} saved successfully.")

    return df



# This function loads CSV data into Snowflake
import pandas as pd
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook

def load_titles_to_snowflake(csv_file_path, snowflake_table, snowflake_conn_id):
    if not os.path.exists(csv_file_path):
        print(f"CSV file {csv_file_path} does not exist.")
        return

    # Connect to Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id)
    connection = snowflake_hook.get_conn()

    # Create a cursor
    cursor = connection.cursor()

    # Define your Snowflake copy command
    snowflake_copy_command = f"""
    COPY INTO {snowflake_table}
    FROM '{csv_file_path}'
    FILE_FORMAT = (TYPE = CSV)
    """

    # Execute the COPY command
    cursor.execute(snowflake_copy_command)

    # Commit the transaction
    connection.commit()

    # Close the cursor and connection
    cursor.close()
    connection.close()

    print(f"Data copied from {csv_file_path} to Snowflake table {snowflake_table} successfully.")





    # This function loads CSV data into Snowflake
def load_names_to_snowflake(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_titles_task')  # Get data from extract_titles_task

    if extracted_data:
        # Connect to Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id)
        connection = snowflake_hook.get_conn()

        # Create a cursor
        cursor = connection.cursor()

        # Define your Snowflake copy command
        snowflake_copy_command = f"""
        COPY INTO {snowflake_table}
        FROM VALUES {','.join([str(tuple(row.values())) for row in extracted_data])}
        """

        # Execute the COPY command
        cursor.execute(snowflake_copy_command)

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()
    

snowflake_query = """ 
    CREATE OR REPLACE TABLE test_table(
        name VARCHAR,
        email VARCHAR
    );
"""


#Ddefining daf arguments
default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 9),
    'email': ['anamsken60@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'netflix_etl_dag',
    default_args=default_args,
    schedule_interval = '@daily',
    catchup=False ) as dag:




    #  is_api_available = HttpSensor(
    #     task_id="is_api_available",
    #     http_conn_id="netflix_api",
    #     endpoint='unogs/search/titles',
    #     headers= { "apikey": "TeQ3XJVWyIw70z0fvREHoiBnC0ljFg4i"},
    #     poke_interval = 3,
    #     response_check = lambda response: "results" in response.text,
    #     timeout = 10
    #     )
     
     create_table = SnowflakeOperator(
        task_id = 'create_table',
        sql = snowflake_query,
        snowflake_conn_id='snowflake_conn'
    )

     extract_titles_task = PythonOperator(
        task_id="extract_titles_task",
        python_callable= extract_titles,
        dag=dag,
    )
     
     extract_people_task = PythonOperator(
        task_id="extract_people_task",
        python_callable= extract_people
    )
     
     load_titles_to_snoflake_tsk = PythonOperator(
        task_id='load_titles_to_snoflake_tsk',
        python_callable=load_titles_to_snowflake,
        op_kwargs={'csv_file_path':'/Users/kennethchinedu/Desktop/My DE/airline_project/Airflow/code/Netflix2024.csv', 'snowflake_table': 'Movies', 'snowflake_conn_id': 'snowflake_conn'},  # Pass keyword arguments here
    )
     
     load_names_to_snoflake_tsk = PythonOperator(
        task_id="load_names_to_snoflake_tsk",
        python_callable= load_names_to_snowflake
    )


    
    #  load_titles_file_to_s3 = BashOperator(
    #     task_id="load_titles_file_to_s3",
    #     bash_command= 'aws s3 mv {{ti.xcom_pull("extract_titles_task")[0]}} s3://zillow-analytics-ec2/lagos/',
        
    # )
     
    #  load_peoples_file_to_s3 = BashOperator(
    #     task_id="load_peoples_file_to_s3",
    #     bash_command= 'aws s3 mv {{ti.xcom_pull("extract_people_task")[0]}} s3://zillow-analytics-ec2/lagos/',
        
    # )
     



create_table  >> extract_titles_task >> load_titles_to_snoflake_tsk >> extract_people_task >> load_names_to_snoflake_tsk