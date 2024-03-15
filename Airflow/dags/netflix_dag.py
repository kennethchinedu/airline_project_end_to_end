from airflow import DAG
import pandas as pd
from datetime import timedelta, datetime
import json, requests, os
from airflow.operators.python import PythonOperator 
from airflow.models import Variable 
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from dotenv import load_dotenv





load_dotenv()

start_year = '2024'
end_year = '2024'
order_by = 'date'
api_key_env_variable = 'NETFLIX_API_KEY'  # New environment variable for API key
limit = 100  # Example: Set the limit for each request

def extract_titles():
    all_data = []
    offset = 0

    # Continue fetching data until there are no more results left
    while True:
        url = f"https://api.apilayer.com/unogs/search/titles?start_year={start_year}&order_by={order_by}&end_year={end_year}&limit={limit}&offset={offset}"
        payload = {}
        headers = {"apikey": 'TeQ3XJVWyIw70z0fvREHoiBnC0ljFg4i'}

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

    # Create a DataFrame from all_data
    df = pd.DataFrame(all_data)

    # Save the DataFrame as a new CSV file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    csv_file_name = f'Netflix2024_{timestamp}.csv'
    df.to_csv(csv_file_name, index=False)
    print(f"CSV file {csv_file_name} saved successfully.")

    # Display the DataFrame
    return df


def extract_people():
    all_data = []
    offset = 500

    # Continue fetching data until there are no more results left
    while True:
        url = f"https://api.apilayer.com/unogs/search/people?person_type=Director"
        payload = {}
        headers = {"apikey": 'TeQ3XJVWyIw70z0fvREHoiBnC0ljFg4i'}

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
        print(f'loaded{offset} data')

    # Create a DataFrame from all_data
    df = pd.DataFrame(all_data)

    # Save the DataFrame as a new CSV file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    csv_file_name = f'Netflix_people_{timestamp}.csv'
    df.to_csv(csv_file_name, index=False)
    print(f"CSV file {csv_file_name} saved successfully.")

    # Display the DataFrame
    return df



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
     

     extract_titles_task = PythonOperator(
        task_id="extract_titles_task",
        python_callable= extract_titles
    ),
     
     extract_people_task = PythonOperator(
        task_id="extract_people_task",
        python_callable= extract_people
    )
     



extract_titles_task >> extract_people_task
