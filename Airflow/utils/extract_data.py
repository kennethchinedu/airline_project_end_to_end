import requests
import pandas as pd
from dotenv import load_dotenv
import os, json


type = 'movie'
start_year = '2015'
end_year = '2024'
order_by = 'date'
limit = 100

def extract_data():

    url = f"https://api.apilayer.com/unogs/search/titles?type={type}&start_year={start_year}&order_by={order_by}&limit={limit}&end_year={end_year}"

    payload = {}
    headers= {
    "apikey": "LQpuQgeu4U6qvBAKZd4TcJeDHGDh4RFf"
    }

    response = requests.request("GET", url, headers=headers, data = payload)


    status_code = response.status_code
    result = response.json()

    print(result)

extract_data()

