import requests
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime 


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
        headers = {"apikey": os.getenv(api_key_env_variable)}

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

