from datetime import datetime
import json
import os
from dotenv import load_dotenv
import requests

# TODO: set up test for API connection
# TODO: add error message for when the data doesn't load

def get_data():
    # Test API endpoint
    try:
        test = "https://api.coingecko.com/api/v3/ping?"
        r = requests.get(test)

    except requests.exceptions.RequestException as e:  
        raise SystemExit(e)

    # Define API endpoint
    url = (
        "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&sparkline=true"
    )

    # Get API token from .env file
    load_dotenv()
    token = os.getenv("API_KEY")
    headers = {"x_cg_demo_api_key": token}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # store API response to raw file
        file_path = f"data/raw/crypto_market_data_raw_{timestamp}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    else:
        print(f"Error retrieving data, status code: {response.status_code}")
