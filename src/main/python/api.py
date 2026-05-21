import json
import sys
import os
from dotenv import load_dotenv
import requests


def get_data(base_path: str, execution_date: str):
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
    load_dotenv(dotenv_path=f"{base_path}/.env")
    token = os.getenv("API_KEY")
    headers = {"x_cg_demo_api_key": token}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()

        # store API response to raw file
        file_path = f"{base_path}/data/raw/crypto_market_data_raw_{execution_date}.json"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    else:
        print(f"Error retrieving data, status code: {response.status_code}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: api.py <base_path>")
        sys.exit(1)
    get_data(sys.argv[1], sys.argv[2])
