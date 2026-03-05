from datetime import datetime
import json
import requests

# TODO: set up test for API connection
# TODO: add error message for when the data doesn't load

def get_data():
    # Test API endpoint
    try:
        test = "https://api.coingecko.com/api/v3/ping?"
        r = requests.get(test)

    except requests.exceptions.RequestException as e:  # This is the correct syntax
        raise SystemExit(e)
    
    get_market_data()
    get_historical_data()


def get_historical_data():   
    # Define API endpoint
    url = (
        "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&sparkline=true"
    )

    headers = {"x_cg_demo_api_key": "CG - G9RLuK6SZfA44AL3DPwnFLKT"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # store API response to raw file
        file_path = f"data/raw/crypto_historical_data_raw_{timestamp}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        # todo remove this print line
        print("data saved in file")

    else:
        print(f"Error retrieving data, status code: {response.status_code}")


def get_market_data():
    # Define API endpoint
    url = (
        "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&sparkline=true"
    )

    headers = {"x_cg_demo_api_key": "CG - G9RLuK6SZfA44AL3DPwnFLKT"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # store API response to raw file
        file_path = (
            f"data/raw/crypto_market_data_raw_{timestamp}.json"
        )

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        # todo remove this print line
        print("data saved in file")

    else:
        print(f"Error retrieving data, status code: {response.status_code}")
