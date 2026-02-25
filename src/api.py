import requests


def get_data():

    # Define API endpoint

    # url = "https://api.coingecko.com/api/v3/ping"
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin"

    headers = {"x_cg_demo_api_key": "CG - G9RLuK6SZfA44AL3DPwnFLKT"}

    response = requests.get(url, headers=headers)

    data = response.json()
