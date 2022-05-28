import json
import logging
import requests

def get_api_data_status(api_url: str):
    data_status = requests.get(api_url, verify=False).status_code

    return data_status

def get_data_from_sinapi(api_url: str) -> dict:
    data = requests.get(api_url, verify=False)

    return data.json()
