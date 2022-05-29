import json
import logging
from typing_extensions import Self
import requests

class DataIngestion():
    """A class to consume data from SUS health system using PySUS"""

    def get_api_data_status(self, api_url: str) -> int:
        data_status = requests.get(api_url, verify=True).status_code
        return data_status

    def get_data_from_pysus(self, api_url: str) -> dict:
        data = requests.get(api_url, verify=True)
        return data.json()

    def __init__(self) -> None:
        """Init method to call class"""
        pass