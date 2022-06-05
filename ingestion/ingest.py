import json
import logging
from typing_extensions import Self
import requests

class DataIngestion():
    """A class to consume data from SUS health system using PySUS"""

    def get_api_data_status(self, api_url: str, user:str, password: str) -> int:
        """
        Function to retrieve the status code from a given API
        
        params:
            api_url: str
        """
        data_status = requests.get(api_url, verify=True, auth=(user, password)).status_code
        return data_status

    def get_data_from_pysus(self, api_url: str, user: str, password: str) -> dict:
        """
        Function to retrieve data from a given API
        
        params:
            api_url: str
        """
        data = requests.get(api_url, verify=True, auth=(user, password))
        return data.json()

    def __init__(self) -> None:
        """Init method to call class"""
        pass
