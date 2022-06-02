from importlib.resources import path
import re
import pytest
import requests
import json

from sqlalchemy import null
from ingestion.ingest import DataIngestion
from ingestion.ingest_pysus import PysusApiIngestion
import pandas as pd

mock_state = 'pe'

@pytest.mark.skip(reason="testing dataframe construction")
def test_api_connection_status():    
    dai = DataIngestion()
    test_url = "https://httpbin.org/ip"
    requested_data_status = dai.get_api_data_status(test_url)

    assert requested_data_status == 200

@pytest.mark.skip(reason="testing dataframe construction")
def test_data_ingestion():
    dai = DataIngestion()
    test_url = "https://httpbin.org/ip"
    request_data = dai.get_data_from_pysus(test_url)

    path_to_json = 'tests\mock_data.json'
    mock_data = json.loads(open(path_to_json).read())
    
    assert request_data == mock_data

def test_ingest_pysus_data():
    pai = PysusApiIngestion()
    requested_dataframe = pai.ingest_covid_data(uf = mock_state)
    print(requested_dataframe.head())
    assert requested_dataframe is not null
