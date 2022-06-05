from importlib.resources import path
import re
import pytest
import requests
import json

from sqlalchemy import null
from ingestion.ingest import DataIngestion
from ingestion.ingest_pysus import PysusApiIngestion
import pandas as pd

from pyspark.sql import SparkSession, DataFrame

import os
from dotenv import load_dotenv
from pathlib import Path


load_dotenv(dotenv_path=Path('.env'))


@pytest.mark.skip(reason="testing dataframe construction")
def test_api_connection_status() -> None:    
    dai = DataIngestion()
    test_url = "https://elasticsearch-saps.saude.gov.br/desc-esus-notifica-estado-pe/_search"
    requested_data_status = dai.get_api_data_status(test_url, user=os.getenv('USER'), password=os.getenv('PASSWORD'))

    assert requested_data_status == 200

@pytest.mark.skip(reason="testing dataframe construction")
def test_data_ingestion() -> None:
    dai = DataIngestion()
    test_url = "https://elasticsearch-saps.saude.gov.br/desc-esus-notifica-estado-pe/_search"
    request_data = dai.get_api_data_status(test_url, user=os.getenv('USER'), password=os.getenv('PASSWORD'))

    path_to_json = 'tests\mock_data.json'
    mock_data = json.loads(open(path_to_json).read())
    
    print(request_data)
    assert request_data == mock_data

@pytest.mark.skip(reason="testing api connection")
def test_env_variables() -> None:
    mock_env_dict = {'USER':'user-public-notificacoes',
                'DATABASE':'desc-esus-notifica-estado-pe',
                'URL':'https://user-public-notificacoes:Za4qNXdyQNSa9YaA@elasticsearch-saps.saude.gov.br'}
    env_dict = {'USER':os.getenv('USER'),
                'DATABASE':os.getenv('DATABASE') + 'pe',
                'URL':os.getenv('URL')}
    assert mock_env_dict == env_dict

# @pytest.mark.skip(reason="testing api connection")
def test_ingest_pysus_data() -> None:
    pai = PysusApiIngestion()
    requested_dataframe = pai.ingest_covid_data(uf='pe')

    print(requested_dataframe.head())

    assert requested_dataframe is not null

def test_write_dataframe() -> None:
    pai = PysusApiIngestion()
    target_path = 'ingested_data'
    pai.write_ingested_data(transformation_path=target_path, uf='pe', dataframe=pai.ingest_covid_data(uf='pe'))

    assert None == None
