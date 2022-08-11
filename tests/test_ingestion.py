import pytest
import json

from sqlalchemy import null
from ingestion.ingest import DataIngestion
from ingestion.ingest_pysus import PysusApiIngestion

import os
from dotenv import load_dotenv
from pathlib import Path

from tests import SPARK


load_dotenv(dotenv_path=Path('.env'))


@pytest.mark.skip(reason="using different method for SUS API data ingestion")
def test_api_connection_status() -> None:    
    dai = DataIngestion()
    test_url = "https://elasticsearch-saps.saude.gov.br/desc-esus-notifica-estado-pe/_search"
    requested_data_status = dai.get_api_data_status(test_url, user=os.getenv('USER'), password=os.getenv('PASSWORD'))

    assert requested_data_status == 200

@pytest.mark.skip(reason="using different method for SUS API data ingestion")
def test_data_ingestion() -> None:
    dai = DataIngestion()
    test_url = "https://elasticsearch-saps.saude.gov.br/desc-esus-notifica-estado-pe/_search"
    request_data = dai.get_api_data_status(test_url, user=os.getenv('USER'), password=os.getenv('PASSWORD'))

    path_to_json = 'tests\mock_data.json'
    mock_data = json.loads(open(path_to_json).read())
    
    print(request_data)
    assert request_data == mock_data

@pytest.mark.skip(reason="testing data ingestion with spark")
def test_env_variables() -> None:
    mock_env_dict = {'USER':'user-public-notificacoes',
                'DATABASE':'desc-esus-notifica-estado-pe',
                'URL':'https://user-public-notificacoes:Za4qNXdyQNSa9YaA@elasticsearch-saps.saude.gov.br'}
    env_dict = {'USER':os.getenv('USER'),
                'DATABASE':os.getenv('DATABASE') + 'pe',
                'URL':os.getenv('URL')}
    assert mock_env_dict == env_dict

# @pytest.mark.skip(reason="testing consumption")
def test_ingest_pysus_data() -> None:
    pai = PysusApiIngestion()
    requested_dataframe = pai.ingest_covid_data(spark=SPARK, uf='pe')

    assert requested_dataframe is not null

# @pytest.mark.skip(reason="testing data ingestion with spark")
def test_write_dataframe() -> None:
    pai = PysusApiIngestion()
    pai.write_ingested_data(uf='pe', dataframe=pai.ingest_covid_data(spark=SPARK, uf='pe'))

    assert None == None
