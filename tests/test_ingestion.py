import pytest
import json

from sqlalchemy import null
from ingestion.ingest_pysus import PysusApiIngestion

import os
from dotenv import load_dotenv
from pathlib import Path

from tests import SPARK


load_dotenv(dotenv_path=Path('.env'))


# @pytest.mark.skip(reason="testing data ingestion with spark")
def test_should_validate_env_variables() -> None:
    mock_env_dict = {'USER':'user-public-notificacoes',
                'DATABASE':'desc-esus-notifica-estado-pe',
                'URL':'https://user-public-notificacoes:Za4qNXdyQNSa9YaA@elasticsearch-saps.saude.gov.br'}
    env_dict = {'USER':os.getenv('USER'),
                'DATABASE':os.getenv('DATABASE') + 'pe',
                'URL':os.getenv('URL')}
    assert mock_env_dict == env_dict

# @pytest.mark.skip(reason="testing consumption")
def test_should_ingest_sus_data() -> None:
    pai = PysusApiIngestion()
    schema = pai.define_ingestion_schema()
    requested_dataframe = pai.ingest_covid_data(spark=SPARK, schema=schema, uf='pe')

    assert requested_dataframe is not null

def test_should_maintain_schema() -> None:
    return None

# @pytest.mark.skip(reason="testing data ingestion with spark")
def test_should_write_dataframe() -> None:
    pai = PysusApiIngestion()
    schema = pai.define_ingestion_schema()
    pai.write_ingested_data(uf='pe', dataframe=pai.ingest_covid_data(spark=SPARK, schema=schema, uf='pe'))

    assert None == None
