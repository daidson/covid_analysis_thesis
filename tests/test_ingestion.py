import requests
import json
import pytest
from ingestion.ingest import DataIngestion as dai


@pytest
def should_test_api_connection_status():    
    test_url = ""
    requested_data_status = dai.get_api_data_status(test_url)

    assert requested_data_status == 200
