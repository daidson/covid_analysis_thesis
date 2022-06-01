from importlib.resources import path
import re
import pytest
import requests
import json
from ingestion.ingest import DataIngestion as dai


# @pytest.mark.skip
def test_api_connection_status():    
    test_url = "https://httpbin.org/ip"
    requested_data_status = dai.get_api_data_status(test_url)

    assert requested_data_status == 200

# TDD -> Test Driven Design
# BDD -> Behavior Driven Design

# @pytest.mark.skip
def test_data_ingestion():
    test_url = "https://httpbin.org/ip"
    request_data = dai.get_data_from_pysus(test_url)

    path_to_json = 'tests\mock_data.json'
    mock_data = json.loads(open(path_to_json).read())
    
    assert request_data == mock_data
