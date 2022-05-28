from data_ingestion import __version__

import requests
import json
import pytest
from data_ingestion.data_consumption import get_api_data_status


@pytest
def should_test_api_connection_status():
    test_url = ""
    requested_data_status = get_api_data_status(test_url)

    assert requested_data_status == 200
