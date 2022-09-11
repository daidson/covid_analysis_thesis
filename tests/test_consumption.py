import os
from dotenv import load_dotenv
from pathlib import Path
import pytest

from sqlalchemy import null

from consumption.data_analysis import PysusApiAnalysis

@pytest.mark.skip(reason="testing ingestion")
def test_should_open_file() -> None:
    paa = PysusApiAnalysis()
    requested_dataframe = paa.open_esus_file(path='ingested_data\esus_data_pe_05_06_2022_05_48_08.parquet')

    requested_dataframe.head()
    assert requested_dataframe is not null
