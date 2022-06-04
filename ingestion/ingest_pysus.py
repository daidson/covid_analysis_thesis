import json
import logging
import requests
from typing_extensions import Self
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pysus.online_data.ESUS import download



class PysusApiIngestion():
    """A class to ingest data from TABNET/DATASUS/SUS using pysus library"""

    def ingest_covid_data(self, spark: SparkSession, uf: str) -> DataFrame:
        """
        Function to ingest covid data from Tabnet using PySUS

        params:
            uf: brazilian state for ingestion reference
        """
        dataframe = download(uf=uf)
        return dataframe
        

    def __init__(self) -> None:
        """Init method to call class"""

        pass
