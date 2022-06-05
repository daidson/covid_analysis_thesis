from operator import index
from pyspark.sql import DataFrame
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import datetime

import os 
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path('.env'))


class PysusApiIngestion():
    """A class to ingest data from TABNET/DATASUS/SUS using pysus library"""

    def ingest_covid_data(self, uf: str) -> DataFrame:
        """
        Function to ingest covid data from Tabnet using PySUS

        :param uf: brazilian state for ingestion reference
        :param url: string for connection with elastic-search
        """
        self.UF = uf.lower()
        es = Elasticsearch([os.getenv('URL')], send_get_body_as="POST")
        query = {"match_all": {}}
        index_to_access = os.getenv('DATABASE') + self.UF
        results = es.search(query=query,
                            size=10000, 
                            request_timeout=60, 
                            index=index_to_access, 
                            filter_path=['hits.hits._source'])
        final_results = results['hits']['hits']

        data = []
        for result in final_results:
            data.append(result["_source"])

        return pd.DataFrame.from_dict(data)
    
    
    def write_ingested_data(self, dataframe: DataFrame, uf: str) -> None:
        """
        Function to save data in parquet
        """
        input_df = pd.DataFrame(dataframe)
        today = datetime.datetime.now()
        dt = today.strftime("%d_%m_%Y_%H_%M_%S")
        output_name = 'esus_data_' + uf + '_' + dt + '.parquet'
        output_dir = 'ingested_data'

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        input_df.to_parquet(f"{output_dir}/{output_name}")

        return print("Dataframe saved to desired path")


    def __init__(self) -> None:
        """Init method to call class"""
        pass
        
