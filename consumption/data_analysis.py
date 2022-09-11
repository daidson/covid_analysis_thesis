import os
import pandas as pd
from pyspark.sql import DataFrame


class PysusApiAnalysis():
    """A class to analyse data from TABNET/DATASUS/SUS using pysus library"""

    def open_esus_file(self, path: str) -> DataFrame:
        dataframe = pd.read_parquet(path)
        
        return dataframe
