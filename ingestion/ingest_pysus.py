from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
from elasticsearch import Elasticsearch
import datetime

import os 
from dotenv import load_dotenv
from pathlib import Path

from ingestion import SPARK

load_dotenv(dotenv_path=Path('.env'))

class PysusApiIngestion():
    """A class to ingest data from TABNET/DATASUS/SUS"""

    def define_ingestion_schema(self) -> list:
        """
        Function to map Covid Data schema from SUS-Tabnet
        This function returns a Type argument
        """
        schema = StructType([
            StructField("resultadoTesteSorologicoIgM", StringType(), True),
            StructField("@timestamp", StringType(), True),
            StructField("resultadoTesteSorologicoIgG", StringType(), True),
            StructField("estadoNotificacaoIBGE", StringType(), True),
            StructField("dataPrimeiraDose", StringType(), True),
            StructField("municipio", StringType(), True),
            StructField("outrasCondicoes", StringType(), True),
            StructField("sexo", StringType(), True),
            StructField("codigoBuscaAtivaAssintomatico", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("dataInicioSintomas", StringType(), True),
            StructField("resultadoTesteSorologicoTotais", StringType(), True),
            StructField("estrangeiro", StringType(), True),
            StructField("racaCor", StringType(), True),
            StructField("dataTesteSorologico", StringType(), True),
            StructField("codigoTriagemPopulacaoEspecifica", StringType(), True),
            StructField("municipioNotificacaoIBGE", StringType(), True),
            StructField("codigoRecebeuVacina", StringType(), True),
            StructField("outroBuscaAtivaAssintomatico", StringType(), True),
            StructField("evolucaoCaso", StringType(), True),
            StructField("idade", StringType(), True),
            StructField("idcodigoLocalRealizacaoTestagemade", StringType(), True),
            StructField("estadoNotificacao", StringType(), True),
            StructField("profissionalSeguranca", StringType(), True),
            StructField("@version", StringType(), True),
            StructField("resultadoTesteSorologicoIgA", StringType(), True),
            StructField("tipoTeste", StringType(), True),
            StructField("dataEncerramento", StringType(), True),
            StructField("estadoTeste", StringType(), True),
            StructField("dataSegundaDose", StringType(), True),
            StructField("estadoIBGE", StringType(), True),
            StructField("testes", ArrayType(StringType()), True),
            StructField("municipioNotificacao", StringType(), True),
            StructField("classificacaoFinal", StringType(), True),
            StructField("registroAtual", BooleanType(), True),
            StructField("codigoDosesVacina", ArrayType(StringType()), True)
        ])

        return schema

    def ingest_covid_data(self, spark: SPARK, schema: list, uf: str) -> DataFrame:
        """
        Function to ingest covid data from SUS-Tabnet using Pyspark

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

        dataframe = spark.createDataFrame(data=data, schema=schema)
        
        return dataframe
    
    
    def write_ingested_data(self, dataframe: DataFrame, uf: str) -> None:
        """
        Function to save dataframe in parquet
        """
        input_df = dataframe
        today = datetime.datetime.now()
        dt = today.strftime("%d_%m_%Y_%H_%M_%S")
        output_name = 'esus_data_' + uf + '_' + dt + '.parquet'
        output_dir = 'ingested_data'

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        input_df.coalesce(10).write.parquet(f"{output_dir}/{output_name}")

        return print("Dataframe saved to desired path")


    def __init__(self) -> None:
        """Init method to call class"""
        pass
        