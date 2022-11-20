import json
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
from elasticsearch import Elasticsearch
import datetime

import os 
from dotenv import load_dotenv
from pathlib import Path

from ingestion import SPARK

load_dotenv(dotenv_path=Path('.env'))

class EsusApiIngestion():
    """A class to ingest data from TABNET/DATASUS/SUS"""

    def define_ingestion_schema(self) -> list:
        """
        Function to map Covid Data schema from SUS-Tabnet
        This function returns a Type argument with a schema list of columns
        """
        schema = StructType([
            StructField("outroTriagemPopulacaoEspecifica", StringType(), True),
            StructField("dataSegundaReforcoDose", StringType(), True),
            StructField("dataTesteSorologico", StringType(), True),
            StructField("@version", StringType(), True),
            StructField("codigoEstrategiaCovid", StringType(), True),
            StructField("dataNotificacao", StringType(), True),
            StructField("municipioIBGE", StringType(), True),
            StructField("outroBuscaAtivaAssintomatico", StringType(), True),
            StructField("estadoIBGE", StringType(), True),
            StructField("dataInicioTratamento", StringType(), True),
            StructField("resultadoTesteSorologicoIgG", StringType(), True),
            StructField("outroLocalRealizacaoTestagem", StringType(), True),
            StructField("cbo", StringType(), True),
            StructField("codigoBuscaAtivaAssintomatico", StringType(), True),
            StructField("codigoDosesVacina", ArrayType(StringType()), True),
            StructField("codigoTriagemPopulacaoEspecifica", StringType(), True),
            StructField("loteSegundaReforcoDose", StringType(), True),
            StructField("dataEncerramento", StringType(), True),
            StructField("resultadoTesteSorologicoTotais", StringType(), True),
            StructField("outroAntiviral", StringType(), True),
            StructField("dataTeste", StringType(), True),
            StructField("registroAtual", BooleanType(), True),
            StructField("codigoQualAntiviral", StringType(), True),
            StructField("sexo", StringType(), True),
            StructField("municipioNotificacaoIBGE", StringType(), True),
            StructField("laboratorioSegundaReforcoDose", StringType(), True),
            StructField("id", StringType(), True),
            StructField("tipoTeste", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("estrangeiro", StringType(), True),
            StructField("evolucaoCaso", StringType(), True),
            StructField("dataPrimeiraDose", StringType(), True),
            StructField("classificacaoFinal", StringType(), True),
            StructField("municipio", StringType(), True),
            StructField("idade", StringType(), True),
            StructField("municipioNotificacao", StringType(), True),
            StructField("racaCor", StringType(), True),
            StructField("tipoTesteSorologico", StringType(), True),
            StructField("codigoRecebeuVacina", StringType(), True),
            StructField("qualAntiviral", StringType(), True),
            StructField("idCollection", StringType(), True),
            StructField("estadoNotificacaoIBGE", StringType(), True),
            StructField("dataInicioSintomas", StringType(), True),
            StructField("codigoContemComunidadeTradicional", StringType(), True),
            StructField("recebeuAntiviral", StringType(), True),
            StructField("dataSegundaDose", StringType(), True),
            StructField("dataReforcoDose", StringType(), True),
            StructField("outrosSintomas", StringType(), True),
            StructField("codigoLocalRealizacaoTestagem", StringType(), True),
            StructField("codigoRecebeuAntiviral", StringType(), True),
            StructField("sintomas", StringType(), True),
            StructField("condicoes", StringType(), True),
            StructField("resultadoTesteSorologicoIgM", StringType(), True),
            StructField("@timestamp", StringType(), True),
            StructField("testes", ArrayType(
                StructType([
                    StructField("codigoEstadoTeste", StringType(), True),
                    StructField("estadoTeste", StringType(), True),
                    StructField("codigoResultadoTeste", StringType(), True),
                    StructField("fabricanteTeste", StringType(), True),
                    StructField("codigoTipoTeste", StringType(), True),
                    StructField("loteTeste", StringType(), True),
                    StructField("codigoFabricanteTeste", StringType(), True),
                    StructField("dataColetaTeste", StructType([
                        StructField("__type", StringType(), True),
                        StructField("iso", StringType(), True)
                        ]), True),
                    StructField("resultadoTeste", StringType(), True),
                    StructField("tipoTeste", StringType(), True)
                ])
            ), True),
            StructField("resultadoTesteSorologicoIgA", StringType(), True),
            StructField("estadoTeste", StringType(), True),
            StructField("estadoNotificacao", StringType(), True),
            StructField("outrasCondicoes", StringType(), True),
            StructField("resultadoTeste", StringType(), True),
            StructField("profissionalSaude", StringType(), True),
            StructField("profissionalSeguranca", StringType(), True),
        ])

        return schema
    

    def ingest_covid_dataframe(self, spark: SPARK, schema: list, uf: str) -> DataFrame:
        """
        Function to ingest all Covid data from SUS-Tabnet using Pyspark

        :param spark: Spark configuration session. Please refer to spark docs when building one.
        :param uf: Brazilian state reference (there are 27 different states)
        :param url: ESUS Elasticsearch connection string
        
        """
        self.UF = uf.lower()

        es = Elasticsearch([os.getenv('ESUS_URL')], send_get_body_as="POST")

        query = {"match_all": {}}

        index_to_access = os.getenv('ESUS_DATABASE') + self.UF

        page = es.search(
            index = index_to_access,
            doc_type = None,
            scroll = '25m',
            search_type = 'query_then_fetch',
            size = 10000,
            query = query
        )
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']["value"]
        
        data = []
        while(scroll_size > 0):
            page = es.scroll(scroll_id = sid, scroll = '5m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            for hit in page['hits']['hits']:
                data.append(hit["_source"])

        dataframe = spark.createDataFrame(data=data, schema=schema)
        
        return dataframe

    def ingest_covid_data_json(self, uf: str) -> list:
        """
        Function to ingest all Covid data from SUS-Tabnet using Pyspark

        :param spark: Spark configuration session. Please refer to spark docs when building one.
        :param uf: Brazilian state reference (there are 27 different states)
        :param url: ESUS Elasticsearch connection string
        
        """
        self.UF = uf.lower()

        es = Elasticsearch([os.getenv('ESUS_URL')], send_get_body_as="POST", 
                            timeout=60, max_retries=10, retry_on_timeout=True)

        query = {"match_all": {}}

        index_to_access = os.getenv('ESUS_DATABASE') + self.UF

        page = es.search(
            index = index_to_access,
            doc_type = None,
            scroll = '25m',
            search_type = 'query_then_fetch',
            size = 10000,
            query = query,
            request_timeout=60
        )
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']["value"]
        
        data = []
        while(scroll_size > 0):
            page = es.scroll(scroll_id = sid, scroll = '5m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            for hit in page['hits']['hits']:
                data.append(hit["_source"])

        return data

    def ingest_sample_dataframe(self, spark: SPARK, schema: list, uf: str) -> DataFrame:
        """
        Function to ingest a Covid data sample from SUS-Tabnet using Pyspark
        This function returns only 10 thousand registers from Tabnet

        :param spark: Spark configuration session. Please refer to spark docs when building one.
        :param uf: Brazilian state reference (there are 27 different states)
        :param url: ESUS Elasticsearch connection string
        """
        self.UF = uf.lower()

        es = Elasticsearch([os.getenv('ESUS_URL')], send_get_body_as="POST")

        query = {"match_all": {}}

        index_to_access = os.getenv('ESUS_DATABASE') + self.UF

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

    def ingest_sample_data_json(self, uf: str) -> list:
        """
        Function to ingest a Covid data sample from SUS-Tabnet using Pyspark
        This function returns only 10 thousand registers from Tabnet

        :param spark: Spark configuration session. Please refer to spark docs when building one.
        :param uf: Brazilian state reference (there are 27 different states)
        :param url: ESUS Elasticsearch connection string
        """
        self.UF = uf.lower()

        es = Elasticsearch([os.getenv('ESUS_URL')], send_get_body_as="POST")

        query = {"match_all": {}}

        index_to_access = os.getenv('ESUS_DATABASE') + self.UF

        results = es.search(query=query,
                            size=10000, 
                            request_timeout=60, 
                            index=index_to_access, 
                            filter_path=['hits.hits._source'])
        final_results = results['hits']['hits']
        
        data = []
        for result in final_results:
            data.append(result["_source"])

        return data    
    
    def write_ingested_dataframe(self, dataframe: DataFrame, uf: str) -> None:
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

        input_df.write.parquet(f"{output_dir}/{output_name}")

        return print("Dataframe saved to desired path")

    def write_ingested_json(self, data: list, uf: str) -> None:
        """
        Function to save dataframe in parquet
        """
        today = datetime.datetime.now()
        dt = today.strftime("%d_%m_%Y_%H_%M_%S")
        output_name = 'esus_data_' + uf + '_' + dt + '.json'
        output_dir = 'ingested_data'

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        with open(f"{output_dir}/{output_name}", 'w', encoding='utf-8') as fp:
            json.dump(data, fp, ensure_ascii=False)

        return print("Dataframe saved to desired path")


    def __init__(self) -> None:
        """Init method to call class"""
        pass
        