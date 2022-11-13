from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
import pyspark.sql.functions as F
import datetime

import os 
from dotenv import load_dotenv
from pathlib import Path

from modeling import SPARK

load_dotenv(dotenv_path=Path('.env'))

class DataModeling():
    """
    A class to model data from TABNET/DATASUS/SUS
    This class allows a user to model data in order to consume it.
    Its  methods can be decoupled from their use case and used globally."""

    def define_schema(self) -> list:
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
            StructField("registroAtual", StringType(), True),
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
            StructField("idade", BooleanType(), True),
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
            StructField("testes", ArrayType(StringType()), True),
            StructField("resultadoTesteSorologicoIgA", StringType(), True),
            StructField("estadoTeste", StringType(), True),
            StructField("estadoNotificacao", StringType(), True),
            StructField("outrasCondicoes", StringType(), True),
            StructField("resultadoTeste", StringType(), True),
            StructField("profissionalSaude", StringType(), True),
            StructField("profissionalSeguranca", StringType(), True),
        ])

        return schema
    
    def ingest_sample_dataframe(self, spark: SPARK, dataframe: DataFrame) -> DataFrame:
        """
        Function to ingest a Covid data sample from SUS-Tabnet using Pyspark
        This function returns only 10 thousand registers from Tabnet

        :param spark: Spark configuration session. Please refer to spark docs when building one.
        :param uf: Brazilian state reference (there are 27 different states)
        :param url: ESUS Elasticsearch connection string
        """
        
        
        return dataframe
    
    def categorize_symptoms_column(self, dataframe: DataFrame) -> DataFrame:

        sintomas

        return dataframe
    
    def categorize_conditions_columns(self, dataframe: DataFrame) -> DataFrame:
        
        condicoes

        return dataframe
    
    def rename_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Function to drop redundant columns.
        This function returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """
        
        dataframe = dataframe.withColumnRenamed("@timestamp", "DATA_NOTIFICACAO_ESUS") \
                                .withColumnRenamed("estadoNotificacao", "ESTADO_NOTIFICACAO_ESUS") \
                                .withColumnRenamed("municipioNotificacao", "MUNICIPIO_NOTIFICACAO_ESUS") \
                                .withColumnRenamed("estado", "ESTADO_RESIDENCIA_PESSOA") \
                                .withColumnRenamed("municipio", "MUNICIPIO_RESIDENCIA_PESSOA") \
                                .withColumnRenamed("outrosSintomas", "OUTROS_SINTOMAS_PESSOA") \
                                .withColumnRenamed("dataInicioSintomas", "DATA_INICIO_SINTOMAS_PESSOA") \
                                .withColumnRenamed("outrasCondicoes", "OUTRAS_CONDICOES_PESSOA") \
                                .withColumnRenamed("", "") \
                                .withColumnRenamed("", "") \
                                .withColumnRenamed("", "") \
                                .withColumnRenamed("", "") \
                                .withColumnRenamed("", "") \
        
        return dataframe
    
    def drop_redundant_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Function to drop redundant columns.
        This function returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """
        
        dataframe = dataframe.drop("estadoNotificacaoIBGE",
                                    "municipioNotificacaoIBGE",
                                    "estadoIBGE",
                                    "municipioIBGE",
                                    "dataNotificacao",
                                    "codigoRecebeuVacina",
                                    "codigoDosesVacina",
                                    "testes",

                                    )
        
        return dataframe
        
    def drop_unused_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Function to drop unused columns that do not follow GDPR/LGPD standards or are API technical information only.
        This function returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """
        
        dataframe = dataframe.drop("@version",
                                    "id",
                                    "racaCor",
                                    "idCollection")
        
        return dataframe
    
    def categorize_doses_data(self, dataframe: DataFrame) -> DataFrame:
        """
        Function to categorize doses data from an individual.
        It also drops the 'codigoDosesVacina' column as it returns data from all doses an individual might has had.
        This function returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """
        
        dataframe = dataframe.withColumn("TEM_PRIMEIRA_DOSE", 
            F.when((dataframe.codigoDosesVacina[0] == "1") |  (dataframe.codigoDosesVacina[1] == "1") | (dataframe.codigoDosesVacina[2] == "1"), "S").otherwise("N")) \
                            .withColumn("TEM_SEGUNDA_DOSE", 
            F.when((dataframe.codigoDosesVacina[0] == "2") |  (dataframe.codigoDosesVacina[1] == "2") | (dataframe.codigoDosesVacina[2] == "2"), "S").otherwise("N")) \
                            .withColumn("TEM_TERCEIRA_DOSE", 
            F.when((dataframe.codigoDosesVacina[0] == "3") |  (dataframe.codigoDosesVacina[1] == "3") | (dataframe.codigoDosesVacina[2] == "3"), "S").otherwise("N"))
        
        return dataframe
    
    def get_last_testing_data(self, dataframe: DataFrame) -> DataFrame:
        """
        Function to get data from the last COVID test an individual has taken.
        It also drops the 'testes' column as it returns data from all tests a person has taken.
        This function returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """
        
        dataframe = dataframe.withColumn("codigoEstadoTeste", dataframe.testes[0].codigoEstadoTeste) \
            .withColumn("codigoFabricanteTeste",  dataframe.testes[0].codigoFabricanteTeste) \
            .withColumn("codigoResultadoTeste",  dataframe.testes[0].codigoResultadoTeste) \
            .withColumn("codigoTipoTeste",  dataframe.testes[0].codigoTipoTeste) \
            .withColumn("dataColetaTeste",  dataframe.testes[0].dataColetaTeste.iso) \
            .withColumn("estadoTeste",  dataframe.testes[0].estadoTeste) \
            .withColumn("fabricanteTeste",  dataframe.testes[0].fabricanteTeste) \
            .withColumn("loteTeste",  dataframe.testes[0].loteTeste) \
            .withColumn("resultadoTeste",  dataframe.testes[0].resultadoTeste) \
            .withColumn("tipoTeste",  dataframe.testes[0].tipoTeste)
        
        return dataframe
    
    def read_json_into_dataframe(self, spark: SPARK, path: str) -> DataFrame:
        """
        Function to read json and make it a dataframe using Pyspark.
        This function returns a dataframe type.

        :param spark: Spark configuration session. Please refer to spark docs when building one.
        :param path: Desired Json path to be read.
        """
        dataframe = spark.read.json(path)
        
        return dataframe

    
    def write_modeled_dataframe(self, dataframe: DataFrame, uf: str) -> None:
        """
        Function to save dataframe in parquet
        """
        today = datetime.datetime.now()
        dt = today.strftime("%d_%m_%Y_%H_%M_%S")
        output_name = 'esus_data_' + uf + '_' + dt + '.parquet'
        output_dir = 'modeled_data'

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        dataframe.write.parquet(f"{output_dir}/{output_name}")

        return print("Dataframe saved to desired path")


    def __init__(self) -> None:
        """Init method to call class"""
        pass
        