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
    
    def categorize_strategy_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Function to categorize strategy columns. It will create new categoric columns based on the values that the strategy columns have.
        The strategy columns refer to strategy as a whole and to search of people without COVID symptoms.
        This function returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """

        dataframe = dataframe \
        .withColumn("ESTRATEGIA_DIAGNOSTICO_ASSISTENCIAL_ESUS", 
            F.when(dataframe.codigoEstrategiaCovid.contains("Diagn'ostico assistencial"), "S").otherwise("N")) \
        .withColumn("ESTRATEGIA_BUSCA_ATIVA_ASSINTOMATICO_ESUS",
            F.when(dataframe.codigoEstrategiaCovid.contains("Busca ativa de assintomatico"), "S").otherwise("N")) \
        .withColumn("ESTRATEGIA_TRIAGEM_POPULACAO_ESPECIFICA_ESUS",
            F.when(dataframe.codigoEstrategiaCovid.contains("Triagem população específica"), "S").otherwise("N")) \
        .withColumn("BUSCA_MONITORAMENTO_CONTATOS_ESUS",
            F.when(dataframe.codigoBuscaAtivaAssintomatico.contains("Monitoramento de contatos"), "S").otherwise("N")) \
        .withColumn("BUSCA_INVESTIGACAO_SURTOS_ESUS",
            F.when(dataframe.codigoBuscaAtivaAssintomatico.contains("Investigação de surtos"), "S").otherwise("N")) \
        .withColumn("BUSCA_MONITORAMENTO_VIAJANTES_RISCO_QUARENTENA_ESUS",
            F.when(dataframe.codigoBuscaAtivaAssintomatico.contains("Monitoramento de viajantes"), "S").otherwise("N")) \
        .withColumn("BUSCA_OUTROS_ESUS",
            F.when(dataframe.codigoBuscaAtivaAssintomatico.contains("Outro"), "S").otherwise("N"))

        return dataframe
    
    def categorize_symptoms_column(self, dataframe: DataFrame) -> DataFrame:
        """
        Function to categorize symptoms column. It will create 9 new categoric columns based on the values that the sympton column has.
        This function returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """

        dataframe = dataframe \
        .withColumn("SINTOMA_ASSINTOMATICO_PESSOA", 
            F.when(dataframe.sintomas.contains("Assintomático"), "S").otherwise("N")) \
        .withColumn("SINTOMA_DOR_CABECA_PESSOA",
            F.when(dataframe.sintomas.contains("Dor de Cabeça"), "S").otherwise("N")) \
        .withColumn("SINTOMA_FEBRE_PESSOA",
            F.when(dataframe.sintomas.contains("Febre"), "S").otherwise("N")) \
        .withColumn("SINTOMA_DISTURBIOS_GUSTATIVOS_PESSOA",
            F.when(dataframe.sintomas.contains("Distúrbios gustativos"), "S").otherwise("N")) \
        .withColumn("SINTOMA_DISTURBIOS_OLFATIVOS_PESSOA",
            F.when(dataframe.sintomas.contains("Distúrbios olfativos"), "S").otherwise("N")) \
        .withColumn("SINTOMA_DISPNEIA_PESSOA",
            F.when(dataframe.sintomas.contains("Dispneia"), "S").otherwise("N")) \
        .withColumn("SINTOMA_TOSSE_PESSOA",
            F.when(dataframe.sintomas.contains("Tosse"), "S").otherwise("N")) \
        .withColumn("SINTOMA_CORIZA_PESSOA",
            F.when(dataframe.sintomas.contains("Coriza"), "S").otherwise("N")) \
        .withColumn("SINTOMA_OUTROS_PESSOA",
            F.when(dataframe.sintomas.contains("Outros"), "S").otherwise("N"))

        return dataframe
    
    def categorize_conditions_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Function to categorize conditions column. It will create 10 new categoric columns based on the values that the conditions column has.
        This function returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """

        dataframe = dataframe \
        .withColumn("CONDICAO_RESPIRATORIA_CRONICA_PESSOA", 
            F.when(dataframe.condicoes.contains("Doenças respiratórias crônicas descompensadas"), "S").otherwise("N")) \
        .withColumn("CONDICAO_RENAL_CRONICA_PESSOA",
            F.when(dataframe.condicoes.contains("Doenças renais crônicas"), "S").otherwise("N")) \
        .withColumn("CONDICAO_CROMOSSOMICA_OU_IMUNOFRAGILIDADE_PESSOA",
            F.when(dataframe.condicoes.contains("Portador de doenças cromossômicas"), "S").otherwise("N")) \
        .withColumn("CONDICAO_CARDIACA_CRONICA_PESSOA",
            F.when(dataframe.condicoes.contains("Doenças cardíacas crônicas"), "S").otherwise("N")) \
        .withColumn("CONDICAO_DIABETES_PESSOA",
            F.when(dataframe.condicoes.contains("Diabetes"), "S").otherwise("N")) \
        .withColumn("CONDICAO_IMUNOSSUPRESSAO_PESSOA",
            F.when(dataframe.condicoes.contains("Imunossupresão"), "S").otherwise("N")) \
        .withColumn("CONDICAO_GESTANTE_PESSOA",
            F.when(dataframe.condicoes.contains("Gestante"), "S").otherwise("N")) \
        .withColumn("CONDICAO_PUERPERA_PESSOA",
            F.when(dataframe.condicoes.contains("Puérpera"), "S").otherwise("N")) \
        .withColumn("CONDICAO_OBESIDADE_PESSOA",
            F.when(dataframe.condicoes.contains("Obesidade"), "S").otherwise("N")) \
        .withColumn("CONDICAO_OUTROS_PESSOA",
            F.when(dataframe.condicoes.contains("Outros"), "S").otherwise("N"))

        return dataframe
    
    def categorize_test_columns(self, dataframe: DataFrame) -> DataFrame:
        
        codigoEstadoTeste
        codigoFabricanteTeste
        codigoResultadoTeste
        codigoTipoTeste
        dataColetaTeste
        estadoTeste
        fabricanteTeste
        loteTeste
        resultadoTeste
        tipoTeste

        return dataframe
    
    def rename_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Function to drop redundant columns.
        This function returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """
        
        dataframe = dataframe.withColumnRenamed("@timestamp", "DATA_INICIO_NOTIFICACAO_ESUS") \
                                .withColumnRenamed("estadoNotificacao", "ESTADO_NOTIFICACAO_ESUS") \
                                .withColumnRenamed("municipioNotificacao", "MUNICIPIO_NOTIFICACAO_ESUS") \
                                .withColumnRenamed("estado", "ESTADO_RESIDENCIA_PESSOA") \
                                .withColumnRenamed("municipio", "MUNICIPIO_RESIDENCIA_PESSOA") \
                                .withColumnRenamed("outrosSintomas", "OUTROS_SINTOMAS_PESSOA") \
                                .withColumnRenamed("dataInicioSintomas", "DATA_INICIO_SINTOMAS_PESSOA") \
                                .withColumnRenamed("outrasCondicoes", "OUTRAS_CONDICOES_PESSOA") \
                                .withColumnRenamed("dataPrimeiraDose", "DATA_PRIMEIRA_DOSE_PESSOA") \
                                .withColumnRenamed("dataSegundaDose", "DATA_SEGUNDA_DOSE_PESSOA") \
                                .withColumnRenamed("dataReforcoDose", "DATA_TERCEIRA_DOSE_PESSOA") \
                                .withColumnRenamed("evolucaoCaso", "EVOLUCAO_CASO_PESSOA") \
                                .withColumnRenamed("classificacaoFinal", "CLASSIFICACAO_FINAL_ESUS") \
                                .withColumnRenamed("dataEncerramento", "DATA_ENCERRAMENTO_NOTIFICACAO_ESUS") \
                                .withColumnRenamed("cbo", "OCUPACAO_PESSOA") \
                                .withColumnRenamed("idade", "IDADE")

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
                                    "@version",
                                    "dataTeste",
                                    "dataTesteSorologico",
                                    "registroAtual",
                                    "sintomas",
                                    "condicoes",
                                    "codigoEstrategiaCovid",
                                    "codigoBuscaAtivaAssintomatico"
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
        