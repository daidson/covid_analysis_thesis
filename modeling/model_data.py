from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, IntegerType
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

    
    
    
    def parse_column_types(self, dataframe: DataFrame) -> DataFrame:
        """
        Method to parse column types. During the ingestion all the columns were saved either as array or string type.
        This method also parses strings to upper case if there is a need for it.
        This allows better handling of the data during analysis.

        :param dataframe: Input dataframe to have types changed
        """

        dataframe = dataframe \
            .withColumn("DATA_INICIO_NOTIFICACAO_ESUS", F.to_date("DATA_INICIO_NOTIFICACAO_ESUS")) \
            .withColumn("DATA_ENCERRAMENTO_NOTIFICACAO_ESUS", F.to_date("DATA_ENCERRAMENTO_NOTIFICACAO_ESUS")) \
            .withColumn("DATA_INICIO_SINTOMAS_PESSOA", F.to_date("DATA_INICIO_SINTOMAS_PESSOA")) \
            .withColumn("DATA_PRIMEIRA_DOSE_PESSOA", F.to_date("DATA_PRIMEIRA_DOSE_PESSOA")) \
            .withColumn("DATA_SEGUNDA_DOSE_PESSOA", F.to_date("DATA_SEGUNDA_DOSE_PESSOA")) \
            .withColumn("DATA_TERCEIRA_DOSE_PESSOA", F.to_date("DATA_TERCEIRA_DOSE_PESSOA")) \
            .withColumn("TESTE_DATA_COLETA_ESUS", F.to_date("TESTE_DATA_COLETA_ESUS")) \
            .withColumn("TESTE_LOTE_ESUS", F.upper("TESTE_LOTE_ESUS")) \
            .withColumn("OCUPACAO_PESSOA", F.upper("OCUPACAO_PESSOA")) \
            .withColumn("ESTADO_RESIDENCIA_PESSOA", F.upper("ESTADO_RESIDENCIA_PESSOA")) \
            .withColumn("ESTADO_NOTIFICACAO_ESUS", F.upper("ESTADO_NOTIFICACAO_ESUS")) \
            .withColumn("MUNICIPIO_RESIDENCIA_PESSOA", F.upper("MUNICIPIO_RESIDENCIA_PESSOA")) \
            .withColumn("MUNICIPIO_NOTIFICACAO_ESUS", F.upper("MUNICIPIO_NOTIFICACAO_ESUS")) \
            .withColumn("IDADE_PESSOA", F.col("IDADE_PESSOA").cast(IntegerType()))

        return dataframe
    
    def categorize_doses_data(self, dataframe: DataFrame) -> DataFrame:
        """
        Method to categorize doses data from an individual.
        This method returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """
        
        dataframe = dataframe \
        .withColumn("TEM_PRIMEIRA_DOSE_PESSOA", 
            F.when((dataframe.codigoDosesVacina[0] == "1") |  (dataframe.codigoDosesVacina[1] == "1") | (dataframe.codigoDosesVacina[2] == "1"), "S").otherwise("N")) \
        .withColumn("TEM_SEGUNDA_DOSE_PESSOA", 
            F.when((dataframe.codigoDosesVacina[0] == "2") |  (dataframe.codigoDosesVacina[1] == "2") | (dataframe.codigoDosesVacina[2] == "2"), "S").otherwise("N")) \
        .withColumn("TEM_TERCEIRA_DOSE_PESSOA", 
            F.when((dataframe.codigoDosesVacina[0] == "3") |  (dataframe.codigoDosesVacina[1] == "3") | (dataframe.codigoDosesVacina[2] == "3"), "S").otherwise("N"))
        
        return dataframe
    
    def get_last_testing_data(self, dataframe: DataFrame) -> DataFrame:
        """
        Method to get data from the last COVID test an individual has taken.
        This method returns a dataframe type.

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
    
    def categorize_classification_and_evolution(self, dataframe: DataFrame) -> DataFrame:
        """
        Method to categorize columns related to classification and evolution of cases.
        It will create 13 new categoric columns based on the values that those columns have.
        This Method returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """

        dataframe = dataframe \
        .withColumn("CLASSIFICACAO_FINAL_CONFIRMACAO_LABORATORIO_ESUS", 
            F.when(dataframe.classificacaoFinal.contains("Confirmado Laboratorial"), "S").otherwise("N")) \
        .withColumn("CLASSIFICACAO_FINAL_CONFIRMACAO_CLINICO_EPIDEMIOLOGICO_ESUS", 
            F.when(dataframe.classificacaoFinal.contains("Confirmado Clínico-Epidemiológico"), "S").otherwise("N")) \
        .withColumn("CLASSIFICACAO_FINAL_DESCARTADO_ESUS", 
            F.when(dataframe.classificacaoFinal.contains("Descartado"), "S").otherwise("N")) \
        .withColumn("CLASSIFICACAO_FINAL_SINDROME_GRIPAL_NAO_ESPECIFICADA_ESUS", 
            F.when(dataframe.classificacaoFinal.contains("Síndrome Gripal"), "S").otherwise("N")) \
        .withColumn("CLASSIFICACAO_FINAL_CONFIRMACAO_CLINICO_IMAGEM_ESUS", 
            F.when(dataframe.classificacaoFinal.contains("Confirmado Clínico-Imagem"), "S").otherwise("N")) \
        .withColumn("CLASSIFICACAO_FINAL_CONFIRMACAO_CRITERIO_CLINICO_ESUS", 
            F.when(dataframe.classificacaoFinal.contains("Confirmado por Critério Clínico"), "S").otherwise("N")) \
        .withColumn("EVOLUCAO_CASO_CANCELADO_ESUS", 
            F.when(dataframe.evolucaoCaso.contains("Cancelado"), "S").otherwise("N")) \
        .withColumn("EVOLUCAO_CASO_IGNORADO_ESUS", 
            F.when(dataframe.evolucaoCaso.contains("Ignorado"), "S").otherwise("N")) \
        .withColumn("EVOLUCAO_CASO_TRATAMENTO_DOMICILIAR_ESUS", 
            F.when(dataframe.evolucaoCaso.contains("Em tratamento domiciliar"), "S").otherwise("N")) \
        .withColumn("EVOLUCAO_CASO_INTERNADO_UTI_ESUS", 
            F.when(dataframe.evolucaoCaso.contains("Internado em UTI"), "S").otherwise("N")) \
        .withColumn("EVOLUCAO_CASO_INTERNADO_ESUS", 
            F.when(dataframe.evolucaoCaso.contains("Internado"), "S").otherwise("N")) \
        .withColumn("EVOLUCAO_CASO_OBITO_ESUS", 
            F.when(dataframe.evolucaoCaso.contains("Óbito"), "S").otherwise("N")) \
        .withColumn("EVOLUCAO_CASO_CURA_ESUS", 
            F.when(dataframe.evolucaoCaso.contains("Cura"), "S").otherwise("N"))

        return dataframe

    def categorize_populational_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Method to categorize populational columns. It will create 17 new categoric columns based on the values that the populational columns have.
        The populational columns refer to population, communities and trial as a whole, all related to COVID symptoms.
        This Method returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """

        dataframe = dataframe \
        .withColumn("MEMBRO_COMUNIDADE_TRADICIONAL_PESSOA", 
            F.when(dataframe.codigoContemComunidadeTradicional == "1", "S").otherwise("N")) \
        .withColumn("TRIAGEM_TRABALHADOR_SERVICOS_ESSENCIAIS_ESUS",
            F.when(dataframe.codigoTriagemPopulacaoEspecifica.contains("Trabalhadores de serviços essenciais"), "S").otherwise("N")) \
        .withColumn("TRIAGEM_PROFISSIONAL_SAUDE_ESUS",
            F.when(dataframe.codigoTriagemPopulacaoEspecifica.contains("Profissionais de saúde"), "S").otherwise("N")) \
        .withColumn("TRIAGEM_GESTANTE_OU_PUERPERA_ESUS",
            F.when(dataframe.codigoTriagemPopulacaoEspecifica.contains("Gestantes e puérperas"), "S").otherwise("N")) \
        .withColumn("TRIAGEM_POVOS_OU_COMUNIDADE_TRADICIONAL_ESUS",
            F.when(dataframe.codigoTriagemPopulacaoEspecifica.contains("Povos e comunidades tradicionais"), "S").otherwise("N")) \
        .withColumn("TRIAGEM_OUTROS_ESUS",
            F.when(dataframe.codigoTriagemPopulacaoEspecifica.contains("Outros"), "S").otherwise("N")) \
        .withColumn("LOCAL_TESTAGEM_SERVICOS_SAUDE_ESUS",
            F.when(dataframe.codigoLocalRealizacaoTestagem.contains("Serviço de saúde"), "S").otherwise("N")) \
        .withColumn("LOCAL_TESTAGEM_TRABALHO_ESUS",
            F.when(dataframe.codigoLocalRealizacaoTestagem.contains("Local de trabalho"), "S").otherwise("N")) \
        .withColumn("LOCAL_TESTAGEM_AEROPORTO_ESUS",
            F.when(dataframe.codigoLocalRealizacaoTestagem.contains("Aeroporto"), "S").otherwise("N")) \
        .withColumn("LOCAL_TESTAGEM_FARMACIA_ESUS",
            F.when(dataframe.codigoLocalRealizacaoTestagem.contains("Farmácia"), "S").otherwise("N")) \
        .withColumn("LOCAL_TESTAGEM_ESCOLA_ESUS",
            F.when(dataframe.codigoLocalRealizacaoTestagem.contains("Escola"), "S").otherwise("N")) \
        .withColumn("LOCAL_TESTAGEM_DOMICILIO_OU_COMUNIDADE_ESUS",
            F.when(dataframe.codigoLocalRealizacaoTestagem.contains("Domicílio"), "S").otherwise("N")) \
        .withColumn("LOCAL_TESTAGEM_OUTROS_ESUS",
            F.when(dataframe.codigoLocalRealizacaoTestagem.contains("Outro"), "S").otherwise("N")) \
        .withColumn("DECLARADO_ESTRANGEIRO_PESSOA", 
            F.when(dataframe.estrangeiro == "Sim", "S").otherwise("N")) \
        .withColumn("DECLARADO_PROFISSIONAL_SAUDE_PESSOA", 
            F.when(dataframe.profissionalSaude == "Sim", "S").otherwise("N")) \
        .withColumn("DECLARADO_PROFISSIONAL_SEGURANCA_PESSOA", 
            F.when(dataframe.profissionalSeguranca == "Sim", "S").otherwise("N")) \
        .withColumn("SEXO_PESSOA", 
            F.when(dataframe.sexo == "Feminino", "F").otherwise("M"))

        return dataframe

    def categorize_strategy_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Method to categorize strategy columns. It will create 7 new categoric columns based on the values that the strategy columns have.
        The strategy columns refer to strategy as a whole and to search of people without COVID symptoms.
        This Method returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """

        dataframe = dataframe \
        .withColumn("ESTRATEGIA_DIAGNOSTICO_ASSISTENCIAL_ESUS", 
            F.when(dataframe.codigoEstrategiaCovid.contains("Diagnóstico assistencial"), "S").otherwise("N")) \
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
        Method to categorize symptoms column. It will create 9 new categoric columns based on the values that the sympton column has.
        This Method returns a dataframe type.

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
        Method to categorize conditions column. It will create 10 new categoric columns based on the values that the conditions column has.
        This Method returns a dataframe type.

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
        """
        Method to categorize test columns. It will create new categoric columns based on the values that the conditions column has.
        This Method returns a dataframe type.

        :param dataframe: Input dataframe to have data changed
        """

        dataframe = dataframe \
        .withColumn("TESTE_ESTADO_SOLICITADO_ESUS", 
            F.when(dataframe.codigoEstadoTeste == "1", "S").otherwise("N")) \
        .withColumn("TESTE_ESTADO_COLETADO_ESUS", 
            F.when(dataframe.codigoEstadoTeste == "2", "S").otherwise("N")) \
        .withColumn("TESTE_ESTADO_CONCLUIDO_ESUS", 
            F.when(dataframe.codigoEstadoTeste == "3", "S").otherwise("N")) \
        .withColumn("TESTE_ESTADO_NAO_SOLICITADO_ESUS", 
            F.when(dataframe.codigoEstadoTeste == "4", "S").otherwise("N")) \
        .withColumn("TESTE_RESULTADO_REAGENTE_ESUS", 
            F.when(dataframe.codigoResultadoTeste == "1", "S").otherwise("N")) \
        .withColumn("TESTE_RESULTADO_NAO_REAGENTE_ESUS", 
            F.when(dataframe.codigoResultadoTeste == "2", "S").otherwise("N")) \
        .withColumn("TESTE_RESULTADO_INCONCLUSIVO_ESUS", 
            F.when(dataframe.codigoResultadoTeste == "3", "S").otherwise("N")) \
        .withColumn("TESTE_TIPO_RTPCR_ESUS", 
            F.when(dataframe.codigoTipoTeste == "1", "S").otherwise("N")) \
        .withColumn("TESTE_TIPO_RTLAMP_ESUS", 
            F.when(dataframe.codigoTipoTeste == "2", "S").otherwise("N")) \
        .withColumn("TESTE_TIPO_RAPIDO_ANTIGENO_ESUS", 
            F.when(dataframe.codigoTipoTeste == "3", "S").otherwise("N")) \
        .withColumn("TESTE_TIPO_RAPIDO_ANTICORPO_IGM_ESUS", 
            F.when(dataframe.codigoTipoTeste == "4", "S").otherwise("N")) \
        .withColumn("TESTE_TIPO_RAPIDO_ANTICORPO_IGG_ESUS", 
            F.when(dataframe.codigoTipoTeste == "5", "S").otherwise("N")) \
        .withColumn("TESTE_TIPO_SOROLOGICO_IGA_ESUS", 
            F.when(dataframe.codigoTipoTeste == "6", "S").otherwise("N")) \
        .withColumn("TESTE_TIPO_SOROLOGICO_IGM_ESUS", 
            F.when(dataframe.codigoTipoTeste == "7", "S").otherwise("N")) \
        .withColumn("TESTE_TIPO_SOROLOGICO_IGG_ESUS", 
            F.when(dataframe.codigoTipoTeste == "8", "S").otherwise("N")) \
        .withColumn("TESTE_TIPO_ANTICORPO_TOTAL_ESUS", 
            F.when(dataframe.codigoTipoTeste == "9", "S").otherwise("N"))

        return dataframe
    
    def rename_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Method to drop redundant columns.
        This Method returns a dataframe type.

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
                                .withColumnRenamed("dataEncerramento", "DATA_ENCERRAMENTO_NOTIFICACAO_ESUS") \
                                .withColumnRenamed("cbo", "OCUPACAO_PESSOA") \
                                .withColumnRenamed("idade", "IDADE_PESSOA") \
                                .withColumnRenamed("dataColetaTeste", "TESTE_DATA_COLETA_ESUS") \
                                .withColumnRenamed("loteTeste", "TESTE_LOTE_ESUS") \
                                .withColumnRenamed("fabricanteTeste", "TESTE_FABRICANTE_ESUS")

        return dataframe
    
    def drop_redundant_and_unused_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Method to drop redundant columns.
        This Method returns a dataframe type.

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
                                    "codigoBuscaAtivaAssintomatico",
                                    "codigoContemComunidadeTradicional",
                                    "estrangeiro",
                                    "codigoTriagemPopulacaoEspecifica",
                                    "codigoLocalRealizacaoTestagem"
                                    "outroBuscaAtivaAssintomatico",
                                    "outroTriagemPopulacaoEspecifica",
                                    "outroLocalRealizacaoTestagem",
                                    "profissionalSeguranca",
                                    "profissionalSaude",
                                    "sexo",
                                    "resultadoTesteSorologicoIgA",
                                    "resultadoTesteSorologicoIgG",
                                    "resultadoTesteSorologicoIgM",
                                    "resultadoTesteSorologicoTotais",
                                    "tipoTesteSorologico",
                                    "estadoTeste",
                                    "fabricanteTeste",
                                    "resultadoTeste",
                                    "tipoTeste",
                                    "codigoEstadoTeste",
                                    "codigoFabricanteTeste",
                                    "codigoResultadoTeste",
                                    "codigoTipoTeste",
                                    "classificacaoFinal",
                                    "evolucaoCaso",
                                    "@version",
                                    "id",
                                    "racaCor",
                                    "idCollection"
                                    )

        return dataframe
    
    def write_dataframe_to_silver_folder(self, dataframe: DataFrame, uf: str) -> DataFrame:
        """
        Method to save
        This munction returns only 10 thousand registers from Tabnet

        :param dataframe: Input dataframe that is going to be saved
        """

        today = datetime.datetime.now()
        dt = today.strftime("%d_%m_%Y_%H_%M_%S")
        output_name = 'esus_modeled_data_' + uf + '_' + dt + '.parquet'
        output_dir = 'silver_data'

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        dataframe.write.parquet(f"{output_dir}/{output_name}")

        return print("Dataframe saved to desired path")

    def write_dataframe_to_gold_folder(self, dataframe: DataFrame, uf: str) -> DataFrame:
        """
        Method to save
        This munction returns only 10 thousand registers from Tabnet

        :param dataframe: Input dataframe that is going to be saved
        """

        today = datetime.datetime.now()
        dt = today.strftime("%d_%m_%Y_%H_%M_%S")
        output_name = 'esus_final_data_' + uf + '_' + dt + '.parquet'
        output_dir = 'gold_data'

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        dataframe.write.parquet(f"{output_dir}/{output_name}")

        return print("Dataframe saved to desired path")

    def read_json_into_dataframe(self, spark: SPARK, path: str) -> DataFrame:
        """
        Method to read json and make it a dataframe using Pyspark.
        This method returns a dataframe type.

        :param spark: Spark configuration session. Please refer to spark docs when building one.
        :param path: Desired Json path to be read.
        """
        dataframe = spark.read.json(path)
        
        return dataframe
    
    def __init__(self) -> None:
        """Init method to call class"""
        pass
        