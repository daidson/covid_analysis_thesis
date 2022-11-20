import pytest

from sqlalchemy import null
from ingestion.ingest_esus import EsusApiIngestion

import os
from dotenv import load_dotenv
from pathlib import Path

from tests import SPARK
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType


load_dotenv(dotenv_path=Path('.env'))

SCHEMA = StructType([
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
            StructField("testes", ArrayType(StringType()), True),
            StructField("resultadoTesteSorologicoIgA", StringType(), True),
            StructField("estadoTeste", StringType(), True),
            StructField("estadoNotificacao", StringType(), True),
            StructField("outrasCondicoes", StringType(), True),
            StructField("resultadoTeste", StringType(), True),
            StructField("profissionalSaude", StringType(), True),
            StructField("profissionalSeguranca", StringType(), True),
        ])

SAMPLE_DATA_ONE = [
    None,
    "2022-08-12T03:00:00.375Z",
    None,
    26,
    "2021-04-13T03:00:00.000Z",
    "Cabo de Santo Agostinho",
    None,
    "Masculino",
    None,
    "Pernambuco",
    None,
    None,
    None,
    "Parda",
    None,
    None,
    "2602902",
    1,
    None,
    "Cura",
    52,
    None,
    "Pernambuco",
    "Não",
    1,
    None,
    None,
    "2022-08-11T03:00:00.000Z",
    None,
    "2021-05-07T03:00:00.000Z",
    26,
    ["{loteTeste=FMFA01211, resultadoTeste=Reagente, codigoResultadoTeste=1, estadoTeste=Concluído, fabricanteTeste=BIO-MANGUINHOS/FIOCRUZ, codigoTipoTeste=3, dataColetaTeste={iso=2022-07-27T03:00:00.000Z, __type=Date}, codigoFabricanteTeste=792, codigoEstadoTeste=3, tipoTeste=TESTE RÁPIDO - ANTÍGENO}"],
    "Cabo de Santo Agostinho",
    "Confirmado Laboratorial",
    True,
    ['3','1','2']
] 

SAMPLE_DATA_TWO = [
    None,
	"2022-08-12T03:00:00.404Z",
	None,
	26,
	"2021-06-11T03:00:00.000Z",
	"Cabo de Santo Agostinho",
	None,
	"Masculino",
	None,
	"Pernambuco",
	None,
	None,
	None,
	"Parda",
	None,
	None,
	2602902,
	1,
	None,
	"Cura",
	52,
	None,
	"Pernambuco",
	"Não",
	1,
	None,
	None,
	"2022-08-11T03:00:00.000Z",
	None,
	"2021-09-11T03:00:00.000Z",
	26,
	["{loteTeste=SEM INFORMACAO, resultadoTeste=Não Reagente, codigoResultadoTeste=2, estadoTeste=Concluído, fabricanteTeste=OUTRAS MARCAS, codigoTipoTeste=3, dataColetaTeste={iso=2022-01-19T03:00:00.000Z, __type=Date}, codigoFabricanteTeste=915, codigoEstadoTeste=3, tipoTeste=TESTE RÁPIDO - ANTÍGENO}"],
	"Cabo de Santo Agostinho",
	"Descartado",
	True,
	['2','3','1']
]


@pytest.mark.skip(reason="testing data ingestion with spark")
def test_should_validate_env_variables() -> None:
    mock_env_dict = {
                'ESUS_USER':'user-public-notificacoes',
                'ESUS_DATABASE':'desc-esus-notifica-estado-pe',
                'ESUS_URL':'https://user-public-notificacoes:Za4qNXdyQNSa9YaA@elasticsearch-saps.saude.gov.br'
                }
    env_dict = {
                'ESUS_USER':os.getenv('ESUS_USER'),
                'ESUS_DATABASE':os.getenv('ESUS_DATABASE') + 'pe',
                'ESUS_URL':os.getenv('ESUS_URL')
                }
    assert mock_env_dict == env_dict

@pytest.mark.skip(reason="testing consumption")
def test_should_ingest_sus_data() -> None:
    eai = EsusApiIngestion()
    schema = eai.define_ingestion_schema()
    requested_dataframe = eai.ingest_covid_data(spark=SPARK, schema=schema, uf='pe')

    assert requested_dataframe is not null

@pytest.mark.skip(reason="this test should be useful for the data consumption as well")
def test_should_maintain_schema() -> None:
    actual_dataframe = SPARK.read.parquet(os.getenv('ACTUAL_DATAFRAME_PATH'))
    actual_columns = set(actual_dataframe.columns)
    expected_dataframe = SPARK.read.parquet(os.getenv('EXPECTED_DATAFRAME_PATH'))
    expected_columns = set(expected_dataframe.columns)
    
    assert expected_columns == actual_columns

@pytest.mark.skip(reason="this test should be useful for the data consumption as well")
def test_should_maintain_data() -> None:
    actual_dataframe = SPARK.read.parquet(os.getenv('ACTUAL_DATAFRAME_PATH'))
    expected_dataframe = SPARK.createDataFrame(
        [
            SAMPLE_DATA_ONE,
            SAMPLE_DATA_TWO
        ],
        SCHEMA
    )
    assert expected_dataframe.collect() == actual_dataframe.take(2)

# @pytest.mark.skip(reason="use this test to write dataframe")
def test_should_write_dataframe() -> None:
    eai = EsusApiIngestion()
    schema = eai.define_ingestion_schema()
    eai.write_ingested_dataframe(uf='pe', dataframe=eai.ingest_covid_dataframe(spark=SPARK, schema=schema, uf='pe'))

    assert None == None

@pytest.mark.skip(reason="use this test to write sample dataframe")
def test_should_write_sample_dataframe() -> None:
    eai = EsusApiIngestion()
    schema = eai.define_ingestion_schema()
    eai.write_ingested_dataframe(uf='pe', dataframe=eai.ingest_sample_dataframe(spark=SPARK, schema=schema, uf='pe'))

    assert None == None

@pytest.mark.skip(reason="proof that json can be saved")
def test_should_write_json_sample() -> None:
    eai = EsusApiIngestion()
    eai.write_ingested_json(uf='pe', data=eai.ingest_sample_data_json(uf='pe'))

    assert None == None

@pytest.mark.skip(reason="use this test to write in json")
def test_should_write_json() -> None:
    eai = EsusApiIngestion()
    eai.write_ingested_json(uf='pe', data=eai.ingest_covid_data_json(uf='pe'))

    assert None == None
