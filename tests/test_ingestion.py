import pytest

from sqlalchemy import null
from ingestion.ingest_esus import EsusApiIngestion

import os
from dotenv import load_dotenv
from pathlib import Path

from tests import SPARK

load_dotenv(dotenv_path=Path('.env'))

BASE_COLUMNS = [
    "resultadoTesteSorologicoIgM",
    "@timestamp",
    "resultadoTesteSorologicoIgG",
    "estadoNotificacaoIBGE",
    "dataPrimeiraDose",
    "municipio",
    "outrasCondicoes",
    "sexo",
    "codigoBuscaAtivaAssintomatico",
    "estado",
    "dataInicioSintomas",
    "resultadoTesteSorologicoTotais",
    "estrangeiro",
    "racaCor",
    "dataTesteSorologico",
    "codigoTriagemPopulacaoEspecifica",
    "municipioNotificacaoIBGE",
    "codigoRecebeuVacina",
    "outroBuscaAtivaAssintomatico",
    "evolucaoCaso",
    "idade",
    "idcodigoLocalRealizacaoTestagemade",
    "estadoNotificacao",
    "profissionalSeguranca",
    "@version",
    "resultadoTesteSorologicoIgA",
    "tipoTeste",
    "dataEncerramento",
    "estadoTeste",
    "dataSegundaDose",
    "estadoIBGE",
    "testes",
    "municipioNotificacao",
    "classificacaoFinal",
    "registroAtual",
    "codigoDosesVacina"
]

SAMPLE_DATA_ONE = [
    None,
    "2022-08-12T03:00:00.375Z",
    "",
    "26",
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
    "52",
    None,
    "Pernambuco",
    "Não",
    "1",
    None,
    None,
    "2022-08-11T03:00:00.000Z",
    None,
    "2021-05-07T03:00:00.000Z",
    "26",
    ["{loteTeste=FMFA01211,resultadoTeste=Reagente,codigoResultadoTeste=1,estadoTeste=Concluído,fabricanteTeste=BIO-MANGUINHOS/FIOCRUZ,codigoTipoTeste=3,dataColetaTeste={iso=2022-07-27T03:00:00.000Z,__type=Date},codigoFabricanteTeste=792,codigoEstadoTeste=3,tipoTeste=TESTE RÁPIDO - ANTÍGENO}"],
    "Cabo de Santo Agostinho",
    "Confirmado Laboratorial",
    True,
    ['3','1','2']
] 

SAMPLE_DATA_TWO = [
    "",
	"2022-08-12T03:00:00.404Z",
	"",
	26,
	"2021-06-11T03:00:00.000Z",
	"Cabo de Santo Agostinho",
	"",
	"Masculino",
	"",
	"Pernambuco",
	"",
	"",
	"",
	"Parda",
	"",
	"",
	2602902,
	1,
	"",
	"Cura",
	52,
	"",
	"Pernambuco",
	"Não",
	1,
	"",
	"",
	"2022-08-11T03:00:00.000Z",
	"",
	"2021-09-11T03:00:00.000Z",
	26,
	"[{loteTeste=SEM INFORMACAO,resultadoTeste=Não Reagente,codigoResultadoTeste=2,estadoTeste=Concluído,fabricanteTeste=OUTRAS MARCAS,codigoTipoTeste=3,dataColetaTeste={iso=2022-01-19T03:00:00.000Z,__type=Date},codigoFabricanteTeste=915,codigoEstadoTeste=3,tipoTeste=TESTE RÁPIDO - ANTÍGENO}]",
	"Cabo de Santo Agostinho",
	"Descartado",
	True,
	"[2,3,1]"
]


@pytest.mark.skip(reason="testing data ingestion with spark")
def test_should_validate_env_variables() -> None:
    mock_env_dict = {'USER':'user-public-notificacoes',
                'DATABASE':'desc-esus-notifica-estado-pe',
                'URL':'https://user-public-notificacoes:Za4qNXdyQNSa9YaA@elasticsearch-saps.saude.gov.br'}
    env_dict = {'USER':os.getenv('USER'),
                'DATABASE':os.getenv('DATABASE') + 'pe',
                'URL':os.getenv('URL')}
    assert mock_env_dict == env_dict

@pytest.mark.skip(reason="testing consumption")
def test_should_ingest_sus_data() -> None:
    eai = EsusApiIngestion()
    schema = eai.define_ingestion_schema()
    requested_dataframe = eai.ingest_covid_data(spark=SPARK, schema=schema, uf='pe')

    assert requested_dataframe is not null

@pytest.mark.skip(reason="testing data")
def test_should_maintain_schema() -> None:
    actual_dataframe = SPARK.read.parquet('E:\\dfa-dev\\TCC\\tcc_code\\covid-project\\ingested_data\\esus_data_pe_12_08_2022_00_31_50.parquet')
    actual_columns = set(actual_dataframe.columns)
    expected_dataframe = SPARK.read.parquet('E:\\dfa-dev\\TCC\\tcc_code\\covid-project\\ingested_data\\sample\\parquet_sample_pe_17_08_2022_23_02_22.parquet')
    expected_columns = set(expected_dataframe.columns)
    
    assert expected_columns == actual_columns

@pytest.mark.skip(reason="testing consumption")
def test_should_maintain_data() -> None:
    actual_dataframe = SPARK.read.parquet('E:\\dfa-dev\\TCC\\tcc_code\\covid-project\\ingested_data\\esus_data_pe_12_08_2022_00_31_50.parquet')
    expected_dataframe = SPARK.createDataFrame(
        [
            SAMPLE_DATA_ONE
            # SAMPLE_DATA_TWO
        ],
        BASE_COLUMNS
    )
    assert expected_dataframe.collect() == actual_dataframe.take(1)

@pytest.mark.skip(reason="testing data ingestion with spark")
def test_should_write_dataframe() -> None:
    eai = EsusApiIngestion()
    schema = eai.define_ingestion_schema()
    eai.write_ingested_data(uf='pe', dataframe=eai.ingest_covid_data(spark=SPARK, schema=schema, uf='pe'))

    assert None == None

