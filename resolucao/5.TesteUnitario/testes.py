import pytest
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from spark_etl import Utils  # Substitua pelo nome correto do seu arquivo se for diferente

"""----------  FIXTURE DE SESSÃO SPARK ----------"""

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("Testes ETL") \
        .getOrCreate()


"""---------- TESTE HAPPY PATH ----------"""

def test_tratar_bronze(spark):
    """
    Testa o processamento do DataFrame na camada bronze com dados válidos.
    Verifica se as transformações de nome e telefone foram aplicadas corretamente
    e se a coluna de partição 'anomesdia' foi criada corretamente.
    """
        
    schema = StructType([
        StructField("cod_cliente", IntegerType(), True),
        StructField("nm_cliente", StringType(), True),
        StructField("nm_pais_cliente", StringType(), True),
        StructField("nm_cidade_cliente", StringType(), True),
        StructField("nm_rua_cliente", StringType(), True),
        StructField("num_casa_cliente", IntegerType(), True),
        StructField("telefone_cliente", StringType(), True),
        StructField("dt_nascimento_cliente", StringType(), True),
        StructField("dt_atualizacao", StringType(), True),
        StructField("tp_pessoa", StringType(), True),
        StructField("vl_renda", DoubleType(), True),
        StructField("anomesdia", StringType(), True)
    ])

    df = spark.createDataFrame([
        (1, "joão da silva", "Brasil", "SP", "Rua A", 10, "(11)91234-5678", None, "2024-10-01", "F", 3000.00, None)
    ], schema=schema)

    resultado = Utils.tratar_bronze(df).collect()[0]

    assert resultado.nm_cliente == "JOÃO DA SILVA"
    assert resultado.num_telefone_cliente == "(11)91234-5678"
    assert resultado.anomesdia == "20241001"


"""---------- TESTE BORDA --------------------"""

def test_limpeza_telefone_formatos_errados(spark):
    """
    Testa a função de limpeza de telefone com diferentes formatos de entrada.
    Verifica se os telefones mal formatados são corrigidos para None.
    """

    df = spark.createDataFrame([
        ("11912345678",),
        ("(11)912345678",),
        ("",),
        (None,)
    ], ["num_telefone_cliente"])

    resultado = Utils.limpeza_telefone(df, "num_telefone_cliente").collect()
    for row in resultado:
        assert row.num_telefone_cliente is None


"""---------- TESTE EXCEÇÃO --------------------"""

def test_limpeza_telefone_coluna_inexistente(spark):
    """
    Testa a função de limpeza de telefone quando a coluna não existe.
    Verifica se uma exceção é lançada.
    """

    df = spark.createDataFrame([
        ("(11)91234-5678",)
    ], ["col_telefone_invalido"])

    with pytest.raises(Exception):
        Utils.limpeza_telefone(df, "num_telefone_cliente").collect()
