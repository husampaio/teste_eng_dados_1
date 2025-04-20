from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    DoubleType,
)

CLIENTS_SCHEMA = StructType(
    [
        StructField("cod_cliente", IntegerType(), False),
        StructField("nm_cliente", StringType(), True),
        StructField("nm_pais_cliente", StringType(), True),
        StructField("nm_cidade_cliente", StringType(), True),
        StructField("nm_rua_cliente", StringType(), True),
        StructField("num_casa_cliente", StringType(), True),
        StructField("telefone_cliente", StringType(), True),
        StructField("dt_nascimento_cliente", DateType(), True),
        StructField("dt_atualizacao", DateType(), True),
        StructField("tp_pessoa", StringType(), True),
        StructField("vl_renda", DoubleType(), True),
    ]
)

CLIENTS_COLUMNS = (
    "cod_cliente",
    "nm_cliente",
    "nm_pais_cliente",
    "nm_cidade_cliente",
    "nm_rua_cliente",
    "num_casa_cliente",
    "telefone_cliente as num_telefone_cliente",
    "dt_nascimento_cliente",
    "dt_atualizacao",
    "tp_pessoa",
    "vl_renda",
    "dt_processamento",
)
