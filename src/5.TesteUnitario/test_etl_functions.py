import pytest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../1.ETL')))
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from etl_functions import *

# Função para utilização do Spark nos testes
@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("test_etl_functions").getOrCreate()

# Função para DataFrame de testes
@pytest.fixture
def df_sample(spark):
    data = [(1, "João", "sp", "2022-05-10", "(95)39194-2483"), 
            (2, "Maria", "rj", "2023-04-04", "(42)71167-9960"), 
            (3, "José", "mg", "2024-01-01", "001-595-499-9546x079"),
            (3, "José", "mg", "2025-02-06", None)]
    
    schema = StructType([
        StructField("id_cliente", IntegerType(), False),
        StructField("nm_cliente", StringType(), False),
        StructField("estado", StringType(), False),
        StructField("dt_atualizacao", DateType(), False),
        StructField("telefone", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

# Teste da função columns_to_uppercase
def test_columns_to_uppercase(df_sample):
    df_sample = columns_to_uppercase(df_sample, ["nm_cliente", "estado"])
    first_row = df_sample.select("nm_cliente", "estado").collect()[0]
    assert first_row[0] == "JOÃO"
    assert first_row[1] == "SP"

def test_columns_to_uppercase_wrong_column(df_sample):
    df_sample = columns_to_uppercase(df_sample, ["cidade"])
    assert "cidade" not in df_sample.columns

def test_columns_to_uppercase_empty_df(spark):
    df_sample = spark.createDataFrame([], StructType([]))
    assert columns_to_uppercase(df_sample, ["nm_cliente"]).count() == 0

# Testes da função rename_columns
def test_rename_columns(df_sample):
    df_sample = rename_columns(df_sample, {"estado": "estado_cliente"})
    assert "estado_cliente" in df_sample.columns

def test_rename_columns_invalid_column(df_sample):
    df_sample = rename_columns(df_sample, {"nm_estado": "estado_cliente"})
    assert "estado_cliente" not in df_sample.columns

def test_rename_columns_empty_df(spark):
    df_sample = spark.createDataFrame([], StructType([]))
    assert rename_columns(df_sample, {"estado": "estado_cliente"}).count() == 0

# Testes da função silver_deduplicate_rows
def test_silver_deduplicate_rows(df_sample):
    df_sample = silver_deduplicate_rows(df_sample, ["id_cliente"], ["dt_atualizacao"])
    assert df_sample.count() == 3

def test_silver_deduplicate_rows_empty_df(spark):
    df_sample = spark.createDataFrame([], StructType([]))
    assert silver_deduplicate_rows(df_sample, ["id_cliente"], ["dt_atualizacao"]).count() == 0

def test_silver_deduplicate_rows_invalid_partition(df_sample):
    with pytest.raises(Exception):
        silver_deduplicate_rows(df_sample, ["telefone"], ["dt_atualizacao"])

#Testes da função format_phone
def test_format_phone(df_sample):
    df_sample = format_phone(df_sample, "telefone", r"\(\d{2}\)\d{5}-\d{4}")
    first_row = df_sample.select("telefone").collect()
    assert first_row[0][0] == "(95)39194-2483"
    assert first_row[2][0] == None

def test_format_phone_invalid_column(df_sample):
    with pytest.raises(Exception):
        format_phone(df_sample, "fax_number", r"\(\d{2}\)\d{5}-\d{4}")

def test_format_phone_empty_df(spark):
    df_sample = spark.createDataFrame([], StructType([StructField("telefone", StringType(), True)]))
    assert format_phone(df_sample, "telefone", r"\(\d{2}\)\d{5}-\d{4}").count() == 0

# Testes da função read_csv
def test_read_csv(spark):
    path = "s3://bucket-test/csv_sample.csv"
    sep = ","
    schema = StructType([
        StructField("id_cliente", IntegerType(), False),
        StructField("nm_cliente", StringType(), False),
        StructField("estado", StringType(), False),
        StructField("dt_atualizacao", DateType(), False),
        StructField("telefone", StringType(), True)
    ])
    
    df_sample = read_csv(path, sep, schema)
    
    assert df_sample.schema == schema
    assert df_sample is not None
    assert df_sample.count() > 0

def test_read_csv_invalid_path():
    with pytest.raises(Exception):
        read_csv("s3://wrong-bucket/csv_sample.csv", ",")

def test_read_empty_csv(spark):
    path = "s3://bucket-test/csv_sample_empty.csv"
    sep = ","
    
    df_sample = read_csv(path, sep)
    
    assert df_sample.count() == 0

# Testes da função write_parquet
def test_write_parquet(df_sample):
    output_path = "s3://bucket-test/"
    write_parquet(df_sample, output_path, mode="append", partition_column="dt_atualizacao")
    assert output_path is not None  

def test_write_parquet_empty_df(spark):
    df_sample = spark.createDataFrame([], StructType([]))
    with pytest.raises(Exception):
        write_parquet(df_sample, "s3://bucket-test/", mode="append", partition_column="dt_atualizacao")

def test_write_parquet_invalid_path(df_sample):
    with pytest.raises(Exception):
        write_parquet(df_sample, "s3://wrong-bucket/csv_sample.csv", mode="append", partition_column="dt_atualizacao")

# Testes da função write_glue_parquet
def test_write_glue_parquet(df_sample):
    assert write_glue_parquet(df_sample, None, "s3://bucket-test/", ["dt_atualizacao"], "test_ctx") is None

def test_write_glue_parquet_empty_df(spark):
    df_sample = spark.createDataFrame([], StructType([]))
    assert write_glue_parquet(df_sample, None, "s3://bucket-test/", ["dt_atualizacao"], "test_ctx") is None

def test_write_glue_parquet_invalid_s3_path(df_sample):
    with pytest.raises(Exception):
        write_glue_parquet(df_sample, None, "s3://wrong-bucket/csv_sample.csv", ["dt_atualizacao"], "test_ctx")