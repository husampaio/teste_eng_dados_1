import pytest
from ETL.utils.functions import Functions as etl_functions
from DataQuality.utils.exceptions import NullValueError, NegativeValueError, UniqueError
from DataQuality.utils.functions import Functions as quality_funcions
from pyspark.sql import SparkSession, DataFrame


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("my_test_session").getOrCreate()


def test_upper_case(spark: SparkSession):
    input_data = [{"name": "name_test"}, {"name": "Name_Test2"}]
    expected_output = [{"name": "NAME_TEST"}, {"name": "NAME_TEST2"}]
    df_input: DataFrame = spark.createDataFrame(input_data)
    df_expected: DataFrame = spark.createDataFrame(expected_output)

    df_output = etl_functions.cols_to_upper(df_input, cols=["name"])
    assert df_output.collect() == df_expected.collect()


def test_phone_number(spark: SparkSession):
    input_data = [
        {"num_telefone_cliente": "(55)22231-4567"},
        {"num_telefone_cliente": "000x345.343214ABC"},
    ]
    expected_output = [
        {"num_telefone_cliente": "(55)22231-4567"},
        {"num_telefone_cliente": None},
    ]
    df_input: DataFrame = spark.createDataFrame(input_data)
    df_expected: DataFrame = spark.createDataFrame(expected_output)

    df_output = etl_functions.treat_phone(df_input)
    assert df_output.collect() == df_expected.collect()


def test_raises(spark: SparkSession):

    input_data = [
        {"cod_cliente": 337, "vl_renda": 5000.0},
        {"cod_cliente": None, "vl_renda": 3200.0},
        {"cod_cliente": 337, "vl_renda": -5000.0},
    ]

    df_input: DataFrame = spark.createDataFrame(input_data)

    with pytest.raises(NullValueError):
        quality_funcions.check_null(df_input, ["cod_cliente"])

    with pytest.raises(NegativeValueError):
        quality_funcions.check_negative(df_input, ["vl_renda"])

    with pytest.raises(UniqueError):
        quality_funcions.check_unique(df_input, ["cod_cliente"])
