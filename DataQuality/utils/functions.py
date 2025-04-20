from dataclasses import dataclass
from typing import List
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

try:
    from utils.exceptions import NegativeValueError, NullValueError, UniqueError
except ModuleNotFoundError:
    from DataQuality.utils.exceptions import (
        NegativeValueError,
        NullValueError,
        UniqueError,
    )  # SOLUTION TO RUN TESTS


@dataclass
class Functions:
    @staticmethod
    def check_null(df: DataFrame, cols: List[str]) -> DataFrame:
        """
        Check if the DataFrame has null values on specified columns

        Parameters:
            df (DataFrame):
                Spark DataFrame
            cols (List[str]):
                Column names

        :raises NullValueError: if column has null values
        """
        for col in cols:
            df_test = df.filter(F.col(col).isNull())
            if not df_test.isEmpty():
                raise NullValueError(f"Column {col} has null values")

    @staticmethod
    def check_negative(df: DataFrame, cols: List[str]) -> DataFrame:
        """
        Check if the DataFrame has negative values on specified columns

        Parameters:
            df (DataFrame):
                Spark DataFrame
            cols (List[str]):
                Column names

        :raises NegativeValueError: if column has negative values
        """
        for col in cols:
            df_test = df.filter(F.col(col) < F.lit(0))
            if not df_test.isEmpty():
                raise NegativeValueError(f"Column {col} has negative values")

    @staticmethod
    def check_unique(df: DataFrame, cols: List[str]) -> DataFrame:
        """
        Check if the DataFrame has duplicated values on specified columns

        Parameters:
            df (DataFrame):
                Spark DataFrame
            cols (List[str]):
                Column names

        :raises UniqueError: if column has duplicates values
        """
        for col in cols:
            df_test = df.groupBy(col).agg(F.count(col).alias("qty"))
            df_test = df_test.filter(F.col("qty") > F.lit(1))
            if not df_test.isEmpty():
                raise UniqueError(f"Column {col} has duplicate values")
