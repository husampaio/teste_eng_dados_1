import logging
from typing import List
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

try:
    from utils.exceptions import (
        NegativeValueError,
        NullValueError,
        UniqueError,
        OutdatedDatasetError,
    )
except ModuleNotFoundError:
    from DataQuality.utils.exceptions import (
        NegativeValueError,
        NullValueError,
        UniqueError,
        OutdatedDatasetError,
    )  # SOLUTION TO RUN TESTS


class Functions:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    @staticmethod
    def check_null(df: DataFrame, cols: List[str]):
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
    def check_negative(df: DataFrame, cols: List[str]):
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
    def check_unique(df: DataFrame, cols: List[str]):
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

    def check_process_date(self, df: DataFrame, process_date_col: str):
        """
        Check if the DataFrame is outdated

        Parameters:
            df (DataFrame):
                Spark DataFrame
            cols (str):
                Column name

        :raises OutdatedDatasetError: if dataset is outdated
        """
        df_test = df.select(F.max(process_date_col).alias(process_date_col)).filter(
            F.datediff(F.current_date(), F.to_date(F.col(process_date_col))) > 1
        )

        if not df_test.isEmpty():
            raise OutdatedDatasetError(f"This dataset is outdated!")
