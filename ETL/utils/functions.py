from dataclasses import dataclass
from typing import List
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F


@dataclass
class Functions:
    @staticmethod
    def cols_to_upper(df: DataFrame, cols: List[str]) -> DataFrame:
        """
        Upper case column values

        Parameters:
            df (DataFrame):
                Spark DataFrame
            cols (List[str]):
                Column names

        Returns:
            DataFrame:
            DataFrame with upper cased columns.
        """
        return df.withColumns({col: F.upper(col) for col in cols})

    @staticmethod
    def add_processing_date(df: DataFrame) -> DataFrame:
        """
        Add processing date

        Parameters:
            df (DataFrame):
                Spark DataFrame

        Returns:
            DataFrame:
            DataFrame with process date.
        """
        return df.withColumn(
            "dt_processamento", F.date_format(F.current_date(), "yyyyMMdd")
        )

    @staticmethod
    def keep_last_record(
        df: DataFrame, partition_col: List[str], dt_ref: str
    ) -> DataFrame:
        """
        Keep just the last record based on specific column

        Parameters:
            df (DataFrame):
                Spark DataFrame
            partition_col (List[str]):
                Column that will be used as key to check most recent record
            dt_ref (str):
                Column that will be used to order values

        Returns:
            DataFrame:
            DataFrame with the most recent record.
        """
        w_spec = Window.partitionBy(partition_col).orderBy(F.col(dt_ref).desc())

        df_last_record = (
            df.withColumn("rn", F.row_number().over(w_spec))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        return df_last_record

    @staticmethod
    def treat_phone(df: DataFrame) -> DataFrame:
        """
        Treat the phone number column, allowing just values that follow the pattern `(XX)XXXXX-XXXX`, values that
        not follow this pattern will be replaced with NULL.

        Parameters:
            df (DataFrame):
                Spark DataFrame

        Returns:
            DataFrame:
            DataFrame treated.
        """
        df_treated = df.withColumn(
            "num_telefone_cliente",
            F.when(
                F.regexp_like(
                    F.col("num_telefone_cliente"), F.lit(r"\((\d{2})\)\d{5}-\d{4}")
                ),
                F.col("num_telefone_cliente"),
            ),
        )

        return df_treated
