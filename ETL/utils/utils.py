from dataclasses import dataclass
from typing import List
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F


@dataclass
class Utils:
    @staticmethod
    def cols_to_upper(df: DataFrame, cols: List[str]) -> DataFrame:
        """
        Upper case column values

        Parameters:
            df: DataFrame
                The Spark DataFrame that will be treated
            cols: List[str]
                Column names

        Returns:
            DataFrame with upper cased columns.
        """
        return df.withColumns({col: F.upper(col) for col in cols})

    @staticmethod
    def add_processing_date(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "dt_processamento", F.date_format(F.current_date(), "yyyyMMdd")
        )

    @staticmethod
    def keep_last_record(
        df: DataFrame, partition_col: List[str], dt_ref: str
    ) -> DataFrame:
        w_spec = Window.partitionBy(partition_col).orderBy(F.col(dt_ref).desc())

        df_last_record = (
            df.withColumn("rn", F.row_number().over(w_spec))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        return df_last_record

    @staticmethod
    def treat_phone(df: DataFrame) -> DataFrame:
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
