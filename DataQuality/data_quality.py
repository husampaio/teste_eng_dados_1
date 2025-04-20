from typing import List
from pyspark.sql import SparkSession
from utils.functions import Functions
from pyspark.sql import DataFrame
import logging


class DataQuality(Functions):
    def __init__(
        self,
        df_to_test: DataFrame,
        not_null_cols: List[str] = None,
        not_negative_cols: List[str] = None,
        unique_values_cols: List[str] = None,
    ):
        self.df_to_test = df_to_test
        self.not_null_cols = not_null_cols
        self.not_negative_cols = not_negative_cols
        self.unique_values_cols = unique_values_cols
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    def run(self):

        if self.not_null_cols:
            self.check_null(self.df_to_test, self.not_null_cols)
        if self.not_negative_cols:
            self.check_negative(self.df_to_test, self.not_negative_cols)
        if self.unique_values_cols:
            self.check_unique(self.df_to_test, self.unique_values_cols)

        self.logger.info("Data quality success!")


if __name__ == "__main__":
    spark: SparkSession = SparkSession.builder.appName("data_quality").getOrCreate()

    df_to_test = spark.read.parquet("silver_layer/clients")

    DataQuality(
        df_to_test=df_to_test,
        not_null_cols=["cod_cliente", "tp_pessoa"],
        not_negative_cols=["vl_renda"],
        unique_values_cols=["cod_cliente"],
    ).run()
