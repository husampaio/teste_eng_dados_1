import os
from typing import List
from pyspark.sql import SparkSession
from utils.functions import Functions
from pyspark.sql import DataFrame


class DataQuality(Functions):
    def __init__(
        self,
        df_to_test: DataFrame,
        not_null_cols: List[str] = None,
        not_negative_cols: List[str] = None,
        unique_values_cols: List[str] = None,
    ):
        super().__init__()
        self.df_to_test = df_to_test
        self.not_null_cols = not_null_cols
        self.not_negative_cols = not_negative_cols
        self.unique_values_cols = unique_values_cols

    def run(self):

        if self.not_null_cols:
            self.check_null(self.df_to_test, self.not_null_cols)
        if self.not_negative_cols:
            self.check_negative(self.df_to_test, self.not_negative_cols)
        if self.unique_values_cols:
            self.check_unique(self.df_to_test, self.unique_values_cols)

        self.check_process_date(self.df_to_test, "dt_processamento")

        self.logger.info("Data quality success!")


if __name__ == "__main__":
    os.environ["AWS_ACCESS_KEY_ID"] = "FAKEACCESSKEY"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "FAKESECRETACCESSKEY"

    spark: SparkSession = (
        SparkSession.builder.appName("teste_itau")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    df_to_test = spark.read.parquet("s3a://bucket-silver/tb_cliente")

    DataQuality(
        df_to_test=df_to_test,
        not_null_cols=["cod_cliente", "tp_pessoa", "vl_renda", "nm_cliente"],
        not_negative_cols=["vl_renda"],
        unique_values_cols=["cod_cliente"],
    ).run()
