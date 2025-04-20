from pyspark.sql import SparkSession
from utils.functions import Functions
from utils.constants import CLIENTS_SCHEMA, CLIENTS_COLUMNS
import os


class TransformClients(Functions):
    def __init__(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "FAKEACCESSKEY"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "FAKESECRETACCESSKEY"

        self.spark: SparkSession = (
            SparkSession.builder.appName("teste_itau")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
            .config(
                "spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate()
        )

    def job(self):
        df_bronze = (
            self.spark.read.csv(
                "datasets/clientes_sinteticos.csv",
                header=True,
                sep=",",
                schema=CLIENTS_SCHEMA,
            )
            .transform(self.cols_to_upper, cols=["nm_cliente"])
            .transform(self.add_processing_date)
            .selectExpr(*CLIENTS_COLUMNS)
        )

        df_bronze.write.partitionBy("dt_processamento").format("parquet").mode(
            "overwrite"
        ).save("s3a://bucket-bronze/tabela_cliente_landing")

        df_silver = df_bronze.transform(
            self.keep_last_record,
            partition_col=["cod_cliente"],
            dt_ref="dt_atualizacao",
        ).transform(self.treat_phone)

        df_silver.write.partitionBy("dt_processamento").format("parquet").mode(
            "overwrite"
        ).save("s3a://bucket-silver/tb_cliente")


if __name__ == "__main__":
    TransformClients().job()
