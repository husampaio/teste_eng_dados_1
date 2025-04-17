from awsglue.utils import getResolvedOptions
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, date_format, upper, col, row_number, desc, when, regexp_replace, count, avg, datediff, current_date
from pyspark.sql.window import Window 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType

spark = (SparkSession.builder
    .appName("ETL") 
    .getOrCreate()
    )


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3PATH'])
                            
csv_path = args["S3PATH"]

#funcoes
class Utils:
    
    @staticmethod
    def limpeza_telefone(df: DataFrame, column: str) -> DataFrame:
        """
        Executa limpeza em campo de telefone seguindo o padrão (NN)NNNNN-NNNN

        :param df: DataFrame a ser tratado
        :param col: Nome da coluna com dados de telefone
        :return df: DataFrame tratado
        """

        return (df.withColumn(
                column,
                when(
                    col(column).rlike(r"^\(\d{2}\)\d{5}-\d{4}$"),
                    col(column)
                    ).otherwise(None)
                )
            )   

    @staticmethod
    def tratar_bronze(df: DataFrame) -> DataFrame:
        """
        Executa tratamentos para escrita em camada bronze criando coluna de particao,
        nome do cliente em letras maiusculas,
        telefone_cliente renomeado para "num_telefone_cliente"

        :return df: DataFrame tratado
        """

        return (df.withColumn(
            "anomesdia",
             date_format(to_date("dt_atualizacao", "yyyy-MM-dd"), "yyyyMMdd")) 
                .withColumn(
                    "nm_cliente", upper(col("nm_cliente"))) 
                .withColumnRenamed(
                    "telefone_cliente", "num_telefone_cliente")
            )
    
    @staticmethod
    def deduplica_set(df: DataFrame, partition_col: str, date_col: str) -> DataFrame:
        """
        Executa limpeza em campo de telefone seguindo o padrão (NN)NNNNN-NNNN

        :param df: DataFrame a ser tratado
        :param coluna: Nome da coluna com dados de telefone
        :return df: DataFrame tratado
        """

        return (df_silver.withColumn(
            "row_number",
                row_number().over(Window.partitionBy('cod_cliente')
                    .orderBy(desc('dt_atualizacao'))
                    )
                ).filter(col("row_number") == 1) 
                .drop("row_number")
            )

####

schema = StructType([
    StructField("cod_cliente", IntegerType(), True),
    StructField("nm_cliente", StringType(), True),
    StructField("nm_pais_cliente", StringType(), True),
    StructField("nm_cidade_cliente", StringType(), True),
    StructField("nm_rua_cliente", StringType(), True),
    StructField("num_casa_cliente", IntegerType(), True),
    StructField("telefone_cliente", StringType(), True),
    StructField("dt_nascimento_cliente", DateType(), True),
    StructField("dt_atualizacao", DateType(), True),
    StructField("tp_pessoa", StringType(), True),
    StructField("vl_renda", DecimalType(10, 2), True),
    StructField("anomesdia", StringType(), True) 
])


"""##################------ TRATAMENTO Bronze --------------#############################################"""

print('Inicio do processamento!')

df_origem = spark.read.csv(csv_path, schema= schema,header=True, inferSchema=True)

df_tratado_bronze = Utils.tratar_bronze(df_origem)

df_silver = df_tratado_bronze

df_dedup = Utils.deduplica_set(df_silver, "cod_cliente", "dt_atualizacao")

df_tratado_silver = Utils.limpeza_telefone(df_dedup, "num_telefone_cliente")


df_tratado_bronze.createTempView("clientes")

spark.sql("""SELECT cod_cliente, COUNT(*) AS total_atualizacoes
FROM clientes
GROUP BY cod_cliente
ORDER BY total_atualizacoes DESC
LIMIT 5"""
).show()

df_tratado_silver.createOrReplaceTempView("clientes_silver")

spark.sql("""
WITH v_idade AS (
  SELECT
    cod_cliente,
    ROUND(datediff(current_date(), dt_nascimento_cliente) / 365.25, 0) AS idade_anos
  FROM clientes_silver
)
SELECT Round(avg(idade_anos), 2) AS media_idade
FROM v_idade
""").show()

