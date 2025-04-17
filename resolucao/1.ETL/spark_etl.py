"""
Descrição: Script para avaliar compentência em teste de engenharia de dados
Responsável: silasgloliveira@yahoo.com
Criado em: 16-04-2025
"""

#imports

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, date_format, upper, col, row_number, desc, when, regexp_replace
from pyspark.sql.window import Window 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType

##

#variaveis globais
s3_warehouse = "silas-bucket-bronze"
s3_bronze = "silas-bucket-bronze"
s3_silver= "silas-bucket-silver"
CATALOG_NAME = "glue_catalog"
DB_NAME_BRONZE = "lake-base-bronze"
DB_NAME_SILVER = "lake-base-silver"
TABLE_NAME = "tb_cliente"

s3_table_bronze_path = f"s3://{s3_bronze}/{TABLE_NAME}_landing"
s3_table_silver_path = f"s3://{s3_silver}/{TABLE_NAME}"
##

#funcoes
class Utils:

    @staticmethod
    def write_iceberg(df: DataFrame, catalog_name: str, database: str, table_name: str, order_col: str, partition_col: str = "anomesdia"):
        """
        Escreve um DataFrame em uma tabela iceberg particionado por uma coluna específica.

        :param df: DataFrame a ser escrito
        :param path: Caminho de destino (ex: caminho no S3 ou local)
        :param partition_col: Coluna para particionamento (padrão: "anomesdia")
        :param mode: Modo de escrita (padrão: "overwrite")
        """

        print("inicio gravacao")

        (df.sortWithinPartitions(order_col) 
            .writeTo(f"{catalog_name}.{database}.{table_name}") 
            .tableProperty("format-version", "2") 
            .partitionedBy(partition_col) 
            .createOrReplace()
            )
        
        print(f"fim da gravacao da tabela {table_name}")
        


    @staticmethod
    def write_parquet(df: DataFrame, path: str, partition_col: str = "anomesdia", mode: str = "overwrite"):
        """
        Escreve um DataFrame em formato Parquet particionado por uma coluna específica.

        :param df: DataFrame a ser escrito
        :param path: Caminho de destino (ex: caminho no S3 ou local)
        :param partition_col: Coluna para particionamento (padrão: "anomesdia")
        :param mode: Modo de escrita (padrão: "overwrite")
        """

        print(f'Inicio da escrita para {path}')
        (df.write 
            .mode(mode) 
            .partitionBy(partition_col) 
            .format("parquet") 
            .save(path)
        )
        print('escrita concluida com sucesso!')
    
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
####

spark = (SparkSession.builder
    .appName("ETL") 
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") 
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{s3_warehouse}/warehouse/") 
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") 
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") 
    .getOrCreate()
    )

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

writer = Utils.write_parquet

"""##################------ TRATAMENTO Bronze --------------#############################################"""

print('Inicio do processamento!')

df_origem = spark.read.csv("clientes_sinteticos.csv", schema= schema,header=True, inferSchema=True)

df_tratado_bronze = Utils.tratar_bronze(df_origem)

print('Inicio da escrita bronze')
# #escrita
# writer(df_tratado_bronze, s3_table_bronze_path, partition_col = "anomesdia")
print('Fim da escrita bronze')


"""
Opcionalmente, pode ser gravado já como tabela direta iceberg para ter ativo os benefícios do Iceberg como open table format 
"""


"""##################------ TRATAMENTO SILVER --------------#############################################

- Deduplica os dados agrupando por código do cliente e recupera o mais recente
- Aplica regra de máscara no número de telefone do cliente deixando nulo quando não for aplicável

"""


df_silver = df_tratado_bronze

df_dedup = Utils.deduplica_set(df_silver, "cod_cliente", "dt_atualizacao")

df_tratado_silver = Utils.limpeza_telefone(df_dedup, "num_telefone_cliente")

print('Inicio da escrita silver')
# #escrita
# writer(df_tratado_silver, s3_table_silver_path, partition_col = "anomesdia")
print('Fim da escrita silver')

df_tratado_silver.coalesce(1).write \
  .mode("overwrite") \
  .option("header", True) \
  .csv("./saida")

print('Fim do processamento!')