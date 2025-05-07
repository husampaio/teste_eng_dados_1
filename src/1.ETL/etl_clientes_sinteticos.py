import sys
import os

from awsglue.utils import  getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../4.DataQuality')))

from etl_functions import *
from data_quality import data_quality_clientes

#Criação da SparkSession e do GlueContext
spark = (
    SparkSession
        .builder
        .appName("etl_clientes_sinteticos")
        .config("spark.sql.adaptive.enabled", "true") 
        .config("spark.sql.shuffle.partitions", "100") #Valor variavel - 200 é o padrão
        .getOrCreate()
)
glueContext = GlueContext(spark.sparkContext)

#Retorno dos argumentos criados para os paths s3
args = getResolvedOptions(sys.argv, ["JOB_NAME", "PATH_RAW", "PATH_BRONZE", "PATH_SILVER"])

#paths e catalogos Glue
job_name = args["JOB_NAME"]
path_raw = args["PATH_RAW"] #Path do csv - s3://
path_bronze = args["PATH_BRONZE"] #Path bronze - s3://bucket-bronze/tabela_cliente_landing
path_silver = args["PATH_SILVER"] #Path silver - s3://bucket-silver/tb_cliente

catalog_bronze = "clientes_db_bronze"
catalog_silver = "clientes_db_silver"
table_name = "tb_cliente"

#schema do csv
schema_raw = StructType([
    StructField("cod_cliente", IntegerType(), False),
    StructField("nm_cliente", StringType(), False),
    StructField("nm_pais_cliente", StringType(), False),
    StructField("nm_cidade_cliente", StringType(), False),
    StructField("nm_rua_cliente", StringType(), False),
    StructField("num_casa_cliente", IntegerType(), False),
    StructField("telefone_cliente", StringType(), False),
    StructField("dt_nascimento_cliente", DateType(), False),
    StructField("dt_atualizacao", DateType(), False),
    StructField("tp_pessoa", StringType(), False),
    StructField("vl_renda", FloatType(), False) 
])


#Inicialização do job
job = Job(glueContext)
job.init(job_name, args)


#Camada bronze
df_bronze = read_csv(path_raw, sep=",", schema=schema_raw)

#Materialização do dataframe em memoria
df_bronze.cache().count()

df_bronze = columns_to_uppercase(df_raw, ["nm_cliente"])
df_bronze = rename_columns(df_bronze, {"telefone_cliente" : "num_telefone_cliente"})
df_bronze = df_bronze.withColumn("anomesdia", current_date())
write_parquet(df_bronze, path_bronze, mode="overwrite", partition_column="anomesdia")
update_glue_partition(catalog_bronze, table_name)

#Liberação do dataframe em cache - evita gargalos de memoria
df_bronze.unpersist()

#Camada silver

#Criação do DynamicFrame lendo da bronze no Glue
df_silver = glueContext.create_dynamic_frame.from_catalog(
            database=catalog_bronze, table_name=table_name)

#Criação do dataframe spark para manipulação
df_silver_transformed = df_silver.toDF()

#Materialização do dataframe em memoria
df_silver_transformed.cache().count()

df_silver_transformed = silver_deduplicate_rows(df_silver_transformed, ["cod_cliente"], ["dt_atualizacao"])
df_silver_transformed = format_phone(df_silver_transformed, "num_telefone_cliente", r"\(\d{2}\)\d{5}-\d{4}")
df_silver_transformed = df_silver_transformed.withColumn("anomesdia", current_date())

df_bronze.unpersist()

#Verificação de data quality
data_quality_clientes(df_silver_transformed)

#Eescrita dos dados na silver
write_glue_parquet(
    df=df_silver_transformed,
    glue_context=glueContext,
    path=path_silver,
    partition_keys=["anomesdia"],
    transformation_ctx="write_clientes_silver"
)
update_glue_partition(catalog_silver, table_name)  

job.commit()
logger.info(f"Job {job_name} finalizado.")
