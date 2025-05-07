from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame, Window, SparkSession
from awsglue.dynamicframe import DynamicFrame


from loguru import logger

def read_csv(path: str, sep: str, schema: StructType = None):
    """
    Lê um arquivo CSV com ou sem schema definido.

    Args:
        path (str): Caminho do arquivo CSV.
        sep (str): Separador de colunas.
        schema (StructType, opcional): Esquema dos dados.
    
    Returns:
        DataFrame: Dados lidos do CSV.
    """
    reader = spark.read

    if schema:
        reader.schema(schema)

    return reader.csv(path=path, header=True, sep=sep)


def write_parquet(df: DataFrame, path: str, mode: str, partition_column: str):
    """
    Escreve um DataFrame em formato Parquet particionado.

    Args:
        df (DataFrame): DataFrame a ser salvo.
        path (str): Caminho de destino no S3 ou HDFS.
        mode (str): Modo de gravação (overwrite, append, etc.).
        partition_column (str): Coluna para particionamento.
    """
    try:
        logger.info(f"Escrevendo dados em: {path}...")
        (
            df.write
            .mode(mode)
            .partitionBy(partition_column)
            .parquet(path, compression="snappy")
        )
    except Exception as error:
        logger.error(f"Erro ao escrever dados em {path}: {error}")


def columns_to_uppercase(df: DataFrame, columns: list) -> DataFrame:
    """
    Converte valores de colunas selecionadas para letras maiúsculas.

    Args:
        df (DataFrame): DataFrame de entrada.
        columns (list): Lista de colunas a serem convertidas.
    
    Returns:
        DataFrame: DataFrame com colunas modificadas.
    """
    for column in columns:
        df = df.withColumn(column, upper(col(column)))
    return df


def rename_columns(df: DataFrame, columns: dict) -> DataFrame:
    """
    Renomeia colunas de acordo com o dicionário fornecido.

    Args:
        df (DataFrame): DataFrame original.
        columns (dict): Mapeamento de nomes antigos para novos.
    
    Returns:
        DataFrame: DataFrame com colunas renomeadas.
    """
    for old_name, new_name in columns.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df 


def silver_deduplicate_rows(df: DataFrame, partition_by: list, order_by: str) -> DataFrame:
    """
    Remove duplicatas mantendo apenas a primeira linha por partição.

    Args:
        df (DataFrame): DataFrame original.
        partition_by (list): Lista de colunas para particionar os dados.
        order_by (str): Coluna usada para ordenar dentro da partição.
    
    Returns:
        DataFrame: DataFrame deduplicado.
    """
    window_spec = Window.partitionBy(partition_by).orderBy(order_by)

    return (
        df.withColumn("row_number", row_number().over(window_spec))
          .filter(col("row_number") == 1)
          .drop("row_number")
    )


def format_phone(df: DataFrame, column_name: str, regex_pattern: str) -> DataFrame:
    """
    Mantém números de telefone que correspondem ao padrão fornecido.
    Telefones fora do padrão são substituídos por null.

    Args:
        df (DataFrame): DataFrame original.
        column_name (str): Nome da coluna a ser validada.
        regex_pattern (str): Padrão regex para validação.

    Returns:
        DataFrame: DataFrame com valores válidos mantidos e inválidos como null.
    """
    return df.withColumn(
        column_name,
        when(col(column_name).rlike(regex_pattern), col(column_name)).otherwise(lit(None))
    )
    


def update_glue_partition(catalog_name: str, table_name: str):
    """
    Atualiza as partições de uma tabela no AWS Glue Catalog.

    Args:
        catalog_name (str): Nome do banco de dados Glue.
        table_name (str): Nome da tabela.
    
    Returns:
        None
    """
    full_path = f"{catalog_name}.{table_name}"

    try:
        logger.info(f"Atualizando partições da tabela {full_path}...")
        spark.sql(f"MSCK REPAIR TABLE {full_path}")
        logger.success("Atualização realizada!")
    except Exception as error:
        logger.error(f"Erro ao atualizar partições da tabela {full_path}: {error}")


def write_glue_parquet(
    df, 
    glue_context, 
    path: str, 
    partition_keys: list, 
    transformation_ctx: str 
):
    """
    Converte um DataFrame para DynamicFrame e escreve no S3 em formato Parquet particionado.

    Args:
        df (DataFrame): DataFrame Spark a ser escrito.
        glue_context (GlueContext): GlueContext ativo.
        path (str): Caminho no S3 para salvar os dados.
        partition_keys (list): Lista de colunas para particionamento.
        transformation_ctx (str): Nome do contexto de transformação do Glue.
    
    Returns:
        None
    """
    try:
        logger.info("Convertendo DataFrame para DynamicFrame...")
        dynamic_frame = DynamicFrame.fromDF(df, glue_context, transformation_ctx)

        logger.info(f"Escrevendo dados particionados em: {path}")
        glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": path,
                "partitionKeys": partition_keys
            },
            format="parquet",
            transformation_ctx=transformation_ctx
        )
        logger.success(f"Dados escritos com sucesso em {path}")
        
    except Exception as e:
        logger.error(f"Erro ao escrever dados no S3: {e}")
