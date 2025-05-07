from pyspark.sql.functions import *
from pyspark.sql.types import *

from loguru import logger

def data_quality_clientes(df: DataFrame):
    
    logger.info(f"Iniciando data quality...")
    
    quality_issues = []
    
    expected_schema = {
        "cod_cliente" : IntegerType,
        "nm_cliente" : StringType,
        "nm_pais_cliente" : StringType,
        "nm_cidade_cliente" : StringType,
        "nm_rua_cliente" : StringType,
        "num_casa_cliente" : IntegerType,
        "num_telefone_cliente" : StringType,
        "dt_nascimento_cliente": DateType,
        "dt_atualizacao": DateType,
        "tp_pessoa" : StringType,
        "vl_renda" : FloatType,
        "anomesdia" : DateType
}

    
    
    #Verificação de schema - colunas e datatypes
    for column, data_type in expected_schema.items():
        if column not in df.columns:
            logger.error(f"Coluna inexistente: {column}")
            quality_issues.append(f"Coluna inexistente: {column}")
        elif not isinstance(df.schema[column].dataType, data_type):
            data_type_found = df.schema[column].dataType
            logger.error(f"Coluna {column} com data type incorreto. Encontrado: {data_type_found} - Esperado {data_type}")
            quality_issues.append(f"Tipo de coluna esperado para {column}: {str(data_type)}")
    
    
    #Verificação de valores nulos para colunas principais da tabela
    main_columns = ["cod_cliente", "nm_cliente", "dt_nascimento_cliente", "vl_renda"]
    
    for column in main_columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            logger.error(f"Valores nulos na coluna {column}: {null_count}")
            quality_issues.append(f"Valores nulos na coluna {column}: {null_count}")
    
    
    #Verificação de unicidade na coluna chave da tabela
    total_count = df.select("cod_cliente").count()
    distinct_ids = df.select("cod_cliente").distinct().count()
    
    if distinct_ids != total_count:
        num_duplicatas = total_count - distinct_ids 
        logger.error(f"A chave cod_cliente da tabela possui {num_duplicatas} duplicatas")
        quality_issues.append(f"A chave cod_cliente da tabela possui {num_duplicatas} duplicatas")
    
    
    #Verificação de formato de telefone
    invalid_phones = (
    df.filter(
        (col("telefone_cliente").isNotNull()) &
        (~col("telefone_cliente").rlike(r"\(\d{2}\)\d{5}-\d{4}"))
    ).count()
)    
    
    if invalid_phones > 0:
        quality_issues.append(f"{invalid_phones} registros possuem telefone inválido.")    
        
    
    #Output final da verificação
    if quality_issues:
        logger.error(f"{len(quality_issues)} erros de data quality encontrados.")
        for issue in quality_issues:
            print(issue)
    
    else:
        logger.success(f"Sem erros de data quality.")