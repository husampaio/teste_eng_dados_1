"""
Descrição: Script para avaliar compentência em teste de engenharia de dados
Responsável: silasgloliveira@yahoo.com
Criado em: 16-04-2025

#############################################################################

Regras de Data Quality 

1. Colunas que não podem ter nulos: [
    "cod_cliente",
    "nm_cliente",
    "tp_pessoa",
    "num_telefone_cliente",
]

2. Coluna com chave única: "cod_cliente"

3. Datas válidas e dentro de intervalo: dt_nascimento_cliente

4. Renda não negativa
"""

# imports
from pyspark.sql import SparkSession
import great_expectations as gx
import great_expectations.expectations as gxe
import datetime

spark = SparkSession.builder.appName("TesteDataQuality").getOrCreate()

df_silver = spark.read.csv("./saida/clientes_silver.csv", header=True, inferSchema=True)

context = gx.get_context(mode="ephemeral")

data_source = context.data_sources.add_spark(name="clientes_silver") #fonte de dado spark

data_asset_name = "my_dataframe_data_asset"

data_asset = data_source.add_dataframe_asset(name=data_asset_name)

batch_definition_name = "my_batch_definition"

batch_definition = data_asset.add_batch_definition_whole_dataframe(
    batch_definition_name
)

batch_parameters = {"dataframe": df_silver}

batch = batch_definition.get_batch(batch_parameters=batch_parameters)

suite = context.suites.add(gx.ExpectationSuite(name="suite_store"))

## calculo data limite idade
current_date = datetime.date.today()
calc_datetime = current_date - datetime.timedelta(days=365*18)
date_limit = calc_datetime.strftime("%Y-%m-%d")
##

# Regras de qualidade de dados
expectations_list = [
    gxe.ExpectColumnValuesToNotBeNull(column="cod_cliente"),
    gxe.ExpectColumnValuesToNotBeNull(column="tp_pessoa"),
    gxe.ExpectColumnValuesToNotBeNull(column="nm_cliente"),
    gxe.ExpectColumnValuesToNotBeNull(column="num_telefone_cliente"), #irá gerar falha de data quality
    gxe.ExpectColumnValuesToBeUnique(column="cod_cliente"),
    gxe.ExpectColumnValuesToBeBetween(column="dt_nascimento_cliente", min_value="1910-01-01" , max_value=date_limit),
    gxe.ExpectColumnValuesToBeBetween(column="vl_renda", min_value=0)
]

# Adicionando as Expectations ao suite
for expectation in expectations_list:
    suite.add_expectation(expectation=expectation)


validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name="validacao_cliente"
)

validation_definition = context.validation_definitions.add(validation_definition)

validation_results = validation_definition.run(batch_parameters=batch_parameters)

if validation_results and not validation_results["success"]:
    print("A validação falhou. Detalhes:")
    for expectation_result in validation_results["results"]:
        if not expectation_result["success"]:
            print(validation_results)
            print(f"  Expectativa falhou: {expectation_result['expectation_config']['expectation_context']} - {expectation_result['expectation_config']['kwargs']}")
    raise Exception("Testes de qualidade de dados falharam!")
else:
    print("Validação bem-sucedida!")
    print(validation_results)






