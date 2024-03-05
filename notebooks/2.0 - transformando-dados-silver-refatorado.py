# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Importando bibliotecas

# COMMAND ----------

from pyspark.sql.functions import to_date, first, col, round

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Lendo arquivos parquet

# COMMAND ----------

df_junto = spark.read.parquet("dbfs:/databricks-results/bronze/*/*/*")

# COMMAND ----------

moedas = ['USD', 'EUR', 'GBP']

df_moedas = df_junto.filter(df_junto.moeda.isin(moedas))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Tratando coluna de data

# COMMAND ----------

df_moedas = df_moedas.withColumn("data", to_date("data"))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Montando dataframe final

# COMMAND ----------

resultado_taxas_conversao = df_moedas.groupBy("data") \
           .pivot("moeda") \
           .agg(first("taxa")) \
           .orderBy("data", ascending=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Tranformando as taxas de conversão em valores reais (BRL)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Formula adotada

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Real (BRL) x Outras Moedas
# MAGIC R$1,00 -> €0,18
# MAGIC R$X -> €0,18
# MAGIC
# MAGIC x = 1/0,18

# COMMAND ----------

resultado_valores_reais = resultado_taxas_conversao.select("*")

# COMMAND ----------

for moeda in moedas: 
        resultado_valores_reais = resultado_valores_reais\
                        .withColumn(
                                moeda, round(1/col(moeda), 4)
                                        )

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Unindo conjuntos de dados

# COMMAND ----------

resultado_taxas_conversao = resultado_taxas_conversao.coalesce(1)
resultado_valores_reais = resultado_valores_reais.coalesce(1)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Salvando CSV na camada silver

# COMMAND ----------

# Salvando arquivos de taxa
resultado_taxas_conversao.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/prata/taxas_conversao")

# Salvando arquivos em reais    
resultado_valores_reais.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/prata/valores_reais")
    
