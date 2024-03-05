# Databricks notebook source
from pyspark.sql.functions import to_date, first, col, round

# COMMAND ----------

spark.read.parquet("dbfs:/databricks-results/bronze/*/*/*").show()

# COMMAND ----------

df_junto = spark.read.parquet("dbfs:/databricks-results/bronze/*/*/*")
df_junto.show(5)

# COMMAND ----------

df_junto.count()

# COMMAND ----------

df_junto.select("moeda").distinct().count()

# COMMAND ----------

moedas = ['USD', 'EUR', 'GBP']

df_moedas = df_junto.filter(df_junto.moeda.isin(moedas))
df_moedas.show()

# COMMAND ----------

df_moedas.select("moeda").distinct().count()

# COMMAND ----------

df_moedas = df_moedas.withColumn("data", to_date("data"))
df_moedas.printSchema()

# COMMAND ----------

resultado_taxas_conversao = df_moedas.groupBy("data") \
           .pivot("moeda") \
           .agg(first("taxa")) \
           .orderBy("data", ascending=False)

resultado_taxas_conversao.show(5)

# COMMAND ----------

resultado_valores_reais = resultado_taxas_conversao.select("*")
resultado_valores_reais.show(5)

# COMMAND ----------

for moeda in moedas: 
        resultado_valores_reais = resultado_valores_reais\
                        .withColumn(
                                moeda, round(1/col(moeda), 4)
                                        )
        resultado_valores_reais.show()

# COMMAND ----------

resultado_valores_reais.show()

# COMMAND ----------

resultado_taxas_conversao = resultado_taxas_conversao.coalesce(1)
resultado_valores_reais = resultado_valores_reais.coalesce(1)


# COMMAND ----------

# Salvando arquivos de taxa
resultado_taxas_conversao.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/prata/taxas_conversao")


# COMMAND ----------

# Salvando arquivos em reais    
resultado_valores_reais.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/prata/valores_reais")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-results/prata/valores_reais"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-results/prata/valores_reais/"))
