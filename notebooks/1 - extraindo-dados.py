# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Importando bibliotecas

# COMMAND ----------

import requests
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Definindo função de extração de dados

# COMMAND ----------

def extraindo_dados(date, base="BRL"):

    url = f"https://api.apilayer.com/exchangerates_data/{date}&base={base}"

    headers = {
    "apikey": "bNSmW8SzZ3ZRpb6xcJqhaL0S411o9L9g"
    }
    parametros = {"base":base,
                #   "symbols": "USD,GBP,EUR" 
                  }

    response = requests.request("GET", url, headers=headers, params=parametros)


    if response.status_code != 200:
        raise Exception("Não consegui extrair dados!!!")

    return response.json()


# COMMAND ----------

extraindo_dados("2024-02-01")

# COMMAND ----------

ano, mes, dia = extraindo_dados("2024-02-01")['date'].split('-')
print(ano, mes, dia)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Definindo caminho para extração bronze

# COMMAND ----------

caminho_completo = f"dbfs:/databricks-results/bronze/{ano}/{mes}/{dia}"
print(caminho_completo)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Transformando os dados em um dataframe Spark

# COMMAND ----------

def dados_para_dataframe (dado_json): 
        dados_tupla = [(moeda, float (taxa)) for moeda, taxa in dado_json["rates"].items()]
        return dados_tupla

# COMMAND ----------

dados_para_dataframe(extraindo_dados("2024-02-01"))

# COMMAND ----------

response = dados_para_dataframe(extraindo_dados("2024-02-01"))

# COMMAND ----------

spark.createDataFrame(response).show(5)


# COMMAND ----------

spark.createDataFrame(response, schema=['moeda', 'taxa']).show(5)

# COMMAND ----------

df_conversoes = spark.createDataFrame(response, schema=['moeda', 'taxa'])
df_conversoes.show(5)

# COMMAND ----------

df_conversoes.withColumn("data", lit(f"{ano}-{mes}-{dia}")).show(5)
df_conversoes.show(5)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Salvando a extração

# COMMAND ----------

df_conversoes.write.format("parquet")\
    .mode("overwrite")\
    .save(caminho_completo)

# COMMAND ----------

display(dbutils.fs.ls(f"{caminho_completo}"))

# COMMAND ----------


