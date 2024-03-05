# Databricks notebook source
# dbutils.widgets.text("data_execucao", "")
# data_execucao = dbutils.widgets.get("data_execucao")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Importando bibliotecas

# COMMAND ----------

import requests
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Definindo funções

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Função de extração de dados

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

# MAGIC %md
# MAGIC
# MAGIC ## Função para transformar os dados em um dataframe  spark

# COMMAND ----------

def dados_para_dataframe(dado_json):
    dados_tupla = [(moeda, float(taxa)) for moeda, taxa in dado_json["rates"].items()]
    return dados_tupla

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Função para salvar os arquivos

# COMMAND ----------

def salvar_arquivo_parquet(conversoes_extraidas):
    ano, mes, dia = conversoes_extraidas['date'].split('-')
    caminho_completo = f"dbfs:/databricks-results/bronze/{ano}/{mes}/{dia}"
    
    response = dados_para_dataframe(conversoes_extraidas)
    df_conversoes = spark.createDataFrame(response, schema=['moeda', 'taxa'])
    df_conversoes = df_conversoes.withColumn("data", lit(f"{ano}-{mes}-{dia}"))
    
    df_conversoes.write.format("parquet")\
                .mode("overwrite")\
                .save(caminho_completo)

    print(f"Os arquivos foram salvos em {caminho_completo}")
 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Tetes / Execução

# COMMAND ----------

cotacoes = extraindo_dados("2024-02-01")
salvar_arquivo_parquet(cotacoes)


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-results/bronze/2024/02/01")) 

# COMMAND ----------

# dbutils.fs.rm("dbfs:/databricks-results/", True)
