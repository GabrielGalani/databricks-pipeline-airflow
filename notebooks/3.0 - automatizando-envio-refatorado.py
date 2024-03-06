# Databricks notebook source
from slack_Sdk import WebClient
import pyspark.pandas as ps

# COMMAND ----------

slack_token = "xoxb-6744899286806-6751439466563-vsAawUjA809pv2MkP9S9bkBb"
client = WebClient(token=slack_token)

# COMMAND ----------

nome_arquivo = dbutils.fs.ls("dbfs:/databricks-results/prata/valores_reais/")[-1].name

# COMMAND ----------

path = "../../../../../dbfs/databricks-results/prata/valores_reais/" + nome_arquivo

# COMMAND ----------

df_valores_reais = ps.read_csv("dbfs:/databricks-results/prata/valores_reais/")

# COMMAND ----------

!mkdir ../imagens

# COMMAND ----------

for moeda in df_valores_reais.columns[1:]:
    fig = df_valores_reais.plot.line(x="data", y=moeda)
    fig.write_image(f"../imagens/{moeda}.png")

# COMMAND ----------

def enviando_imagens(moeda_cotacao):
    enviando_imagens = client.files_upload_v2(
    channel="C06MNURVBST",  
    title="Enviando imagens de cotacoes",
    file=f"../imagens/{moeda_cotacao}.png"
)

# COMMAND ----------

for moeda in df_valores_reais.columns[1:]:
    enviando_imagens(moeda)
