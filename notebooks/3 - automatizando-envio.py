# Databricks notebook source
# MAGIC %pip install kaleido slack-sdk #levar para o job

# COMMAND ----------

import slack-sdk
from slack_Sdk import WebClient
import pyspark.pandas as ps

# COMMAND ----------

slack_token = "xoxb-6737575427414-6741312365653-fjquxudHMUzQD9C1y81PV9tq"
client = WebClient(token=slack_token)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-results/prata/valores_reais/"))


# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-results/prata/valores_reais/")[-1].name


# COMMAND ----------

nome_arquivo = dbutils.fs.ls("dbfs:/databricks-results/prata/valores_reais/")[-1].name


# COMMAND ----------

!pwd


# COMMAND ----------

!ls


# COMMAND ----------

!ls ../../


# COMMAND ----------

!ls ../../dbfs/

# COMMAND ----------

!ls ../../dbfs/databricks-results/prata/valores_reais/


# COMMAND ----------

path = "../../dbfs/databricks-results/prata/valores_reais/" + nome_arquivo

# COMMAND ----------

enviando_arquivo_csv = client.files_upload_v2(
    channel="C06MFK9GBN3",  
    title="Arquivo no formato CSV do valor do real convertido",
    file=path,
    filename="valores_reais.csv",
    initial_comment="Segue anexo o arquivo CSV:",
)

# COMMAND ----------

ps.read_csv("dbfs:/databricks-results/prata/valores_reais/").head()


# COMMAND ----------

df_valores_reais = ps.read_csv("dbfs:/databricks-results/prata/valores_reais/")
df_valores_reais.head()

# COMMAND ----------

df_valores_reais.plot.line(x="data", y='USD')


# COMMAND ----------

!mkdir imagens

# COMMAND ----------

!ls


# COMMAND ----------

df_valores_reais.columns


# COMMAND ----------

df_valores_reais.columns[1:]


# COMMAND ----------

for moeda in df_valores_reais.columns[1:]:
    fig = df_valores_reais.plot.line(x="data", y=moeda)
    fig.write_image(f".imagens/{moeda}.png")


# COMMAND ----------

!ls ./imagens


# COMMAND ----------

def enviando_imagens(moeda_cotacao):
    enviando_imagens = client.files_upload_v2(
    channel="C06MFK9GBN3",  
    title="Enviando imagens de cotacoes",
    file=f"./imagens/{moeda_cotacao}.png"
)

# COMMAND ----------

for moeda in df_valores_reais.columns[1:]:
    enviando_imagens(moeda)
