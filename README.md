# Orquestrando Dados na Nuvem com Airflow + Azure Databricks

## Descrição do Projeto - (Fictício)

Como engenheiro de dados em uma empresa multinacional, fui desafiado a criar uma solução para acompanhar as flutuações de diferentes moedas ao redor do mundo, visando a previsão de gastos e outros KPIs. O objetivo é extrair diariamente as cotações das moedas em que a empresa opera: Dólar Americano (USD), Libra Esterlina (GBP) e Euro (EUR). Além disso, é necessário enviar essas informações diariamente, às nove da manhã, para os responsáveis pelos gastos da empresa em formato tabular.

## Funcionalidades

- Extração diária das cotações das moedas (USD, EUR, GBP).
- Envio automatizado da tabela de cotações aos responsáveis pelos gastos da empresa.
- Criação de gráficos atualizados com o histórico de cotação das moedas nos últimos dois meses.

## Datalake - estrutura
O Datalake foi arquitetado pensando na arquitetura medalhão, onde a camada bronze recebe os dados brutos, a camada silver recebe os dados com primeiro estágio de tratamento e a camada gold (não usada) recebe os dados refinados, conforme imagem abaixo:

### Estrutura - datalake - img
![estrutura-datalake](https://github.com/GabrielGalani/databricks-pipeline-airflow/blob/main/Estrutura-datalake/Datalake%20-%20arquitetura.png)
### Estrutura - bronze - img
![estrutura-bronze](https://github.com/GabrielGalani/databricks-pipeline-airflow/raw/main/Estrutura-datalake/Bronze%20-%20estrutura.png)

## Exemplo de Tabela Enviada

| Data       | USD      | EUR      | GBP      |
|------------|----------|----------|----------|
| 2023-05-22 | 0.201305 | 0.186176 | 0.161871 |
| 2023-05-21 | 0.200068 | 0.184865 | 0.160561 |
| 2023-05-20 | 0.199985 | 0.184767 | 0.160676 |
| ...        | ...      | ...      | ...      |


## Exemplo de Gráficos

- Histórico de cotação do Dólar Americano (USD) nos últimos dois meses.
- ![dolar](/imagens/USD.png)
- Histórico de cotação do Euro (EUR) nos últimos dois meses.
- ![dolar](/imagens/EUR.png)
- Histórico de cotação da Libra Esterlina (GBP) nos últimos dois meses.
- ![dolar](/imagens/GBP.png)

## Integração com Slack

Dada a natureza remota do trabalho, utilizamos o Slack como plataforma de comunicação interna. As tabelas e gráficos são enviados automaticamente para o Slack, facilitando o acesso e a análise das informações.

## Tecnologias Utilizadas

- Pyspark
- API - apilayer
- Databricks
- Apache Airflow

## Execução do Pipeline

O pipeline foi construído e é executado na Cloud Azure, utilizando o Apache Airflow para orquestração das tarefas e o Databricks para processamento distribuído. A integração com o Slack permite a comunicação eficiente e o compartilhamento das informações com o time responsável pelos gastos da empresa.

Esperamos que essa solução auxilie o time a ter previsões mais precisas acerca do consumo do orçamento, proporcionando insights valiosos para a gestão financeira da empresa.
