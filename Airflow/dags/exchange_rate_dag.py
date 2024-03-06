from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    'Executando-notebook-etl',
    start_date=datetime(2024, 3, 4),
    schedule_interval="0 9 * * *"
    ) as dag_executando_notebook_extracao:


        extraindo_dados = DatabricksRunNowOperator(
        task_id = 'Extraindo-conversoes',
        databricks_conn_id = 'databricks_default',
        job_id = '292067935571330',
        notebook_params={"data_execucao": '{{data_interval_end.strftime("%Y-%m-%d")}}'}

    )
            
        transformando_dados_silver = DatabricksRunNowOperator(
        task_id = 'Transformando-dados',
        databricks_conn_id = 'databricks_default',
        job_id =  '140817525018336'
    )
        
        enviando_relatorio = DatabricksRunNowOperator(
        task_id = 'Enviando-relatorio',
        databricks_conn_id = 'databricks_default',
        job_id =  '633014441347933'
    )
    
        extraindo_dados >> transformando_dados_silver >> enviando_relatorio
