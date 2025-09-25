# /dags/exercicio_10_1_openfda_etl_direct_to_bq.py

from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

# =====================================================================================
# VARIÁVEIS DE CONFIGURAÇÃO - PERSONALIZE COM SEUS DADOS!
# =====================================================================================
GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT_ID = "fluted-curve-470913-d3"  # <-- MUDE AQUI
BIGQUERY_DATASET = "openfda_dataset"  # <-- MUDE AQUI (deve existir no BigQuery)
BIGQUERY_TABLE = "daily_ibuprofen_events"  # Nome da tabela que será criada
# =====================================================================================


@dag(
    dag_id="exercicio_10_1_openfda_etl_direct_to_bq",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md="""
    ### ETL do OpenFDA Direto para o BigQuery (Sem GCS)
    
    Este DAG realiza o seguinte processo:
    1. **Extrai**: Busca eventos adversos diários relacionados ao Ibuprofeno na API do OpenFDA.
    2. **Transforma e Carrega**: Processa os dados, os achata em um DataFrame e o carrega
       diretamente para uma tabela no BigQuery.
    """,
    tags=["exercicio", "openfda", "gcp", "etl", "direct-load"],
)
def openfda_direct_to_bq_etl_dag():
    """
    DAG para o Exercício 10.1: ETL de dados diários do OpenFDA para o BigQuery sem usar GCS.
    """

    @task
    def extract_openfda_events(logical_date: str) -> list:
        """
        Busca dados da API OpenFDA para a data de execução lógica.
        """
        execution_date_str = datetime.fromisoformat(logical_date).strftime("%Y%m%d")
        print(f"Buscando dados para a data: {execution_date_str}")
        
        url = (
            "https://api.fda.gov/drug/event.json?"
            f'search=receivedate:[{execution_date_str}+TO+{execution_date_str}]'
            '&search=patient.drug.medicinalproduct:"ibuprofen"'
            "&limit=100"
        )
        
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        if "results" not in data or not data["results"]:
            print("Nenhum resultado encontrado para esta data.")
            return []
            
        return data["results"]

    @task
    def transform_and_load_to_bigquery(events: list):
        """
        Transforma os dados JSON e carrega o DataFrame resultante diretamente para o BigQuery.
        """
        if not events:
            print("Nenhum evento para processar. Finalizando a tarefa.")
            return

        df_flat = pd.json_normalize(events)
        
        columns_to_keep = {
            "safetyreportid": "report_id",
            "receivedate": "receive_date",
            "seriousnessdeath": "is_serious_death",
            "seriousnesshospitalization": "is_serious_hospitalization",
            "patient.patientsex": "patient_sex",
            "patient.patientonsetage": "patient_age",
            "patient.patientonsetageunit": "patient_age_unit",
        }
        
        existing_cols = {k: v for k, v in columns_to_keep.items() if k in df_flat.columns}
        df_final = df_flat[list(existing_cols.keys())].rename(columns=existing_cols)

        if "patient_sex" in df_final.columns:
            df_final["patient_sex"] = df_final["patient_sex"].map({"1": "Male", "2": "Female"})

        print(f"DataFrame com {len(df_final)} linhas pronto para ser carregado.")
        print(df_final.head())
        
        # Conexão com o BigQuery usando o Hook
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        
        # Obter o cliente Python do Google Cloud BigQuery
        client = bq_hook.get_client(project_id=GCP_PROJECT_ID)
        
        # Definir o ID completo da tabela de destino
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
        
        # Configurar o job de carregamento
        job_config = bigquery.LoadJobConfig(
            # O BigQuery vai detectar o schema a partir do DataFrame
            autodetect=True,
            # Se a tabela já tiver dados, os novos serão adicionados no final
            write_disposition="WRITE_APPEND",
            # Se a tabela não existir, ela será criada
            create_disposition="CREATE_IF_NEEDED",
        )
        
        # Executar o carregamento do DataFrame para o BigQuery
        job = client.load_table_from_dataframe(
            dataframe=df_final,
            destination=table_id,
            job_config=job_config,
        )
        
        job.result()  # Espera o job ser concluído
        
        print(f"Carregamento concluído. {job.output_rows} linhas foram adicionadas à tabela {table_id}.")


    # Definindo o fluxo de trabalho (agora mais simples)
    raw_events = extract_openfda_events(logical_date="{{ ds }}")
    transform_and_load_to_bigquery(events=raw_events)


# Instanciando o DAG
openfda_direct_to_bq_etl_dag()
