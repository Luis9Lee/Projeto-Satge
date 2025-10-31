# Documentação do Apache Airflow - Pipeline Databricks

## Visão Geral da Orquestração

O Apache Airflow é utilizado para orquestrar todo o pipeline de dados no Databricks, garantindo execução agendada, monitoramento e controle de dependências entre as camadas Bronze, Silver e Gold.

## Estrutura da DAG

### Configurações Principais

```python
"""
DAG: etl_databricks_principal
Descrição: Orquestra o pipeline completo de dados COVID-19 e economia no Databricks
Arquitetura: Bronze → Silver → Gold → Insights (Medallion Architecture)
Responsável: Engenharia de Dados
"""

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
import pendulum

# Configurações da DAG
default_args = {
    'owner': 'engenharia_dados',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5)
}

with DAG(
    dag_id='etl_databricks_principal',
    default_args=default_args,
    description='Pipeline completo ETL COVID-19 e dados econômicos no Databricks',
    schedule_interval='0 2 * * *',
    start_date=pendulum.datetime(2025, 1, 1, tz='America/Sao_Paulo'),
    end_date=None,
    catchup=False,
    tags=['etl', 'databricks', 'producao', 'covid', 'economia'],
    max_active_runs=1,
    concurrency=1
) as dag:

    # Tarefa 1: Camada Bronze - Ingestão de dados brutos
    bronze_covid_19 = DatabricksRunNowOperator(
        task_id='executar_bronze_covid_19',
        databricks_conn_id='databricks_default',
        notebook_params={
            'data_processamento': '{{ ds }}',
            'ambiente': 'producao',
            'camada': 'bronze',
            'reprocessar': 'false',
            'executar_api_worldbank': 'true'
        },
        python_params=None,
        spark_submit_params=None
    )
    bronze_covid_19.notebook_path = "/Workspace/Users/luisgaltm@gmail.com/desafio stage/Bronze_covid-19"

    # Tarefa 2: Camada Silver - Processamento e qualidade
    silver_covid_19 = DatabricksRunNowOperator(
        task_id='executar_silver_covid_19',
        databricks_conn_id='databricks_default',
        notebook_params={
            'data_processamento': '{{ ds }}',
            'ambiente': 'producao', 
            'camada': 'silver',
            'reprocessar': 'false',
            'validar_qualidade': 'true',
            'aplicar_limpeza': 'true',
            'executar_enriquecimento': 'true'
        },
        python_params=None,
        spark_submit_params=None
    )
    silver_covid_19.notebook_path = "/Workspace/Users/luisgaltm@gmail.com/desafio stage/Silver_covid-19"

    # Tarefa 3: Camada Gold - Modelagem analítica
    gold_covid_19 = DatabricksRunNowOperator(
        task_id='executar_gold_covid_19',
        databricks_conn_id='databricks_default',
        notebook_params={
            'data_processamento': '{{ ds }}',
            'ambiente': 'producao',
            'camada': 'gold', 
            'gerar_agregados_anuais': 'true',
            'calcular_metricas_continente': 'true',
            'criar_rankings_paises': 'true'
        },
        python_params=None,
        spark_submit_params=None
    )
    gold_covid_19.notebook_path = "/Workspace/Users/luisgaltm@gmail.com/desafio stage/Gold_covid-19"

    # Tarefa 4: Insights e relatórios executivos
    insights_gold_covid_19 = DatabricksRunNowOperator(
        task_id='executar_insights_gold_covid_19',
        databricks_conn_id='databricks_default',
        notebook_params={
            'data_processamento': '{{ ds }}',
            'ambiente': 'producao',
            'gerar_insights': 'true',
            'executar_consultas_analiticas': 'true',
            'criar_relatorio_executivo': 'true',
            'enviar_notificacao': 'true'
        },
        python_params=None,
        spark_submit_params=None
    )
    insights_gold_covid_19.notebook_path = "/Workspace/Users/luisgaltm@gmail.com/desafio stage/Insights_Da_Gold_covid-19"

    # Definir dependências entre as tarefas
    bronze_covid_19 >> silver_covid_19 >> gold_covid_19 >> insights_gold_covid_19

    # Documentação das tarefas
    bronze_covid_19.doc_md = """
    ### Bronze_covid-19 - Ingestão de Dados Brutos
    
    **Caminho:** `/Workspace/Users/luisgaltm@gmail.com/desafio stage/Bronze_covid-19`
    
    **Objetivo:** Ingestão de todas as fontes de dados COVID-19 e econômicos
    **Fontes Processadas:**
    - country_dataset.csv (delimitador ;)
    - cases_dataset.csv
    - hospital_dataset.csv
    - tests_dataset.csv
    - vaccination_dataset.csv
    - API World Bank (GDP growth)
    
    **Saída:** Tabelas no schema bronze do Unity Catalog
    **Tabelas Criadas:** country_raw, cases_raw, hospital_raw, tests_raw, vaccination_raw, gdp_raw
    """

    silver_covid_19.doc_md = """
    ### Silver_covid-19 - Processamento e Qualidade
    
    **Caminho:** `/Workspace/Users/luisgaltm@gmail.com/desafio stage/Silver_covid-19`
    
    **Objetivo:** Limpeza, validação e enriquecimento dos dados brutos
    **Processos Executados:**
    - Conversão segura de tipos numéricos
    - Tratamento de valores missing e outliers
    - Validação de consistência temporal
    - Enriquecimento com joins estratégicos
    - Cálculo de métricas derivadas (casos/milhão, etc.)
    
    **Saída:** Tabelas no schema silver do Unity Catalog  
    **Tabelas Criadas:** country_cleaned, cases_cleaned, vaccination_cleaned, gdp_cleaned, covid_enriched
    """

    gold_covid_19.doc_md = """
    ### Gold_covid-19 - Modelagem Analítica
    
    **Caminho:** `/Workspace/Users/luisgaltm@gmail.com/desafio stage/Gold_covid-19`
    
    **Objetivo:** Criação de modelos analíticos e agregados estratégicos
    **Processos Executados:**
    - Agregações anuais por país e continente
    - Cálculo de correlações COVID-economia
    - Criação de rankings de países
    - Desenvolvimento de índices de resiliência
    - Preparação para dashboards executivos
    
    **Saída:** Tabelas no schema gold do Unity Catalog
    **Tabelas Criadas:** covid_economic_annual, continent_metrics, top_countries_economic_impact, top_countries_best_response
    """

    insights_gold_covid_19.doc_md = """
    ### Insights_Da_Gold_covid-19 - Análise e Relatórios
    
    **Caminho:** `/Workspace/Users/luisgaltm@gmail.com/desafio stage/Insights_Da_Gold_covid-19`
    
    **Objetivo:** Geração de insights estratégicos e relatórios executivos
    **Processos Executados:**
    - Análise de correlações entre variáveis
    - Identificação de padrões e tendências
    - Geração de relatórios executivos
    - Preparação de dados para apresentação
    - Cálculo de métricas de negócio
    
    **Saídas:**
    - Relatórios PDF para diretoria
    - Insights para tomada de decisão
    - Métricas de performance do pipeline
    - Alertas e notificações
    """
```

## Considerações Finais

Esta DAG implementa uma orquestração robusta e escalável para o pipeline de dados, proporcionando:

- **Agendamento Confiável**: Execução diária automática
- **Controle de Dependências**: Garantia da ordem correta de processamento
- **Monitoramento**: Alertas e métricas de performance
- **Manutenibilidade**: Documentação completa e parâmetros configuráveis
- **Escalabilidade**: Facilidade de adicionar novas fontes ou transformações

O pipeline está pronto para operação em ambiente de produção, fornecendo dados confiáveis e insights estratégicos para análise do impacto da COVID-19 na economia global.

<img width="899" height="153" alt="image" src="https://github.com/user-attachments/assets/dbcab3dc-278a-4256-a539-19ae5fbe767f" />
