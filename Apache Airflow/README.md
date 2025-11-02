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

Esta DAG implementa uma orquestração robusta e escalável para o pipeline de dados, proporcionando:

- **Agendamento Confiável**: Execução diária automática
- **Controle de Dependências**: Garantia da ordem correta de processamento
- **Monitoramento**: Alertas e métricas de performance
- **Manutenibilidade**: Documentação completa e parâmetros configuráveis
- **Escalabilidade**: Facilidade de adicionar novas fontes ou transformações

O pipeline está pronto para operação em ambiente de produção, fornecendo dados confiáveis e insights estratégicos para análise do impacto da COVID-19 na economia global.

<img width="899" height="153" alt="image" src="https://github.com/user-attachments/assets/dbcab3dc-278a-4256-a539-19ae5fbe767f" />

# Documentação do Apache Airflow - Pipeline Databricks e Google Colab

## Visão Geral da Orquestração
O Apache Airflow é utilizado para orquestrar todo o pipeline de dados tanto no Databricks quanto no Google Colab, garantindo execução agendada, monitoramento e controle de dependências entre as camadas Bronze, Silver e Gold.

---

## Estrutura da DAG Principal - Databricks

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

---

## DAG Alternativa - Google Colab com API REST

### Visão Geral da Integração Colab
Para ambientes onde o Databricks não está disponível, implementamos uma solução alternativa utilizando Google Colab como servidor de processamento com API REST.

### Código da DAG para Google Colab

```python
"""
DAG: etl_covid_colab_final
Descrição: Pipeline ETL COVID-19 usando Google Colab como API REST
Arquitetura: Bronze → Silver → Gold via API autenticada
Responsável: Engenharia de Dados
"""

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import pendulum
import json
import base64

default_args = {
    'owner': 'engenharia_dados',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=2),
    'start_date': pendulum.datetime(2025, 1, 1, tz='America/Sao_Paulo')
}

def verify_connection():
    """Verifica se a conexão com a API do Colab está configurada"""
    from airflow.hooks.base import BaseHook
    try:
        conn = BaseHook.get_connection('covid_colab_api_secure')
        print(f"✅ Connection OK: {conn.conn_id}")
        return True
    except Exception as e:
        raise AirflowException(f"❌ Connection not found: {e}")

def validate_api_response(response):
    """Valida as respostas da API do Colab"""
    try:
        result = response.json()
        print(f"📨 Response: {result}")
        if result.get('status') in ['success', 'healthy', 'completed']:
            return result
        else:
            raise AirflowException(f"API Error: {result}")
    except ValueError as e:
        if response.status_code == 200:
            return {"status": "success", "message": "Response OK"}
        raise AirflowException(f"Invalid JSON: {response.text}")

def log_execution_results(**context):
    """Log de resultados finais da execução"""
    ti = context['ti']
    print("📊 ETL COVID-19 - RESULTADOS")
    
    tasks = ['executar_bronze', 'executar_silver', 'executar_gold']
    for task_id in tasks:
        try:
            result = ti.xcom_pull(task_ids=task_id)
            if result:
                status = result.get('status', 'unknown')
                message = result.get('message', 'No message')
                print(f"📋 {task_id}: {status.upper()} - {message}")
        except Exception as e:
            print(f"❌ {task_id}: Erro - {e}")

# Headers com autenticação básica para API do Colab
auth_string = base64.b64encode(b"covid_user:covid123").decode('utf-8')
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {auth_string}"
}

with DAG(
    dag_id='etl_covid_colab_final',
    default_args=default_args,
    description='Pipeline ETL COVID-19 usando Google Colab como API',
    schedule_interval=None,
    catchup=False,
    tags=['covid', 'etl', 'colab', 'api', 'rest'],
    max_active_runs=1
) as dag:

    # Task inicial
    start = DummyOperator(task_id='inicio_pipeline')

    # Verificar conexão
    verify_connection_task = PythonOperator(
        task_id='verificar_conexao',
        python_callable=verify_connection
    )

    # Executar camada Bronze via API
    executar_bronze = SimpleHttpOperator(
        task_id='executar_bronze_covid_19',
        http_conn_id='covid_colab_api_secure',
        endpoint='etl/bronze',
        method='POST',
        data=json.dumps({
            'data_processamento': '{{ ds }}',
            'ambiente': 'producao'
        }),
        headers=headers,
        response_check=validate_api_response,
        log_response=True,
        do_xcom_push=True
    )

    # Executar camada Silver via API
    executar_silver = SimpleHttpOperator(
        task_id='executar_silver_covid_19',
        http_conn_id='covid_colab_api_secure',
        endpoint='etl/silver',
        method='POST',
        data=json.dumps({
            'data_processamento': '{{ ds }}',
            'ambiente': 'producao'
        }),
        headers=headers,
        response_check=validate_api_response,
        log_response=True,
        do_xcom_push=True
    )

    # Executar camada Gold via API
    executar_gold = SimpleHttpOperator(
        task_id='executar_gold_covid_19',
        http_conn_id='covid_colab_api_secure',
        endpoint='etl/gold',
        method='POST',
        data=json.dumps({
            'data_processamento': '{{ ds }}',
            'ambiente': 'producao'
        }),
        headers=headers,
        response_check=validate_api_response,
        log_response=True,
        do_xcom_push=True
    )

    # Task de logging
    log_results = PythonOperator(
        task_id='log_execution_results',
        python_callable=log_execution_results,
        provide_context=True
    )

    # Task final
    end = DummyOperator(task_id='fim_pipeline')

    # Definir dependências
    start >> verify_connection_task >> executar_bronze >> executar_silver >> executar_gold >> log_results >> end
```

### Como Funciona a Integração com Google Colab

#### 1. **Configuração do Ambiente Colab**
- Servidor Flask com autenticação básica
- Endpoints REST para cada camada do pipeline
- Processamento Spark distribuído no Colab

#### 2. **Estrutura da API no Colab**
```python
# Endpoints disponíveis
POST /etl/bronze    # Ingestão de dados brutos
POST /etl/silver    # Processamento e qualidade
POST /etl/gold      # Modelagem analítica
POST /etl/full      # Pipeline completo
```

#### 3. **Credenciais de Acesso**
- **URL**: `https://5000-m-s-38jjolr8rvo67-d.us-east1-2.prod.colab.dev`
- **Usuário**: `covid_user`
- **Senha**: `covid123`

#### 4. **Fluxo de Execução**
1. Airflow envia requisição POST para endpoint no Colab
2. Colab executa o processamento Spark correspondente
3. Colab retorna status e métricas de execução
4. Airflow captura resposta via XCom para logging

### Vantagens da Solução Colab

#### ✅ **Flexibilidade**
- Execução sob demanda sem agendamento fixo
- Fácil prototipagem e testes
- Ideal para ambientes de desenvolvimento

#### ✅ **Custo Zero**
- Utilização gratuita do Google Colab
- Sem custos de infraestrutura adicional
- Ideal para POCs e projetos acadêmicos

#### ✅ **Integração Simplificada**
- API REST padrão
- Autenticação básica
- Respostas JSON estruturadas

### Configuração Necessária no Airflow

#### 1. **Connection no Airflow**
```
Connection ID: covid_colab_api_secure
Connection Type: HTTP
Host: https://5000-m-s-38jjolr8rvo67-d.us-east1-2.prod.colab.dev
Login: covid_user
Password: covid123
```

#### 2. **Pré-requisitos**
- Servidor Flask rodando no Colab
- Arquivos de dados carregados no ambiente Colab
- Conexão de internet estável

---

## Comparação das Soluções

| Aspecto | Databricks | Google Colab |
|---------|------------|--------------|
| **Agendamento** | Automático (diário) | Manual sob demanda |
| **Custo** | Corporativo | Gratuito |
| **Escalabilidade** | Alta | Limitada |
| **Confiabilidade** | Produção | Desenvolvimento |
| **Manutenção** | Baixa | Média |

<img width="992" height="114" alt="image" src="https://github.com/user-attachments/assets/2af1e06f-3cda-4b8b-b191-b43c8b4c41e7" />


---

## Considerações Finais

### Para Ambiente de Produção
**Recomendação**: Utilize a DAG `etl_databricks_principal` para:
- Processamentos diários automatizados
- Alta confiabilidade e escalabilidade
- Ambiente corporativo com suporte

### Para Desenvolvimento e Testes
**Recomendação**: Utilize a DAG `etl_covid_colab_final` para:
- Prototipagem rápida
- Testes de conceito
- Ambientes acadêmicos ou de estudo

Ambas as soluções implementam a mesma arquitetura de dados (Medallion Architecture) e garantem a qualidade e consistência dos dados processados, proporcionando flexibilidade na escolha da plataforma conforme as necessidades específicas de cada cenário.



