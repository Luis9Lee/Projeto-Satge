# Documentação Geral do Projeto - Análise COVID-19 e Impacto Econômico

## 📋 Visão Geral do Projeto

### Contexto Business
Este projeto foi desenvolvido como resposta ao case da **STAGE - Case Engenharia**, com o objetivo de analisar os cenários do período de pandemia para identificar padrões mundiais que possam criar direcionamentos estratégicos para a empresa.

**Missão:** Consolidar, tratar e obter insights sobre dados da pandemia COVID-19 integrados com indicadores econômicos para suporte à tomada de decisão estratégica.

## 🎯 Objetivos do Projeto

### Objetivos Principais
1. **Estruturação de Dados**: Criar pipeline robusto para ingestão e tratamento de dados
2. **Qualidade e Confiabilidade**: Garantir acuracidade e agilidade nos modelos desenvolvidos
3. **Geração de Insights**: Identificar padrões e correlações entre saúde e economia
4. **Apresentação Estratégica**: Comunicar resultados para diretoria com perfil analítico

### Metas Técnicas
- ✅ Implementar arquitetura Medallion (Bronze-Silver-Gold)
- ✅ Integrar múltiplas fontes de dados (CSV + API pública)
- ✅ Orquestrar com Apache Airflow
- ✅ Garantir qualidade e performance dos dados
- ✅ Gerar análises business-ready

## 🏗️ Arquitetura da Solução

### Diagrama da Arquitetura
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   FONTES DE     │    │   DATA PLATFORM  │    │   ORQUESTRAÇÃO  │    │   VISUALIZAÇÃO  │
│     DADOS       │    │  (Databricks)    │    │   (Airflow)     │    │   & INSIGHTS    │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • CSVs locais   │───▶│ • Bronze (Raw)   │───▶│ • DAG Diária    │───▶│ • SQL Analytics │
│ • API World Bank│    │ • Silver (Clean) │    │ • Controle Deps │    │ • Dashboards    │
│                 │    │ • Gold (Business)│    │ • Monitoramento │    │ • Relatórios    │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
```

### Stack Tecnológica
| Camada | Tecnologia | Justificativa |
|--------|------------|---------------|
| **Storage** | Delta Lake (Unity Catalog) | ACID transactions, versioning, performance |
| **Processamento** | Databricks, PySpark | Processamento distribuído, escalabilidade |
| **Orquestração** | Apache Airflow | Controle de dependências, agendamento |
| **Metadados** | Unity Catalog | Governança e discoverability |
| **Análise** | Spark SQL, Python | Análises ad-hoc e relatórios |

## 📊 Fontes de Dados

### Fontes Primárias (Fornecidas)
| Dataset | Descrição | Volume | Periodicidade |
|---------|-----------|---------|---------------|
| `country_dataset.csv` | Dados demográficos e socioeconômicos | ~200 países | Estático |
| `cases_dataset.csv` | Casos e óbitos por COVID-19 | ~500K registros | Diária |
| `hospital_dataset.csv` | Dados hospitalares | ~200K registros | Diária |
| `tests_dataset.csv` | Testes realizados | ~300K registros | Diária |
| `vaccination_dataset.csv` | Dados de vacinação | ~400K registros | Diária |

### Fonte Externa (API Pública)
- **World Bank API**: Indicadores de crescimento do PIB (2019-2023)
- **Justificativa**: Complementar análise com impacto econômico
- **Endpoint**: `NY.GDP.MKTP.KD.ZG` (GDP growth annual %)

## 🔄 Pipeline de Dados

### Camada Bronze - Ingestão
**Objetivo**: Coleta e preservação dos dados brutos
```python
# Funcionalidades implementadas:
✅ Detecção automática de delimitadores CSV
✅ Sistema de mapeamento de colunas para Delta Lake
✅ Consumo de API REST com tratamento de erro
✅ Tabelas de metadados para rastreabilidade
✅ Relatórios de ingestão automáticos
```

### Camada Silver - Limpeza e Qualidade
**Objetivo**: Dados confiáveis e padronizados
```python
# Transformações aplicadas:
✅ Conversão segura de tipos numéricos (vírgulas/pontos)
✅ Validação de chaves e integridade referencial
✅ Filtros de qualidade (população > 0, datas válidas)
✅ Padronização temporal (DateType)
✅ Enriquecimento com joins estratégicos
✅ Cálculo de métricas derivadas (por milhão)
```

### Camada Gold - Modelagem Business
**Objetivo**: Dados otimizados para análise
```python
# Modelos desenvolvidos:
📈 Agregados anuais (covid_economic_annual)
🌍 Métricas continentais (continent_metrics)  
🏆 Rankings estratégicos (top_countries)
📊 Análises de correlação (covid_economy_correlation)
📋 Resumo executivo (executive_summary)
```

## 🚀 Orquestração com Airflow

### Estrutura da DAG
```python
DAG: etl_databricks_principal
Schedule: Diário às 02:00
Dependências: Bronze → Silver → Gold → Insights
```

### Tarefas e Parâmetros
| Tarefa | Notebook | Parâmetros Chave |
|--------|----------|------------------|
| **Bronze** | `Bronze_covid-19` | `executar_api_worldbank=true` |
| **Silver** | `Silver_covid-19` | `validar_qualidade=true` |
| **Gold** | `Gold_covid-19` | `criar_rankings_paises=true` |
| **Insights** | `Insights_Da_Gold_covid-19` | `criar_relatorio_executivo=true` |

## 💡 Insights e Análises Business

### Principais Descobertas

#### 1. Impacto Econômico por Região
```sql
-- Países com maior queda no PIB (2020)
SELECT iso_code, continent, gdp_growth_percent 
FROM gold.covid_economic_annual 
WHERE year = 2020 
ORDER BY gdp_growth_percent ASC 
LIMIT 10;
```

#### 2. Correlação Vacinação x Economia
```sql
-- Recuperação econômica pós-vacinação
SELECT 
    CASE 
        WHEN vaccination_rate < 30 THEN 'Baixa Vacinação'
        WHEN vaccination_rate BETWEEN 30 AND 60 THEN 'Média Vacinação' 
        ELSE 'Alta Vacinação'
    END as faixa_vacinacao,
    AVG(gdp_growth_percent) as media_crescimento_pib
FROM gold.covid_economic_annual
WHERE year = 2021
GROUP BY faixa_vacinacao;
```

#### 3. Índices de Resiliência
- **Severity Index**: Mortalidade × Casos por milhão
- **Recovery Index**: Crescimento PIB × Taxa de vacinação
- **Efficiency Index**: Resposta sanitária vs. impacto econômico

### Métricas Business Desenvolvidas
| Métrica | Fórmula | Interpretação |
|---------|---------|---------------|
| **Recovery Score** | `GDP_growth × Vaccination_rate` | Eficácia da recuperação |
| **Severity Index** | `(deaths_per_million × cases_per_million) / 1000` | Impacto sanitário |
| **Economic Resilience** | `GDP_growth_2021 - GDP_growth_2020` | Capacidade de recuperação |

## 🛠️ Melhores Práticas Implementadas

### Engenharia de Dados
1. **Medallion Architecture**: Separação clara de responsabilidades
2. **Data Quality**: Validações em múltiplas camadas
3. **Idempotência**: Reprocessamento seguro
4. **Observability**: Logs, métricas e alertas
5. **Documentação**: Código auto-documentado e docs técnicos

### Performance e Otimização
```python
# Técnicas aplicadas:
✅ Particionamento por ano/continente
✅ Uso de Delta Lake para upserts
✅ Compactação e organização de dados
✅ Cache estratégico para agregações
✅ Predicate pushdown em filtros comuns
```

### Governança e Metadados
- **Unity Catalog**: Controle de acesso e lineage
- **Tabelas de Mapeamento**: Rastreabilidade de colunas
- **Documentação Embedded**: Docstrings e comentários
- **Parâmetros Configuráveis**: Flexibilidade por ambiente

## 📈 Valor Business Gerado

### Para a Diretoria
- **Visão Holística**: Integração saúde-economia em tempo real
- **Tomada de Decisão**: Insights baseados em dados confiáveis
- **Oportunidades**: Identificação de mercados resilientes
- **Risco**: Antecipação de cenários críticos

### Para a Equipe Técnica
- **Base Sólida**: Arquitetura escalável para novos dados
- **Produtividade**: Ferramentas e processos otimizados
- **Qualidade**: Garantia de confiabilidade dos dados
- **Manutenibilidade**: Código limpo e documentado

## 🔮 Próximos Passos e Expansões

### Fontes Adicionais Recomendadas
| Fonte | Tipo | Valor Business |
|-------|------|----------------|
| **Mobility Data** (Google/Apple) | API | Impacto em mobilidade/comércio |
| **Financial Markets** | API | Correlação com bolsas mundiais |
| **Commodity Prices** | CSV | Impacto em cadeias de suprimento |
| **Climate Data** | API | Análise de sazonalidade |

### Melhorias Técnicas
1. **Real-time Processing**: Streaming para dados mais recentes
2. **ML Integration**: Modelos preditivos de tendências
3. **Data Mesh**: Domínios específicos por área de negócio
4. **Advanced Monitoring**: Anomaly detection no pipeline

### Expansões de Análise
- **Segmentação**: Análise por setores econômicos
- **Time-series Forecasting**: Projeções de cenários
- **Network Analysis**: Propagação entre países conectados
- **Sentiment Analysis**: Impacto de notícias e mídia

## 🎯 Conclusão

Este projeto demonstra uma **solução completa de engenharia de dados** que atende aos requisitos do case da STAGE:

✅ **Compreensão do Cenário**: Arquitetura alinhada aos objetivos business  
✅ **Capacidade Técnica**: Stack moderna e boas práticas de engenharia  
✅ **Qualidade de Dados**: Processos robustos de validação e limpeza  
✅ **Geração de Insights**: Análises estratégicas e métricas business-ready  
✅ **Comunicação**: Documentação clara para diferentes perfis  

A solução está **pronta para produção** e pode ser expandida para incorporar novas fontes e análises, proporcionando valor contínuo para a estratégia da empresa.

---

<img width="950" height="438" alt="image" src="https://github.com/user-attachments/assets/7be8a3b8-a1a3-408d-bf0e-7e9b279a16a6" />
<img width="928" height="416" alt="image" src="https://github.com/user-attachments/assets/6e046c32-06b3-44d5-9b53-367a77e620b4" />
<img width="927" height="354" alt="image" src="https://github.com/user-attachments/assets/953f2422-bf62-4d47-94e1-aef95033d041" />


---

## 🆕 Adições com Google Colab

### 🎯 **Objetivo da Expansão Colab**
Prover uma alternativa flexível e de custo zero para execução do pipeline, ideal para:
- **Ambientes de desenvolvimento e testes**
- **POCs rápidas e prototipagem**
- **Execução sob demanda sem agendamento fixo**
- **Situações onde Databricks não está disponível**

### 🏗️ **Arquitetura Híbrida Atualizada**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   FONTES DE     │    │   PLATAFORMAS    │    │   ORQUESTRAÇÃO  │
│     DADOS       │    │   DE PROCESSO    │    │   (Airflow)     │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ • CSVs locais   │────│   DATABRICKS     │    │ • DAG Diária    │
│ • API World Bank│    │  (Produção)      │────│   (Produção)    │
│                 │    │                  │    │                 │
│                 │    │   GOOGLE COLAB   │    │ • DAG Sob Demanda│
│                 │────│  (Desenvolvimento)│────│   (Desenvolvimento)│
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │                         │
                              ▼                         ▼
                    ┌─────────────────┐    ┌─────────────────┐
                    │   VISUALIZAÇÃO  │    │     INSIGHTS    │
                    │   & INSIGHTS    │    │                 │
                    └─────────────────┘    └─────────────────┘
```

---

## 🛠️ **Stack Tecnológica Expandida**

| Camada | Databricks (Produção) | Google Colab (Desenvolvimento) |
|--------|----------------------|--------------------------------|
| **Storage** | Delta Lake (Unity Catalog) | Delta Lake (Local/Google Drive) |
| **Processamento** | Databricks Runtime | PySpark no Colab |
| **Orquestração** | Airflow com Databricks Operator | Airflow com HTTP Operator |
| **API/Interface** | Databricks UI | Flask REST API |
| **Autenticação** | Token Databricks | HTTP Basic Auth |
| **Custo** | Corporativo | **Gratuito** |

---

## 🔌 **Integração Google Colab**

### **API REST com Flask**
```python
# Arquitetura da API no Colab
┌─────────────────────────────────────────┐
│           FLASK REST API                │
│  Porta: 5000 | Auth: Basic HTTP         │
├─────────────────────────────────────────┤
│  POST /etl/bronze  → executar_bronze()  │
│  POST /etl/silver  → executar_silver()  │
│  POST /etl/gold    → executar_gold()    │
│  POST /etl/full    → pipeline_completo()│
│  GET  /health      → status_servico()   │
└─────────────────────────────────────────┘
```

### **Credenciais de Acesso**
```yaml
# Connection no Airflow
Connection ID: covid_colab_api_secure
Type: HTTP
Host: https://[COLAB_URL].prod.colab.dev
Login: covid_user
Password: covid123
Schema: https
Port: 5000
```

---

## 📊 **Pipeline Colab - Fluxo Detalhado**

### **1. Configuração do Ambiente Colab**
```python
# Célula 1: Setup Inicial
!pip install pyspark==3.4.0 delta-spark==2.4.0
!pip install flask flask-httpauth flask-cors
!apt-get install openjdk-11-jdk-headless -qq

# Configuração Spark
builder = SparkSession.builder.appName("ColabCOVIDAnalysis")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Estrutura de diretórios
BASE_PATH = "/content/covid_data"
os.makedirs(f"{BASE_PATH}/bronze", exist_ok=True)
# ... silver e gold
```

### **2. Camadas de Processamento (Idênticas ao Databricks)**
```python
# Estrutura mantida para compatibilidade
✅ Bronze: ingestão_raw() → prefixos_colunas()
✅ Silver: limpeza_dados() → enriquecimento()
✅ Gold: agregados_anuais() → metricas_business()
```

### **3. Exposição via API**
```python
@app.route('/etl/bronze', methods=['POST'])
@auth.login_required
def api_bronze():
    # Executa camada bronze via HTTP
    success, message = executar_bronze(data_processamento)
    return jsonify({
        'status': 'success' if success else 'error',
        'message': message,
        'timestamp': str(datetime.now())
    })
```

---

## 🔄 **DAGs Airflow para Colab**

### **DAG Principal Colab**
```python
# etl_covid_colab_final.py
with DAG('etl_covid_colab_final', schedule_interval=None) as dag:
    
    verify_connection = PythonOperator(
        task_id='verificar_conexao',
        python_callable=verify_connection
    )
    
    executar_bronze = SimpleHttpOperator(
        task_id='executar_bronze',
        http_conn_id='covid_colab_api_secure',
        endpoint='etl/bronze',
        method='POST',
        headers=headers_auth,  # Basic Auth
        response_check=validate_api_response
    )
    
    # Silver e Gold similares
```

### **Diferenças das DAGs**

| Aspecto | Databricks DAG | Colab DAG |
|---------|----------------|-----------|
| **Operator** | `DatabricksRunNowOperator` | `SimpleHttpOperator` |
| **Conexão** | `databricks_default` | `covid_colab_api_secure` |
| **Autenticação** | Token Databricks | HTTP Basic Auth |
| **Execução** | Notebooks remotos | API REST endpoints |
| **Agendamento** | Diário (02:00) | Manual/Sob demanda |
| **Monitoramento** | Databricks Jobs UI | Flask logs + Airflow |

---

## 🎯 **Vantagens da Solução Colab**

### ✅ **Flexibilidade Operacional**
```python
# Execução sob demanda
airflow dags trigger etl_covid_colab_final

# Parâmetros dinâmicos via API
{
    "data_processamento": "2025-11-02",
    "ambiente": "desenvolvimento",
    "reprocessar": "false"
}
```

### ✅ **Custo Zero**
- **Google Colab**: Gratuito para uso básico
- **Airflow Local**: Sem custos de cloud
- **API REST**: Protocolo padrão sem licenças

### ✅ **Rápida Prototipagem**
```python
# Teste rápido no Colab
!python -c "
from minha_api import executar_bronze
sucesso, mensagem = executar_bronze('2025-11-02')
print(f'Resultado: {sucesso} - {mensagem}')
"
```

### ✅ **Compatibilidade com Produção**
```python
# Mesma lógica de negócio
def processar_cases():
    """IDÊNTICA ao Databricks - garante consistência"""
    df_cases = spark.read.format("delta").load(f"{BRONZE_PATH}/cases_raw")
    # ... mesma transformação
    return df_processed
```

---

## 🚀 **Casos de Uso Específicos Colab**

### **1. Desenvolvimento e Testes**
```python
# Cenário: Nova transformação
# 1. Desenvolva no Colab
novo_calculo = df.withColumn("nova_metrica", ...)

# 2. Teste via API
response = requests.post(
    "https://colab-url/etl/silver",
    auth=("covid_user", "covid123"),
    json={"testar_nova_feature": "true"}
)

# 3. Promova para produção
# (Copie código para Databricks)
```

### **2. Demonstrações e Workshops**
```python
# Live coding com resultados imediatos
!curl -X POST https://colab-url/etl/bronze \
  -u "covid_user:covid123" \
  -H "Content-Type: application/json" \
  -d '{"data_processamento":"2025-11-02"}'
```

### **3. Backup e Contingência**
```python
# Se Databricks indisponível
# 1. Execute no Colab
# 2. Mesmos dados, mesma lógica
# 3. Recuperação rápida
```

---

## 📋 **Comparação Detalhada**

| Critério | Databricks | Google Colab |
|----------|------------|--------------|
| **Custo** | 💰 Corporativo | 🆓 Gratuito |
| **Performance** | 🚀 Alta (cluster dedicado) | ⚡ Moderada (recursos compartilhados) |
| **Confiabilidade** | 🔒 Alta (SLA enterprise) | 🛡️ Moderada (depende do Colab) |
| **Escalabilidade** | 📈 Automática | 📊 Limitada |
| **Manutenção** | 🔧 Baixa (managed service) | 🛠️ Média (configuração manual) |
| **Integração** | 🔗 Nativa com Azure/AWS | 🌐 HTTP/REST padrão |
| **Time-to-Market** | ⏱️ Moderado | 🏃‍♂️ Rápido |

---

## 🔧 **Configuração do Ambiente Colab**

### **Pré-requisitos**
```bash
# 1. Upload de arquivos
/content/country_dataset.csv
/content/cases_dataset.csv
/content/vaccination_dataset.csv
# ... outros datasets

# 2. Execução sequencial
# Célula 1: Instalação dependências
# Célula 2: Configuração Spark  
# Célula 3: Bronze
# Célula 4: Silver
# Célula 5: Gold
# Célula 6: API Flask
```

### **Deploy da API**
```python
# Ao executar a célula da API:
🌐 URL PÚBLICA: https://5000-m-s-xxxxxxxxx.prod.colab.dev
🔐 CREDENCIAIS: covid_user / covid123
📋 ENDPOINTS: /health, /etl/bronze, /etl/silver, /etl/gold, /etl/full
```

---

## 🎯 **Valor Business da Expansão Colab**

### **Para Desenvolvedores**
```yaml
Produtividade: 
  - Desenvolvimento rápido sem burocracia
  - Testes instantâneos de transformações
  - Debugging simplificado

Aprendizado:
  - Ambiente sandbox para experimentos
  - Curva de aprendizado reduzida
  - Prototipagem sem riscos
```

### **Para Negócio**
```yaml
Agilidade:
  - Novas análises em horas, não dias
  - Validação rápida de hipóteses
  - Resposta ágil a demandas urgentes

Custo:
  - Redução de custos em desenvolvimento
  - Otimização de recursos cloud
  - ROI mais rápido em POCs
```

### **Para Arquitetura**
```yaml
Resiliência:
  - Plano B para contingência
  - Multi-cloud strategy
  - Redundância operacional

Flexibilidade:
  - Escolha da plataforma por use case
  - Migração facilitada entre ambientes
  - Adoção gradual de novas tecnologias
```

---

## 📈 **Métricas de Sucesso Colab**

### **Operacionais**
```python
# Disponibilidade da API
uptime_api = "~95%"  # Depende da sessão Colab

# Tempo de execução
tempo_bronze = "2-5 minutos"
tempo_silver = "3-7 minutos" 
tempo_gold = "1-3 minutos"

# Confiabilidade
taxa_sucesso = ">90%"  # Em sessões estáveis
```

### **Business**
```python
# Velocidade de desenvolvimento
time_to_first_insight = "1-2 horas"  # vs dias no Databricks

# Custo desenvolvimento
custo_desenvolvimento = "$0"  # Colab gratuito

# Flexibilidade
numero_experimentos = "Ilimitado"  # Reset fácil da sessão
```

---

## 🔮 **Roadmap Futuro Colab**

### **Melhorias Imediatas**
```python
# 1. Persistência de dados
- Integração com Google Drive
- Backup automático dos Deltas

# 2. Monitoramento avançado
- Health checks da API
- Métricas de performance
- Alertas de falha

# 3. Segurança
- Rotação de credenciais
- HTTPS obrigatório
- Rate limiting
```

### **Expansões Planejadas**
```python
# 1. Novos endpoints
GET /metrics/performance
GET /data/export?format=csv
POST /analysis/correlation

# 2. Integrações
- Google Sheets para relatórios
- Slack notifications
- Data Studio dashboards

# 3. Features avançadas
- Cache de resultados
- Processamento assíncrono
- Versionamento de modelos
```

---

## 🎯 **Conclusão da Expansão Colab**

A integração do **Google Colab** ao pipeline existente proporciona:

### ✅ **Complementaridade Estratégica**
- **Databricks**: Produção, escala, confiabilidade
- **Google Colab**: Desenvolvimento, agilidade, custo-zero

### ✅ **Arquitetura Híbrida Robusta**
```python
# Opção flexível por cenário
def escolher_plataforma(use_case):
    if use_case in ["producao", "escala", "sla"]:
        return "DATABRICKS"
    elif use_case in ["desenvolvimento", "teste", "poc"]:
        return "GOOGLE_COLAB"
    else:
        return "MELHOR_CUSTO_BENEFICIO"
```

### ✅ **Preparação para o Futuro**
- **Multi-cloud readiness**
- **Disaster recovery**
- **Team empowerment**

A solução agora oferece **o melhor dos dois mundos**: robustez enterprise do Databricks com agilidade startup do Google Colab, atendendo a todos os cenários do case STAGE com excelência técnica e pragmatismo operacional. 🚀

