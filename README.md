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
