# Camadas de Dados do COVID-19 e Economia

## Visão Geral do Projeto

Este projeto implementa um pipeline completo de dados seguindo a **Medallion Architecture** no Databricks, processando dados relacionados à COVID-19 e indicadores econômicos. O pipeline é organizado em três camadas principais (Bronze → Silver → Gold) com orquestração via Apache Airflow, proporcionando uma solução robusta para análise do impacto da pandemia na economia global.

### Arquitetura do Sistema

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CAMADA BRONZE │    │   CAMADA SILVER │    │    CAMADA GOLD  │    │    INSIGHTS     │
│                 │    │                 │    │                 │    │                 │
│  - Ingestão     │───▶│  - Limpeza      │───▶│  - Agregações   │───▶│  - Análises     │
│  - Raw Data     │    │  - Validação    │    │  - Modelagem    │    │  - Relatórios   │
│  - Múltiplas    │    │  - Enriquecimento│    │  - Métricas    │    │  - Dashboards   │
│    Fontes       │    │  - Qualidade    │    │  - Rankings     │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 1. Camada Bronze - Ingestão de Dados Brutos

### Objetivo
Coleta e ingestão inicial de dados de múltiplas fontes, preservando os dados em seu formato original para auditoria e reprocessamento.

### Fontes de Dados Processadas

| Fonte | Tipo | Descrição | Volume Esperado |
|-------|------|-----------|-----------------|
| `country_dataset.csv` | CSV | Dados demográficos e socioeconômicos | ~200 países |
| `cases_dataset.csv` | CSV | Casos e óbitos por COVID-19 | ~500K registros |
| `hospital_dataset.csv` | CSV | Dados hospitalares | ~200K registros |
| `tests_dataset.csv` | CSV | Testes realizados | ~300K registros |
| `vaccination_dataset.csv` | CSV | Dados de vacinação | ~400K registros |
| API World Bank | JSON | Crescimento do PIB (2019-2023) | ~10K registros |

### Tecnologias e Técnicas Utilizadas

- **Detecção Automática de Delimitadores**: Para lidar com diferentes formatos de CSV
- **Mapeamento de Colunas**: Sistema para criar nomes de colunas válidos no Delta Lake
- **Tratamento de APIs REST**: Consumo da API World Bank com tratamento de erros
- **Persistência de Metadados**: Tabelas de mapeamento para rastreabilidade

### Processos Principais

1. **Leitura Adaptativa de CSVs**: Detecta automaticamente delimitadores (`,`, `;`, `\t`, `|`)
2. **Normalização de Colunas**: Cria nomes curtos e válidos para o Delta Lake
3. **Ingestão de API**: Consome dados econômicos em tempo real
4. **Controle de Qualidade**: Validações básicas e relatórios de ingestão

---
## Código de Configuração Inicial (Executar Primeiro)

```python
# =============================================
# CONFIGURAÇÃO INICIAL DO UNITY CATALOG
# Execute este código primeiro para configurar
# =============================================

# Configurações para Unity Catalog
catalog_name = "`data-stage`"  # Com crases para escapar o hífen
base_schema = "default"

# Criar schemas para as três camadas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver") 
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")

print("Schemas criados com sucesso!")

# Listar todos os schemas no catalog
print("\n=== SCHEMAS DISPONÍVEIS ===")
schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
schemas.show()
```

## Código da Camada Bronze

```python
# =============================================
# CAMADA BRONZE - INGESTÃO DE DADOS BRUTOS
# =============================================

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import requests
import pandas as pd

# Configurações
catalog_name = "`data-stage`"
schema_name = "bronze"

# Inicializar Spark Session
spark = SparkSession.builder.appName("Bronze_Layer").getOrCreate()

print("🚀 INICIANDO CAMADA BRONZE")

# =============================================
# FUNÇÕES AUXILIARES
# =============================================

def criar_nomes_colunas_seguros(df, prefixo):
    """Cria nomes de colunas curtos e válidos para Delta Lake"""
    return {col: f"{prefixo}_{i:02d}" for i, col in enumerate(df.columns)}

def salvar_tabela_com_mapeamento(df, nome_tabela, prefixo_colunas):
    """Salva tabela principal e tabela de mapeamento de colunas"""
    try:
        # Criar e aplicar mapeamento de colunas
        mapeamento = criar_nomes_colunas_seguros(df, prefixo_colunas)
        for col_original, col_nova in mapeamento.items():
            df = df.withColumnRenamed(col_original, col_nova)
        
        # Salvar tabela principal
        df.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .saveAsTable(f"{catalog_name}.{schema_name}.{nome_tabela}")
        
        # Salvar tabela de mapeamento
        dados_mapeamento = [(orig, nova) for orig, nova in mapeamento.items()]
        schema_mapeamento = StructType([
            StructField("coluna_original", StringType(), True),
            StructField("coluna_curta", StringType(), True)
        ])
        df_mapeamento = spark.createDataFrame(dados_mapeamento, schema_mapeamento)
        df_mapeamento.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(f"{catalog_name}.{schema_name}.{nome_tabela}_mapeamento")
        
        return True, mapeamento
        
    except Exception as e:
        print(f"❌ Erro ao salvar {nome_tabela}: {str(e)}")
        return False, None

def detectar_delimitador_arquivo(caminho_arquivo):
    """Detecta automaticamente o delimitador do arquivo CSV"""
    delimitadores = [",", ";", "\t", "|"]
    
    for delim in delimitadores:
        try:
            df_test = spark.read \
                .option("header", "true") \
                .option("delimiter", delim) \
                .option("inferSchema", "true") \
                .csv(caminho_arquivo)
            
            if len(df_test.columns) > 1:
                return df_test, delim
        except:
            continue
    
    return None, None

def processar_arquivo_csv(caminho_arquivo, nome_tabela, prefixo_colunas):
    """Processa arquivo CSV com delimitador automático"""
    try:
        df, delimitador = detectar_delimitador_arquivo(caminho_arquivo)
        if df is None:
            print(f"❌ Não foi possível ler {caminho_arquivo}")
            return None
        
        print(f"✅ {caminho_arquivo}: {len(df.columns)} colunas (delimitador: '{delimitador}')")
        return salvar_tabela_com_mapeamento(df, nome_tabela, prefixo_colunas)[1]
        
    except Exception as e:
        print(f"❌ Erro em {caminho_arquivo}: {str(e)}")
        return None

def processar_api_worldbank():
    """Ingere dados da API World Bank - GDP Growth"""
    try:
        url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.KD.ZG?date=2019:2023&format=json&per_page=10000"
        response = requests.get(url, timeout=30)
        data = response.json()
        
        if len(data) > 1 and isinstance(data[1], list):
            pdf = pd.json_normalize(data[1])
            colunas = ['countryiso3code', 'date', 'value', 'country.value', 'country.id', 'indicator.value']
            colunas_disponiveis = [col for col in colunas if col in pdf.columns]
            
            df_api = spark.createDataFrame(pdf[colunas_disponiveis])
            print(f"✅ API World Bank: {df_api.count()} registros")
            return salvar_tabela_com_mapeamento(df_api, "gdp_raw", "gdp")[1]
        else:
            print("⚠️  API não retornou dados")
            return None
            
    except Exception as e:
        print(f"❌ Erro na API: {str(e)}")
        return None

# =============================================
# EXECUÇÃO PRINCIPAL
# =============================================

def executar_camada_bronze():
    """Executa toda a ingestão da camada Bronze"""
    
    print("📥 INICIANDO INGESTÃO DE DADOS...")
    mapeamentos = {}
    
    # 1. Country dataset (delimitador ;)
    print("\n📍 Country Dataset")
    df_country = spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("inferSchema", "true") \
        .csv("/Volumes/data-stage/default/dados/country_dataset.csv")
    mapeamentos["country_raw"] = salvar_tabela_com_mapeamento(df_country, "country_raw", "cou")[1]
    
    # 2. Demais datasets (detecção automática de delimitador)
    datasets = [
        ("cases_dataset.csv", "cases_raw", "cas"),
        ("hospital_dataset.csv", "hospital_raw", "hos"), 
        ("tests_dataset.csv", "tests_raw", "tes"),
        ("vaccination_dataset.csv", "vaccination_raw", "vac")
    ]
    
    for arquivo, tabela, prefixo in datasets:
        print(f"\n📍 {arquivo}")
        mapeamento = processar_arquivo_csv(
            f"/Volumes/data-stage/default/dados/{arquivo}", tabela, prefixo)
        if mapeamento:
            mapeamentos[tabela] = mapeamento
    
    # 3. API World Bank
    print("\n📍 API World Bank")
    mapeamento_api = processar_api_worldbank()
    if mapeamento_api:
        mapeamentos["gdp_raw"] = mapeamento_api
    
    return mapeamentos

# =============================================
# RELATÓRIO FINAL
# =============================================

def gerar_relatorio_bronze(mapeamentos):
    """Gera relatório final da camada Bronze"""
    print("\n" + "="*50)
    print("📊 RELATÓRIO - CAMADA BRONZE")
    print("="*50)
    
    # Contagem de tabelas e registros
    tabelas = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
    tabelas_principais = [t[1] for t in tabelas if "_mapeamento" not in t[1]]
    
    print(f"\n📁 Tabelas criadas: {len(tabelas_principais)}")
    
    for tabela in tabelas_principais:
        df = spark.table(f"{catalog_name}.{schema_name}.{tabela}")
        print(f"   ▸ {tabela}: {df.count():,} registros, {len(df.columns)} colunas")
    
    print(f"\n✅ CAMADA BRONZE CONCLUÍDA - {len(mapeamentos)} tabelas ingeridas")

# =============================================
# EXECUÇÃO
# =============================================

if __name__ == "__main__":
    mapeamentos_finais = executar_camada_bronze()
    gerar_relatorio_bronze(mapeamentos_finais)
```

## 2. Camada Silver - Limpeza e Enriquecimento

### Objetivo
Transformar dados brutos em dados confiáveis e padronizados, aplicando regras de qualidade, limpeza e enriquecimento para preparação da análise.

### Processos de Transformação

#### 2.1 Limpeza de Dados
- **Conversão Segura de Tipos**: Função especializada para números com diferentes formatos (vírgulas, pontos)
- **Tratamento de Valores Nulos**: Filtragem de registros críticos sem chaves
- **Padronização de Datas**: Conversão para formato DateType do Spark
- **Validação de Consistência**: Filtros para população > 0, datas válidas

#### 2.2 Enriquecimento
- **Junção de Dados**: Integração entre casos COVID e dados demográficos
- **Cálculo de Métricas**: Casos por milhão, óbitos por milhão
- **Extração de Períodos**: Ano e mês para agregações temporais
- **Padronização de Códigos**: ISO codes como chave universal

### Tabelas Produzidas

| Tabela | Descrição | Principais Transformações |
|--------|-----------|---------------------------|
| `country_cleaned` | Dados demográficos limpos | Conversão numérica, filtros de qualidade |
| `cases_cleaned` | Casos e óbitos processados | Renomeação, conversão numérica, padronização temporal |
| `vaccination_cleaned` | Vacinação processada | Mesmas transformações dos casos |
| `gdp_cleaned` | Dados econômicos | Filtro temporal (2019-2023), conversão numérica |
| `covid_enriched` | Dados integrados | Joins, métricas calculadas, períodos extraídos |

---

## Código da Camada Silver

```python
# =============================================
# CAMADA SILVER - LIMPEZA E ENRIQUECIMENTO
# =============================================

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Configurações
catalog_name = "`data-stage`"
bronze_schema = "bronze"
silver_schema = "silver"

# Inicializar Spark Session
spark = SparkSession.builder.appName("Silver_Layer").getOrCreate()

print("🔄 INICIANDO CAMADA SILVER")

# =============================================
# FUNÇÕES AUXILIARES
# =============================================

def limpar_numero(coluna):
    """Converte números com pontos/vírgulas para formato double"""
    return (when(col(coluna).rlike("^[-]?[0-9]*[.,]?[0-9]+$"), 
                regexp_replace(col(coluna), "\\.", ""))
            .when(col(coluna).rlike("^[-]?[0-9]*[,]?[0-9]+$"), 
                regexp_replace(col(coluna), ",", "."))
            .otherwise(None)
            .cast("double"))

# =============================================
# PROCESSAMENTO COUNTRY
# =============================================

def processar_country():
    """Processa dados estáticos de países"""
    print("\n📍 Country Dataset")
    
    df_country = spark.table(f"{catalog_name}.{bronze_schema}.country_raw")
    
    # Aplicar transformações
    df_processed = (df_country
        .withColumn("population_density", limpar_numero("cou_01"))
        .withColumn("median_age", limpar_numero("cou_02"))
        .withColumn("aged_65_older", limpar_numero("cou_03"))
        .withColumn("aged_70_older", limpar_numero("cou_04"))
        .withColumn("gdp_per_capita", limpar_numero("cou_05"))
        .withColumn("extreme_poverty", limpar_numero("cou_06"))
        .withColumn("cardiovasc_death_rate", limpar_numero("cou_07"))
        .withColumn("diabetes_prevalence", limpar_numero("cou_08"))
        .withColumn("female_smokers", limpar_numero("cou_09"))
        .withColumn("male_smokers", limpar_numero("cou_10"))
        .withColumn("handwashing_facilities", limpar_numero("cou_11"))
        .withColumn("hospital_beds_per_thousand", limpar_numero("cou_12"))
        .withColumn("life_expectancy", limpar_numero("cou_13"))
        .withColumn("human_development_index", limpar_numero("cou_14"))
        .withColumn("population", col("cou_15").cast("bigint"))
        .filter(col("cou_00").isNotNull())
        .filter(col("population") > 0)
        .withColumnRenamed("cou_00", "iso_code")
        .drop(*[f"cou_{i:02d}" for i in range(16)])
    )
    
    df_processed.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{silver_schema}.country_cleaned")
    
    print(f"✅ {df_processed.count()} países processados")
    return df_processed

# =============================================
# PROCESSAMENTO CASES
# =============================================

def processar_cases():
    """Processa dados de casos e óbitos"""
    print("\n📍 Cases Dataset")
    
    df_cases = spark.table(f"{catalog_name}.{bronze_schema}.cases_raw")
    
    # Renomear colunas
    renomear_colunas = {
        'cas_00': 'iso_code', 'cas_01': 'continent', 'cas_02': 'location',
        'cas_03': 'date', 'cas_04': 'total_cases', 'cas_05': 'new_cases',
        'cas_06': 'new_cases_smoothed', 'cas_07': 'total_deaths', 
        'cas_08': 'new_deaths', 'cas_09': 'new_deaths_smoothed',
        'cas_10': 'total_cases_per_million', 'cas_11': 'new_cases_per_million',
        'cas_12': 'new_cases_smoothed_per_million', 'cas_13': 'total_deaths_per_million',
        'cas_14': 'new_deaths_per_million', 'cas_15': 'new_deaths_smoothed_per_million'
    }
    
    df_renomeado = df_cases
    for col_curta, col_nova in renomear_colunas.items():
        if col_curta in df_renomeado.columns:
            df_renomeado = df_renomeado.withColumnRenamed(col_curta, col_nova)
    
    # Converter colunas numéricas
    colunas_numericas = ['total_cases', 'new_cases', 'new_cases_smoothed', 'total_deaths', 
                        'new_deaths', 'new_deaths_smoothed', 'total_cases_per_million',
                        'new_cases_per_million', 'new_cases_smoothed_per_million',
                        'total_deaths_per_million', 'new_deaths_per_million', 
                        'new_deaths_smoothed_per_million']
    
    df_processed = df_renomeado
    for coluna in colunas_numericas:
        if coluna in df_processed.columns:
            df_processed = df_processed.withColumn(coluna, limpar_numero(coluna))
    
    df_processed = (df_processed
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        .filter(col("iso_code").isNotNull())
        .filter(col("date").isNotNull())
    )
    
    df_processed.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{silver_schema}.cases_cleaned")
    
    print(f"✅ {df_processed.count()} registros processados")
    return df_processed

# =============================================
# PROCESSAMENTO VACCINATION
# =============================================

def processar_vaccination():
    """Processa dados de vacinação"""
    print("\n📍 Vaccination Dataset")
    
    df_vaccination = spark.table(f"{catalog_name}.{bronze_schema}.vaccination_raw")
    
    # Renomear colunas
    renomear_colunas = {
        'vac_00': 'iso_code', 'vac_01': 'date', 'vac_02': 'total_vaccinations',
        'vac_03': 'people_vaccinated', 'vac_04': 'people_fully_vaccinated',
        'vac_05': 'total_boosters', 'vac_06': 'new_vaccinations',
        'vac_07': 'new_vaccinations_smoothed', 'vac_08': 'total_vaccinations_per_hundred',
        'vac_09': 'people_vaccinated_per_hundred', 'vac_10': 'people_fully_vaccinated_per_hundred',
        'vac_11': 'total_boosters_per_hundred', 'vac_12': 'new_vaccinations_smoothed_per_million',
        'vac_13': 'new_people_vaccinated_smoothed', 'vac_14': 'new_people_vaccinated_smoothed_per_hundred'
    }
    
    df_renomeado = df_vaccination
    for col_curta, col_nova in renomear_colunas.items():
        if col_curta in df_renomeado.columns:
            df_renomeado = df_renomeado.withColumnRenamed(col_curta, col_nova)
    
    # Converter colunas numéricas
    colunas_numericas = ['total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
                        'total_boosters', 'new_vaccinations', 'new_vaccinations_smoothed',
                        'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred',
                        'people_fully_vaccinated_per_hundred', 'total_boosters_per_hundred',
                        'new_vaccinations_smoothed_per_million', 'new_people_vaccinated_smooted',
                        'new_people_vaccinated_smoothed_per_hundred']
    
    df_processed = df_renomeado
    for coluna in colunas_numericas:
        if coluna in df_processed.columns:
            df_processed = df_processed.withColumn(coluna, limpar_numero(coluna))
    
    df_processed = (df_processed
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        .filter(col("iso_code").isNotNull())
        .filter(col("date").isNotNull())
    )
    
    df_processed.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{silver_schema}.vaccination_cleaned")
    
    print(f"✅ {df_processed.count()} registros processados")
    return df_processed

# =============================================
# ENRIQUECIMENTO DOS DADOS
# =============================================

def enriquecer_dados():
    """Cria tabela enriquecida com joins"""
    print("\n📍 Enriquecendo Dados")
    
    df_cases = spark.table(f"{catalog_name}.{silver_schema}.cases_cleaned")
    df_country = spark.table(f"{catalog_name}.{silver_schema}.country_cleaned")
    
    df_enriched = (df_cases
        .join(df_country.select("iso_code", "population", "gdp_per_capita", "life_expectancy"), 
              "iso_code", "left")
        .withColumn("cases_per_million", 
                   when(col("population") > 0, (col("total_cases") / col("population")) * 1000000)
                   .otherwise(None))
        .withColumn("deaths_per_million",
                   when(col("population") > 0, (col("total_deaths") / col("population")) * 1000000)
                   .otherwise(None))
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
    )
    
    df_enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{silver_schema}.covid_enriched")
    
    print(f"✅ {df_enriched.count()} registros enriquecidos")
    return df_enriched

# =============================================
# EXECUÇÃO PRINCIPAL
# =============================================

def executar_camada_silver():
    """Executa todo o processamento da camada Silver"""
    
    print("🎯 EXECUTANDO PROCESSAMENTO SILVER")
    
    # Criar schema se necessário
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema}")
    
    # Processar datasets
    processar_country()
    processar_cases()
    processar_vaccination()
    processar_gdp()
    enriquecer_dados()
    
    print("\n✅ SILVER CONCLUÍDA")

# =============================================
# RELATÓRIO FINAL
# =============================================

def gerar_relatorio_silver():
    """Gera relatório final da Silver"""
    print("\n" + "="*50)
    print("📊 RELATÓRIO - SILVER")
    print("="*50)
    
    tabelas = spark.sql(f"SHOW TABLES IN {catalog_name}.{silver_schema}").collect()
    
    for tabela in tabelas:
        nome = tabela[1]
        df = spark.table(f"{catalog_name}.{silver_schema}.{nome}")
        print(f"▸ {nome}: {df.count():,} registros")

# =============================================
# EXECUÇÃO
# =============================================

if __name__ == "__main__":
    executar_camada_silver()
    gerar_relatorio_silver()
```

## 3. Camada Gold - Modelagem Analítica

### Objetivo
Criar modelos de dados otimizados para análise business, com agregações, métricas estratégicas e preparação para visualização.

### Modelos Desenvolvidos

#### 3.1 Agregados Anuais (`covid_economic_annual`)
- **Agregações Temporais**: Soma de casos e óbitos anuais
- **Métricas de Vacinação**: Taxas máximas anuais de vacinação
- **Indicadores Econômicos**: Crescimento do PIB integrado
- **Joins Estratégicos**: Dados demográficos para contexto

#### 3.2 Métricas Continentais (`continent_metrics`)
- **Agregações Geográficas**: Somas e médias por continente
- **Taxas Normalizadas**: Casos e óbitos por milhão de habitantes
- **Comparativos**: Crescimento econômico médio por região

#### 3.3 Rankings Estratégicos
- **Impacto Econômico**: Países com maior queda no PIB (2020)
- **Melhor Resposta**: Países com menor mortalidade (2021)
- **Índices de Recuperação**: Combinação de crescimento PIB e vacinação

#### 3.4 Análises de Correlação
- **COVID vs Economia**: Correlação entre casos e crescimento PIB
- **Vacinação vs Recuperação**: Impacto da vacinação na economia
- **Índices Compostos**: Severidade e recuperação

### Insights Business Gerados

1. **Resiliência Econômica**: Identificação de países que melhor se recuperaram
2. **Eficácia de Políticas**: Correlação entre vacinação e performance econômica
3. **Padrões Regionais**: Diferenças continentais na resposta à pandemia
4. **Métricas de Impacto**: Severidade vs capacidade de recuperação

---

## Código da Camada Gold

```python
# =============================================
# CAMADA GOLD - AGREGAÇÕES E MODELAGEM FINAL
# =============================================

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Configurações
catalog_name = "`data-stage`"
silver_schema = "silver"
gold_schema = "gold"

# Inicializar Spark Session
spark = SparkSession.builder.appName("Gold_Layer").getOrCreate()

print("🏆 INICIANDO CAMADA GOLD")

# =============================================
# AGREGAÇÃO ANUAL - COVID E ECONOMIA
# =============================================

def criar_agregados_anuais():
    """Cria tabela com agregados anuais de COVID e economia"""
    print("\n📍 Agregados Anuais")
    
    # Carregar dados da Silver
    df_covid = spark.table(f"{catalog_name}.{silver_schema}.covid_enriched")
    df_vaccination = spark.table(f"{catalog_name}.{silver_schema}.vaccination_cleaned")
    df_gdp = spark.table(f"{catalog_name}.{silver_schema}.gdp_cleaned")
    df_country = spark.table(f"{catalog_name}.{silver_schema}.country_cleaned")
    
    # Agregar casos anuais
    df_casos_anual = (df_covid
        .filter(col("year").between(2020, 2023))
        .groupBy("iso_code", "continent", "year")
        .agg(
            sum("new_cases").alias("total_cases_annual"),
            sum("new_deaths").alias("total_deaths_annual"),
            avg("cases_per_million").alias("avg_cases_per_million"),
            avg("deaths_per_million").alias("avg_deaths_per_million"),
            first("population").alias("population"),
            first("gdp_per_capita").alias("gdp_per_capita")
        )
        .filter(col("total_cases_annual").isNotNull())
    )
    
    # Agregar vacinação anual
    df_vac_anual = (df_vaccination
        .withColumn("year", year(col("date")))
        .filter(col("year").between(2020, 2023))
        .groupBy("iso_code", "year")
        .agg(
            max("people_fully_vaccinated").alias("people_fully_vaccinated"),
            max("total_vaccinations").alias("total_vaccinations"),
            max("people_vaccinated_per_hundred").alias("vaccination_rate")
        )
    )
    
    # Join final
    df_final = (df_casos_anual
        .join(df_vac_anual, ["iso_code", "year"], "left")
        .join(df_gdp.select("iso_code", "year", "gdp_growth_percent"), 
              ["iso_code", "year"], "left")
        .join(df_country.select("iso_code", "life_expectancy", "median_age"), 
              "iso_code", "left")
        .filter(col("continent").isNotNull())
    )
    
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{gold_schema}.covid_economic_annual")
    
    print(f"✅ {df_final.count()} registros anuais")
    return df_final

# =============================================
# MÉTRICAS POR CONTINENTE
# =============================================

def criar_metricas_continente():
    """Cria agregados por continente"""
    print("\n📍 Métricas por Continente")
    
    df_anual = spark.table(f"{catalog_name}.{gold_schema}.covid_economic_annual")
    
    df_continente = (df_anual
        .groupBy("continent", "year")
        .agg(
            sum("total_cases_annual").alias("total_cases"),
            sum("total_deaths_annual").alias("total_deaths"),
            avg("gdp_growth_percent").alias("avg_gdp_growth"),
            avg("vaccination_rate").alias("avg_vaccination_rate"),
            sum("population").alias("total_population")
        )
        .withColumn("cases_per_million", (col("total_cases") / col("total_population")) * 1000000)
        .withColumn("deaths_per_million", (col("total_deaths") / col("total_population")) * 1000000)
        .filter(col("total_population") > 0)
    )
    
    df_continente.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{gold_schema}.continent_metrics")
    
    print(f"✅ {df_continente.count()} registros continentais")
    return df_continente

# =============================================
# TOP PAÍSES - INSIGHTS
# =============================================

def criar_top_paises():
    """Cria tabelas com rankings de países"""
    print("\n📍 Top Países")
    
    df_anual = spark.table(f"{catalog_name}.{gold_schema}.covid_economic_annual")
    
    # Países com maior impacto econômico (queda no PIB)
    df_impacto_economico = (df_anual
        .filter(col("year") == 2020)
        .filter(col("gdp_growth_percent").isNotNull())
        .select("iso_code", "continent", "gdp_growth_percent", "total_cases_annual")
        .orderBy("gdp_growth_percent")
        .limit(20)
    )
    
    # Países com melhor resposta (menor mortalidade)
    df_melhor_resposta = (df_anual
        .filter(col("year") == 2021)
        .filter(col("avg_deaths_per_million").isNotNull())
        .select("iso_code", "continent", "avg_deaths_per_million", "vaccination_rate")
        .orderBy("avg_deaths_per_million")
        .limit(20)
    )
    
    df_impacto_economico.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{gold_schema}.top_countries_economic_impact")
    
    df_melhor_resposta.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{gold_schema}.top_countries_best_response")
    
    print(f"✅ Rankings criados")
    return df_impacto_economico, df_melhor_resposta

# =============================================
# CORRELAÇÃO COVID-ECONOMIA
# =============================================

def criar_analise_correlacao():
    """Cria análise de correlação entre COVID e economia"""
    print("\n📍 Análise de Correlação")
    
    df_anual = spark.table(f"{catalog_name}.{gold_schema}.covid_economic_annual")
    
    df_correlacao = (df_anual
        .filter(col("gdp_growth_percent").isNotNull())
        .filter(col("total_cases_annual").isNotNull())
        .withColumn("cases_per_capita", col("total_cases_annual") / col("population"))
        .groupBy("continent", "year")
        .agg(
            count("*").alias("country_count"),
            avg("gdp_growth_percent").alias("avg_gdp_growth"),
            avg("cases_per_capita").alias("avg_cases_per_capita"),
            avg("vaccination_rate").alias("avg_vaccination_rate"),
            corr("gdp_growth_percent", "cases_per_capita").alias("correlation_gdp_cases")
        )
        .filter(col("country_count") > 5)  # Mínimo de países para correlação
    )
    
    df_correlacao.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{gold_schema}.covid_economy_correlation")
    
    print(f"✅ {df_correlacao.count()} análises de correlação")
    return df_correlacao

# =============================================
# RESUMO EXECUTIVO
# =============================================

def criar_resumo_executivo():
    """Cria tabela resumo para dashboards"""
    print("\n📍 Resumo Executivo")
    
    df_continente = spark.table(f"{catalog_name}.{gold_schema}.continent_metrics")
    df_correlacao = spark.table(f"{catalog_name}.{gold_schema}.covid_economy_correlation")
    
    df_resumo = (df_continente
        .join(df_correlacao.select("continent", "year", "correlation_gdp_cases"), 
              ["continent", "year"])
        .withColumn("severity_index", col("deaths_per_million") * col("cases_per_million") / 1000)
        .withColumn("recovery_index", 
                   when(col("avg_gdp_growth") > 0, col("avg_vaccination_rate") * col("avg_gdp_growth"))
                   .otherwise(col("avg_vaccination_rate") * 0.5))
    )
    
    df_resumo.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{gold_schema}.executive_summary")
    
    print(f"✅ Resumo executivo criado")
    return df_resumo

# =============================================
# EXECUÇÃO PRINCIPAL
# =============================================

def executar_camada_gold():
    """Executa todo o processamento da camada Gold"""
    
    print("🎯 EXECUTANDO PROCESSAMENTO GOLD")
    
    # Criar schema se necessário
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema}")
    
    # Executar transformações
    criar_agregados_anuais()
    criar_metricas_continente()
    criar_top_paises()
    criar_analise_correlacao()
    criar_resumo_executivo()
    
    print("\n✅ GOLD CONCLUÍDA")

# =============================================
# RELATÓRIO FINAL
# =============================================

def gerar_relatorio_gold():
    """Gera relatório final da Gold"""
    print("\n" + "="*50)
    print("📊 RELATÓRIO - GOLD")
    print("="*50)
    
    tabelas = spark.sql(f"SHOW TABLES IN {catalog_name}.{gold_schema}").collect()
    
    for tabela in tabelas:
        nome = tabela[1]
        df = spark.table(f"{catalog_name}.{gold_schema}.{nome}")
        print(f"▸ {nome}: {df.count():,} registros")
        
        # Mostrar amostra para tabela principal
        if "covid_economic_annual" in nome:
            print("   📋 Amostra:")
            df.limit(3).show(truncate=False)

# =============================================
# EXECUÇÃO
# =============================================

if __name__ == "__main__":
    executar_camada_gold()
    gerar_relatorio_gold()
```

## Código de Insights da Gold (Análises e Consultas)

```python
# =============================================
# VERIFICAÇÃO E INSIGHTS GOLD 
# =============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Configurações (as mesmas da Gold)
catalog_name = "`data-stage`"
gold_schema = "gold"

spark = SparkSession.builder.appName("Verificacao_Gold").getOrCreate()

def verificar_insights_gold():
    """Gera insights analíticos da camada Gold"""
    print("\n🔍 INSIGHTS - CAMADA GOLD")
    
    try:
        # 1. Top países - impacto econômico
        print("\n📉 TOP 10 - MAIOR QUEDA NO PIB (2020):")
        df_econ = spark.table(f"{catalog_name}.{gold_schema}.top_countries_economic_impact")
        df_econ.select("iso_code", "continent", "gdp_growth_percent").show(10, truncate=False)
        
        # 2. Métricas por continente
        print("\n🌍 MÉTRICAS POR CONTINENTE (2021):")
        df_cont = spark.table(f"{catalog_name}.{gold_schema}.continent_metrics")
        df_cont.filter(col("year") == 2021).select(
            "continent", "total_cases", "total_deaths", "avg_gdp_growth"
        ).show(truncate=False)
        
        # 3. Dados anuais resumidos
        print("\n📈 DADOS ANUAIS - AMOSTRA:")
        df_anual = spark.table(f"{catalog_name}.{gold_schema}.covid_economic_annual")
        df_anual.filter(col("year") == 2021).select(
            "iso_code", "continent", "total_cases_annual", "gdp_growth_percent", "vaccination_rate"
        ).limit(5).show(truncate=False)
        
    except Exception as e:
        print(f"❌ Erro na verificação: {str(e)}")

# =============================================
# CONSULTAS SQL - ANÁLISE BUSINESS 
# =============================================

def consultas_analiticas():
    """Consultas SQL prontas para análise business"""
    
    print("\n💼 CONSULTAS PARA ANÁLISE BUSINESS")
    
    # 1. Países com maior recuperação econômica pós-vacinação
    print("\n1. Países com melhor recuperação (2021):")
    spark.sql(f"""
        SELECT 
            iso_code,
            continent,
            gdp_growth_percent as gdp_2021,
            vaccination_rate,
            (gdp_growth_percent * vaccination_rate) as recovery_score
        FROM {catalog_name}.{gold_schema}.covid_economic_annual
        WHERE year = 2021 
            AND gdp_growth_percent > 0
            AND vaccination_rate > 50
        ORDER BY recovery_score DESC
        LIMIT 10
    """).show(truncate=False)
    
    # 2. Continentes com maior resiliência 
    print("\n2. Resiliência por continente:")
    spark.sql(f"""
        SELECT 
            continent,
            AVG(gdp_growth_percent) as avg_gdp_growth_2020_2021,
            AVG(vaccination_rate) as avg_vaccination_rate,
            AVG(total_cases_annual) as avg_casos_anuais,
            AVG(avg_casos_per_million) as avg_casos_por_milhao
        FROM {catalog_name}.{gold_schema}.covid_economic_annual
        WHERE year BETWEEN 2020 AND 2021
        GROUP BY continent
        ORDER BY avg_gdp_growth_2020_2021 DESC
    """).show(truncate=False)
    
    # 3. Correlação entre vacinação e crescimento econômico
    print("\n3. Impacto da vacinação no PIB (2021):")
    spark.sql(f"""
        SELECT 
            CASE 
                WHEN vaccination_rate < 30 THEN 'Baixa Vacinação'
                WHEN vaccination_rate BETWEEN 30 AND 60 THEN 'Média Vacinação' 
                ELSE 'Alta Vacinação'
            END as faixa_vacinacao,
            COUNT(*) as numero_paises,
            AVG(gdp_growth_percent) as media_crescimento_pib,
            AVG(total_cases_annual) as media_casos,
            AVG(avg_casos_per_million) as media_casos_por_milhao
        FROM {catalog_name}.{gold_schema}.covid_economic_annual
        WHERE year = 2021
        GROUP BY faixa_vacinacao
        ORDER BY media_crescimento_pib DESC
    """).show(truncate=False)

# =============================================
# RELATÓRIO EXECUTIVO
# =============================================

def gerar_relatorio_executivo():
    """Gera relatório executivo consolidado"""
    print("\n📊 RELATÓRIO EXECUTIVO - IMPACTO COVID-ECONOMIA")
    print("="*60)
    
    # Resumo global
    print("\n🌎 VISÃO GLOBAL (2020-2021):")
    spark.sql(f"""
        SELECT 
            '2020' as ano,
            SUM(total_cases) as total_casos_globais,
            SUM(total_deaths) as total_obitos_globais,
            AVG(avg_gdp_growth) as media_crescimento_pib_global
        FROM {catalog_name}.{gold_schema}.continent_metrics
        WHERE year = 2020
        
        UNION ALL
        
        SELECT 
            '2021' as ano,
            SUM(total_cases) as total_casos_globais,
            SUM(total_deaths) as total_obitos_globais,
            AVG(avg_gdp_growth) as media_crescimento_pib_global
        FROM {catalog_name}.{gold_schema}.continent_metrics
        WHERE year = 2021
    """).show(truncate=False)
    
    # Top 5 países por recuperação
    print("\n🏆 TOP 5 PAÍSES - MELHOR RECUPERAÇÃO (2021):")
    spark.sql(f"""
        SELECT 
            iso_code,
            continent,
            gdp_growth_percent as crescimento_pib_2021,
            vaccination_rate as taxa_vacinacao,
            total_cases_annual as casos_2021,
            (gdp_growth_percent * vaccination_rate / 100) as indice_recuperacao
        FROM {catalog_name}.{gold_schema}.covid_economic_annual
        WHERE year = 2021 
            AND gdp_growth_percent > 0
            AND vaccination_rate > 50
        ORDER BY indice_recuperacao DESC
        LIMIT 5
    """).show(truncate=False)
    
    # Correlação por continente
    print("\n📈 CORRELAÇÃO VACINAÇÃO x CRESCIMENTO PIB:")
    spark.sql(f"""
        SELECT 
            continent,
            year,
            avg_vaccination_rate,
            avg_gdp_growth,
            correlation_gdp_cases,
            CASE 
                WHEN correlation_gdp_cases > 0.5 THEN 'Forte Positiva'
                WHEN correlation_gdp_cases > 0.2 THEN 'Moderada Positiva'
                WHEN correlation_gdp_cases > -0.2 THEN 'Fraca'
                WHEN correlation_gdp_cases > -0.5 THEN 'Moderada Negativa'
                ELSE 'Forte Negativa'
            END as intensidade_correlacao
        FROM {catalog_name}.{gold_schema}.covid_economy_correlation
        WHERE year IN (2020, 2021)
        ORDER BY continent, year
    """).show(truncate=False)

# =============================================
# EXECUÇÃO PRINCIPAL
# =============================================

def executar_insights_gold():
    """Executa todos os insights da camada Gold"""
    
    print("🎯 EXECUTANDO ANÁLISES E INSIGHTS")
    
    # Verificação básica dos dados
    verificar_insights_gold()
    
    # Consultas analíticas
    consultas_analiticas()
    
    # Relatório executivo
    gerar_relatorio_executivo()
    
    print("\n✅ INSIGHTS CONCLUÍDOS - DADOS PRONTOS PARA DECISÃO")

# =============================================
# EXECUÇÃO
# =============================================

if __name__ == "__main__":
    executar_insights_gold()
```

## Resumo Completo da Sequência de Execução:

1. **Primeiro**: Executar o código de **Configuração Inicial**
2. **Segundo**: Executar a **Camada Bronze** 
3. **Terceiro**: Executar a **Camada Silver**
4. **Quarto**: Executar a **Camada Gold**
5. **Quinto**: Executar os **Insights da Gold**

Cada código é independente e deve ser executado nessa ordem para garantir que o pipeline funcione corretamente. Os insights da Gold são opcionais e servem para gerar análises business e relatórios executivos com base nos dados já processados.
