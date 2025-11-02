# Documentação Completa - Pipeline ETL COVID-19 Google Colab

## 📋 Índice
1. [Configuração Inicial](#1-configuração-inicial)
2. [Camada Bronze](#2-camada-bronze)
3. [Camada Silver](#3-camada-silver) 
4. [Camada Gold](#4-camada-gold)
5. [API REST](#5-api-rest)
6. [DAG Airflow](#6-dag-airflow)

---

## 1. Configuração Inicial

### 1.1 Instalação de Dependências
```python
# Célula 1: Instalação de pacotes
!pip install pyspark==3.4.0
!pip install delta-spark==2.4.0
!pip install pandas requests flask flask-httpauth flask-cors
!apt-get install openjdk-11-jdk-headless -qq > /dev/null

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
```

### 1.2 Configuração Spark
```python
# Célula 2: Sessão Spark
from pyspark.sql import SparkSession
from delta import *

builder = SparkSession.builder \
    .appName("ColabCOVIDAnalysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print("✅ Spark Session criada com sucesso!")
```

### 1.3 Estrutura de Diretórios
```python
# Célula 3: Configuração de paths
BASE_PATH = "/content/covid_data"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"

for path in [BASE_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH]:
    os.makedirs(path, exist_ok=True)

print("✅ Diretórios criados com sucesso!")
print(f"📁 Bronze: {BRONZE_PATH}")
print(f"📁 Silver: {SILVER_PATH}")
print(f"📁 Gold: {GOLD_PATH}")
```

---

## 2. Camada Bronze

### 2.1 Código Completo Bronze
```python
# Célula 4: Camada Bronze Completa
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import requests
import pandas as pd
import os

# Configurações
BRONZE_PATH = "/content/covid_data/bronze"

print("🔄 INICIANDO CAMADA BRONZE - INGESTÃO DE DADOS")

def processar_api_worldbank():
    """Processa dados econômicos do World Bank API"""
    try:
        url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.KD.ZG?format=json&per_page=10000"
        response = requests.get(url)
        data = response.json()
        
        records = []
        for item in data[1]:
            record = {
                'country_iso': item['country']['id'],
                'country_name': item['country']['value'],
                'year': int(item['date']),
                'gdp_growth': item['value'],
                'indicator_name': item['indicator']['value']
            }
            records.append(record)
        
        df = spark.createDataFrame(records)
        return df
    except Exception as e:
        print(f"❌ Erro na API World Bank: {e}")
        return None

def aplicar_prefixo_colunas(df, prefixo):
    """Aplica prefixo nas colunas para evitar conflitos"""
    for i, coluna in enumerate(df.columns):
        df = df.withColumnRenamed(coluna, f"{prefixo}_{i:02d}")
    return df

# 1. Country Dataset
print("📍 PROCESSANDO country_dataset.csv")
try:
    df_country = spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("inferSchema", "true") \
        .csv("/content/country_dataset.csv")
    
    df_country_prefixed = aplicar_prefixo_colunas(df_country, "cou")
    df_country_prefixed.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{BRONZE_PATH}/country_raw")
    print(f"✅ Country: {df_country.count()} registros")
except Exception as e:
    print(f"❌ Erro country: {e}")

# 2. Cases Dataset
print("📍 PROCESSANDO cases_dataset.csv")
try:
    df_cases = spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("inferSchema", "true") \
        .csv("/content/cases_dataset.csv")
    
    df_cases_prefixed = aplicar_prefixo_colunas(df_cases, "cas")
    df_cases_prefixed.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{BRONZE_PATH}/cases_raw")
    print(f"✅ Cases: {df_cases.count()} registros")
except Exception as e:
    print(f"❌ Erro cases: {e}")

# 3. Vaccination Dataset
print("📍 PROCESSANDO vaccination_dataset.csv")
try:
    df_vaccination = spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("inferSchema", "true") \
        .csv("/content/vaccination_dataset.csv")
    
    df_vaccination_prefixed = aplicar_prefixo_colunas(df_vaccination, "vac")
    df_vaccination_prefixed.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{BRONZE_PATH}/vaccination_raw")
    print(f"✅ Vaccination: {df_vaccination.count()} registros")
except Exception as e:
    print(f"❌ Erro vaccination: {e}")

# 4. Hospital Dataset
print("📍 PROCESSANDO hospital_dataset.csv")
try:
    df_hospital = spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("inferSchema", "true") \
        .csv("/content/hospital_dataset.csv")
    
    df_hospital_prefixed = aplicar_prefixo_colunas(df_hospital, "hos")
    df_hospital_prefixed.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{BRONZE_PATH}/hospital_raw")
    print(f"✅ Hospital: {df_hospital.count()} registros")
except Exception as e:
    print(f"❌ Erro hospital: {e}")

# 5. Tests Dataset
print("📍 PROCESSANDO tests_dataset.csv")
try:
    df_tests = spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("inferSchema", "true") \
        .csv("/content/tests_dataset.csv")
    
    df_tests_prefixed = aplicar_prefixo_colunas(df_tests, "tes")
    df_tests_prefixed.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{BRONZE_PATH}/tests_raw")
    print(f"✅ Tests: {df_tests.count()} registros")
except Exception as e:
    print(f"❌ Erro tests: {e}")

# 6. GDP Data (API World Bank)
print("📍 PROCESSANDO DADOS GDP DA API")
try:
    df_gdp = processar_api_worldbank()
    if df_gdp is not None:
        df_gdp_prefixed = aplicar_prefixo_colunas(df_gdp, "gdp")
        df_gdp_prefixed.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{BRONZE_PATH}/gdp_raw")
        print(f"✅ GDP: {df_gdp.count()} registros")
    else:
        print("⚠️  API não retornou dados")
except Exception as e:
    print(f"❌ Erro GDP: {e}")

print("🎉 CAMADA BRONZE CONCLUÍDA!")

# Verificação final
print("\n🔍 VERIFICAÇÃO BRONZE:")
tabelas = ["country_raw", "cases_raw", "vaccination_raw", "hospital_raw", "tests_raw", "gdp_raw"]
for tabela in tabelas:
    try:
        df = spark.read.format("delta").load(f"{BRONZE_PATH}/{tabela}")
        print(f"✅ {tabela}: {df.count()} registros, {len(df.columns)} colunas")
    except Exception as e:
        print(f"❌ {tabela}: {e}")
```

---

## 3. Camada Silver

### 3.1 Código Completo Silver
```python
# Célula 5: Camada Silver Completa
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Configurações
BRONZE_PATH = "/content/covid_data/bronze"
SILVER_PATH = "/content/covid_data/silver"

print("🔄 INICIANDO CAMADA SILVER - PROCESSAMENTO E QUALIDADE")

def converter_formato_numerico(coluna):
    """Padroniza formatos numéricos internacionais para double"""
    return (regexp_replace(col(coluna), ",", ".")
            .cast("double"))

def salvar_tabela_silver(df, nome_tabela):
    """Salva tabela na Silver com merge schema"""
    df.write \
        .format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .save(f"{SILVER_PATH}/{nome_tabela}")
    print(f"✅ {nome_tabela}: {df.count():,} registros")

def processar_country():
    """Processa dados demográficos e socioeconômicos dos países"""
    print("📍 Processando dados demográficos...")

    df_country = spark.read.format("delta").load(f"{BRONZE_PATH}/country_raw")

    df_processed = (df_country
        .withColumn("population_density", converter_formato_numerico("cou_01"))
        .withColumn("median_age", converter_formato_numerico("cou_02"))
        .withColumn("aged_65_older", converter_formato_numerico("cou_03"))
        .withColumn("aged_70_older", converter_formato_numerico("cou_04"))
        .withColumn("gdp_per_capita", converter_formato_numerico("cou_05"))
        .withColumn("extreme_poverty", converter_formato_numerico("cou_06"))
        .withColumn("cardiovasc_death_rate", converter_formato_numerico("cou_07"))
        .withColumn("diabetes_prevalence", converter_formato_numerico("cou_08"))
        .withColumn("female_smokers", converter_formato_numerico("cou_09"))
        .withColumn("male_smokers", converter_formato_numerico("cou_10"))
        .withColumn("handwashing_facilities", converter_formato_numerico("cou_11"))
        .withColumn("hospital_beds_per_thousand", converter_formato_numerico("cou_12"))
        .withColumn("life_expectancy", converter_formato_numerico("cou_13"))
        .withColumn("human_development_index", converter_formato_numerico("cou_14"))
        .withColumn("population", col("cou_15").cast("bigint"))
        .filter(col("cou_00").isNotNull())
        .filter(col("population") > 0)
        .withColumnRenamed("cou_00", "iso_code")
        .drop(*[f"cou_{i:02d}" for i in range(16)])
    )

    salvar_tabela_silver(df_processed, "country_cleaned")
    return df_processed

def processar_cases():
    """Processa dados epidemiológicos de casos e óbitos"""
    print("📍 Processando dados de casos e óbitos...")

    df_cases = spark.read.format("delta").load(f"{BRONZE_PATH}/cases_raw")

    mapeamento_colunas = {
        'cas_00': 'iso_code', 'cas_01': 'continent', 'cas_02': 'location',
        'cas_03': 'date', 'cas_04': 'total_cases', 'cas_05': 'new_cases',
        'cas_06': 'new_cases_smoothed', 'cas_07': 'total_deaths',
        'cas_08': 'new_deaths', 'cas_09': 'new_deaths_smoothed',
        'cas_10': 'total_cases_per_million', 'cas_11': 'new_cases_per_million',
        'cas_12': 'new_cases_smoothed_per_million', 'cas_13': 'total_deaths_per_million',
        'cas_14': 'new_deaths_per_million', 'cas_15': 'new_deaths_smoothed_per_million'
    }

    df_renomeado = df_cases
    for col_antiga, col_nova in mapeamento_colunas.items():
        if col_antiga in df_renomeado.columns:
            df_renomeado = df_renomeado.withColumnRenamed(col_antiga, col_nova)

    colunas_numericas = [
        'total_cases', 'new_cases', 'new_cases_smoothed', 'total_deaths',
        'new_deaths', 'new_deaths_smoothed', 'total_cases_per_million',
        'new_cases_per_million', 'new_cases_smoothed_per_million',
        'total_deaths_per_million', 'new_deaths_per_million',
        'new_deaths_smoothed_per_million'
    ]

    df_processed = df_renomeado
    for coluna in colunas_numericas:
        if coluna in df_processed.columns:
            df_processed = df_processed.withColumn(coluna, converter_formato_numerico(coluna))

    df_processed = (df_processed
        .withColumn("date", to_date(col("date"), "dd/MM/yyyy"))
        .filter(col("iso_code").isNotNull())
        .filter(col("date").isNotNull())
        .filter(col("continent").isNotNull())
    )

    salvar_tabela_silver(df_processed, "cases_cleaned")
    return df_processed

def processar_vaccination():
    """Processa dados de campanhas de vacinação"""
    print("📍 Processando dados de vacinação...")

    df_vaccination = spark.read.format("delta").load(f"{BRONZE_PATH}/vaccination_raw")

    mapeamento_colunas = {
        'vac_00': 'iso_code', 'vac_01': 'date', 'vac_02': 'total_vaccinations',
        'vac_03': 'people_vaccinated', 'vac_04': 'people_fully_vaccinated',
        'vac_05': 'total_boosters', 'vac_06': 'new_vaccinations',
        'vac_07': 'new_vaccinations_smoothed', 'vac_08': 'total_vaccinations_per_hundred',
        'vac_09': 'people_vaccinated_per_hundred', 'vac_10': 'people_fully_vaccinated_per_hundred',
        'vac_11': 'total_boosters_per_hundred', 'vac_12': 'new_vaccinations_smoothed_per_million',
        'vac_13': 'new_people_vaccinated_smoothed', 'vac_14': 'new_people_vaccinated_smoothed_per_hundred'
    }

    df_renomeado = df_vaccination
    for col_antiga, col_nova in mapeamento_colunas.items():
        if col_antiga in df_renomeado.columns:
            df_renomeado = df_renomeado.withColumnRenamed(col_antiga, col_nova)

    colunas_numericas = [
        'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
        'total_boosters', 'new_vaccinations', 'new_vaccinations_smoothed',
        'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred',
        'people_fully_vaccinated_per_hundred', 'total_boosters_per_hundred',
        'new_vaccinations_smoothed_per_million', 'new_people_vaccinated_smoothed',
        'new_people_vaccinated_smoothed_per_hundred'
    ]

    df_processed = df_renomeado
    for coluna in colunas_numericas:
        if coluna in df_processed.columns:
            df_processed = df_processed.withColumn(coluna, converter_formato_numerico(coluna))

    df_processed = (df_processed
        .withColumn("date", to_date(col("date"), "dd/MM/yyyy"))
        .filter(col("iso_code").isNotNull())
        .filter(col("date").isNotNull())
    )

    salvar_tabela_silver(df_processed, "vaccination_cleaned")
    return df_processed

def processar_gdp():
    """Processa indicadores econômicos do World Bank"""
    print("📍 Processando dados econômicos...")

    df_gdp = spark.read.format("delta").load(f"{BRONZE_PATH}/gdp_raw")

    mapeamento_colunas = {
        'gdp_00': 'iso_code', 'gdp_01': 'year', 'gdp_02': 'gdp_growth_percent',
        'gdp_03': 'country_name', 'gdp_04': 'country_id', 'gdp_05': 'indicator_name'
    }

    df_renomeado = df_gdp
    for col_antiga, col_nova in mapeamento_colunas.items():
        if col_antiga in df_renomeado.columns:
            df_renomeado = df_renomeado.withColumnRenamed(col_antiga, col_nova)

    df_processed = (df_renomeado
        .withColumn("year", col("year").cast("integer"))
        .withColumn("gdp_growth_percent", converter_formato_numerico("gdp_growth_percent"))
        .filter(col("iso_code").isNotNull())
        .filter(col("year").between(2019, 2023))
        .filter(col("gdp_growth_percent").isNotNull())
    )

    salvar_tabela_silver(df_processed, "gdp_cleaned")
    return df_processed

def enriquecer_dados():
    """Integra e enriquece dados para análise multidimensional"""
    print("📍 Integrando e enriquecendo dados...")

    df_cases = spark.read.format("delta").load(f"{SILVER_PATH}/cases_cleaned")
    df_country = spark.read.format("delta").load(f"{SILVER_PATH}/country_cleaned")

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
        .withColumn("quarter", quarter(col("date")))
        .filter(col("iso_code").isNotNull())
        .filter(col("date").isNotNull())
    )

    salvar_tabela_silver(df_enriched, "covid_enriched")
    return df_enriched

def executar_camada_silver():
    """Orquestra todo o processamento da camada Silver"""
    print("🎯 INICIANDO PROCESSAMENTO SILVER")
    print("=" * 50)

    processar_country()
    processar_cases()
    processar_vaccination()
    processar_gdp()
    enriquecer_dados()

    print("=" * 50)
    print("✅ PROCESSAMENTO SILVER CONCLUÍDO")

def verificar_qualidade_silver():
    """Valida a qualidade dos dados processados"""
    print("\n🔍 VERIFICAÇÃO DE QUALIDADE - SILVER")

    tabelas_silver = [
        "country_cleaned", "cases_cleaned", "vaccination_cleaned",
        "gdp_cleaned", "covid_enriched"
    ]

    for tabela in tabelas_silver:
        try:
            df = spark.read.format("delta").load(f"{SILVER_PATH}/{tabela}")
            print(f"📊 {tabela}: {df.count():,} registros | {len(df.columns)} colunas")
        except Exception as e:
            print(f"❌ {tabela}: {str(e)}")

# Execução
executar_camada_silver()
verificar_qualidade_silver()
```

---

## 4. Camada Gold

### 4.1 Código Completo Gold
```python
# Célula 6: Camada Gold Completa
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Configurações
SILVER_PATH = "/content/covid_data/silver"
GOLD_PATH = "/content/covid_data/gold"

print("🏆 INICIANDO CAMADA GOLD - MODELAGEM ANALÍTICA")

def salvar_tabela_gold(df, nome_tabela):
    """Salva tabela na Gold com merge schema"""
    df.write \
        .format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}/{nome_tabela}")
    print(f"✅ {nome_tabela}: {df.count():,} registros")

def criar_agregados_anuais():
    """Cria visão consolidada anual de COVID e economia"""
    print("📍 Criando agregados anuais...")

    df_covid = spark.read.format("delta").load(f"{SILVER_PATH}/covid_enriched")
    df_vaccination = spark.read.format("delta").load(f"{SILVER_PATH}/vaccination_cleaned")
    df_gdp = spark.read.format("delta").load(f"{SILVER_PATH}/gdp_cleaned")
    df_country = spark.read.format("delta").load(f"{SILVER_PATH}/country_cleaned")

    df_casos_anual = (df_covid
        .filter(col("year").between(2020, 2023))
        .groupBy("iso_code", "continent", "location", "year")
        .agg(
            sum("new_cases").alias("total_cases_annual"),
            sum("new_deaths").alias("total_deaths_annual"),
            avg("cases_per_million").alias("avg_cases_per_million"),
            avg("deaths_per_million").alias("avg_deaths_per_million"),
            first("population").alias("population"),
            first("gdp_per_capita").alias("gdp_per_capita"),
            first("life_expectancy").alias("life_expectancy")
        )
        .filter(col("total_cases_annual").isNotNull())
    )

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

    df_final = (df_casos_anual
        .join(df_vac_anual, ["iso_code", "year"], "left")
        .join(df_gdp.select("iso_code", "year", "gdp_growth_percent"),
              ["iso_code", "year"], "left")
        .filter(col("continent").isNotNull())
    )

    salvar_tabela_gold(df_final, "covid_economic_annual")
    return df_final

def criar_metricas_continente():
    """Cria visão agregada por continente"""
    print("📍 Criando métricas continentais...")

    df_anual = spark.read.format("delta").load(f"{GOLD_PATH}/covid_economic_annual")

    df_continente = (df_anual
        .groupBy("continent", "year")
        .agg(
            sum("total_cases_annual").alias("total_cases"),
            sum("total_deaths_annual").alias("total_deaths"),
            avg("gdp_growth_percent").alias("avg_gdp_growth"),
            avg("vaccination_rate").alias("avg_vaccination_rate"),
            sum("population").alias("total_population"),
            count("iso_code").alias("country_count")
        )
        .withColumn("cases_per_million",
                   when(col("total_population") > 0,
                        (col("total_cases") / col("total_population")) * 1000000)
                   .otherwise(None))
        .withColumn("deaths_per_million",
                   when(col("total_population") > 0,
                        (col("total_deaths") / col("total_population")) * 1000000)
                   .otherwise(None))
        .filter(col("total_population") > 0)
    )

    salvar_tabela_gold(df_continente, "continent_metrics")
    return df_continente

def criar_rankings_paises():
    """Cria rankings para análise comparativa"""
    print("📍 Criando rankings estratégicos...")

    df_anual = spark.read.format("delta").load(f"{GOLD_PATH}/covid_economic_annual")

    df_impacto_economico = (df_anual
        .filter(col("year") == 2020)
        .filter(col("gdp_growth_percent").isNotNull())
        .select("iso_code", "continent", "location", "gdp_growth_percent", "total_cases_annual")
        .orderBy("gdp_growth_percent")
        .limit(20)
    )

    df_melhor_resposta = (df_anual
        .filter(col("year") == 2021)
        .filter(col("avg_deaths_per_million").isNotNull())
        .select("iso_code", "continent", "location", "avg_deaths_per_million", "vaccination_rate")
        .orderBy("avg_deaths_per_million")
        .limit(20)
    )

    salvar_tabela_gold(df_impacto_economico, "top_countries_economic_impact")
    salvar_tabela_gold(df_melhor_resposta, "top_countries_best_response")

    return df_impacto_economico, df_melhor_resposta

def criar_analise_correlacao():
    """Analisa correlações entre variáveis COVID e economia"""
    print("📍 Analisando correlações...")

    df_anual = spark.read.format("delta").load(f"{GOLD_PATH}/covid_economic_annual")

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
        .filter(col("country_count") > 5)
    )

    salvar_tabela_gold(df_correlacao, "covid_economy_correlation")
    return df_correlacao

def criar_resumo_executivo():
    """Cria visão consolidada para tomada de decisão"""
    print("📍 Criando resumo executivo...")

    df_continente = spark.read.format("delta").load(f"{GOLD_PATH}/continent_metrics")
    df_correlacao = spark.read.format("delta").load(f"{GOLD_PATH}/covid_economy_correlation")

    df_resumo = (df_continente
        .join(df_correlacao.select("continent", "year", "correlation_gdp_cases"),
              ["continent", "year"])
        .withColumn("severity_index", col("deaths_per_million") * col("cases_per_million") / 1000)
        .withColumn("recovery_index",
                   when(col("avg_gdp_growth") > 0,
                        col("avg_vaccination_rate") * col("avg_gdp_growth"))
                   .otherwise(col("avg_vaccination_rate") * 0.5))
        .withColumn("resilience_score",
                   col("recovery_index") - col("severity_index"))
    )

    salvar_tabela_gold(df_resumo, "executive_summary")
    return df_resumo

def executar_camada_gold():
    """Orquestra toda a modelagem da camada Gold"""
    print("🎯 INICIANDO MODELAGEM GOLD")
    print("=" * 50)

    criar_agregados_anuais()
    criar_metricas_continente()
    criar_rankings_paises()
    criar_analise_correlacao()
    criar_resumo_executivo()

    print("=" * 50)
    print("✅ MODELAGEM GOLD CONCLUÍDA")

def verificar_gold():
    """Valida os resultados da camada Gold"""
    print("\n🔍 VERIFICAÇÃO GOLD:")

    tabelas_gold = [
        "covid_economic_annual",
        "continent_metrics",
        "top_countries_economic_impact",
        "top_countries_best_response",
        "covid_economy_correlation",
        "executive_summary"
    ]

    for tabela in tabelas_gold:
        try:
            df = spark.read.format("delta").load(f"{GOLD_PATH}/{tabela}")
            print(f"📊 {tabela}: {df.count():,} registros")
        except Exception as e:
            print(f"❌ {tabela}: {e}")

# Execução
executar_camada_gold()
verificar_gold()
```

---

## 5. API REST

### 5.1 Código Completo API Flask
```python
# Célula 7: API REST Completa
!pip install flask flask-httpauth flask-cors requests pyspark delta-spark pandas -q

print("📦 Dependências instaladas!")

from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
from flask_cors import CORS
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
import pandas as pd
import requests
from datetime import datetime
import os
import json

# Configurar Spark
builder = SparkSession.builder \
    .appName("COVID_ETL_API") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Configurar diretórios
BASE_PATH = "/content/covid_data"
for folder in ["bronze", "silver", "gold"]:
    os.makedirs(f"{BASE_PATH}/{folder}", exist_ok=True)

print("✅ Spark e diretórios configurados!")

# Configuração Flask
app = Flask(__name__)
CORS(app)
auth = HTTPBasicAuth()

# Credenciais
USERS = {
    "covid_user": "covid123"
}

@auth.verify_password
def verify_password(username, password):
    if username in USERS and USERS[username] == password:
        return username
    return None

@auth.error_handler
def auth_error():
    return jsonify({
        'status': 'error',
        'message': 'Acesso não autorizado'
    }), 401

# Funções de processamento
def executar_bronze(data_processamento):
    """Executa camada Bronze - ingestão de dados"""
    try:
        print(f"🚀 Iniciando Bronze - {data_processamento}")
        
        # Verificar arquivos
        arquivos_necessarios = ['country_dataset.csv', 'cases_dataset.csv', 'vaccination_dataset.csv']
        for arquivo in arquivos_necessarios:
            if not os.path.exists(f"/content/{arquivo}"):
                return False, f"Arquivo {arquivo} não encontrado. Faça upload primeiro!"
        
        # Country Dataset
        df_country = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("inferSchema", "true") \
            .csv("/content/country_dataset.csv")
        
        for i, col_name in enumerate(df_country.columns):
            df_country = df_country.withColumnRenamed(col_name, f"cou_{i:02d}")
        
        df_country.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/bronze/country_raw")
        
        # Cases Dataset  
        df_cases = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("inferSchema", "true") \
            .csv("/content/cases_dataset.csv")
        
        for i, col_name in enumerate(df_cases.columns):
            df_cases = df_cases.withColumnRenamed(col_name, f"cas_{i:02d}")
            
        df_cases.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/bronze/cases_raw")
        
        # Vaccination Dataset
        df_vaccination = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("inferSchema", "true") \
            .csv("/content/vaccination_dataset.csv")
        
        for i, col_name in enumerate(df_vaccination.columns):
            df_vaccination = df_vaccination.withColumnRenamed(col_name, f"vac_{i:02d}")
            
        df_vaccination.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/bronze/vaccination_raw")
        
        return True, f"Bronze concluído: {df_country.count()} países, {df_cases.count()} casos"
        
    except Exception as e:
        return False, f"Erro no Bronze: {str(e)}"

def executar_silver(data_processamento):
    """Executa camada Silver - limpeza e enriquecimento"""
    try:
        print(f"🔄 Iniciando Silver - {data_processamento}")
        
        # Processar Country
        df_country = spark.read.format("delta").load(f"{BASE_PATH}/bronze/country_raw")
        
        df_country_clean = (df_country
            .withColumn("population_density", regexp_replace(col("cou_01"), ",", ".").cast("double"))
            .withColumn("median_age", regexp_replace(col("cou_02"), ",", ".").cast("double"))
            .withColumn("gdp_per_capita", regexp_replace(col("cou_05"), ",", ".").cast("double"))
            .withColumn("population", col("cou_15").cast("bigint"))
            .filter(col("cou_00").isNotNull())
            .withColumnRenamed("cou_00", "iso_code")
        )
        
        df_country_clean.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/silver/country_cleaned")
        
        # Processar Cases
        df_cases = spark.read.format("delta").load(f"{BASE_PATH}/bronze/cases_raw")
        
        casos_rename = {
            'cas_00': 'iso_code', 'cas_01': 'continent', 'cas_02': 'location',
            'cas_03': 'date', 'cas_04': 'total_cases', 'cas_05': 'new_cases'
        }
        
        df_cases_renamed = df_cases
        for old, new in casos_rename.items():
            df_cases_renamed = df_cases_renamed.withColumnRenamed(old, new)
        
        df_cases_clean = (df_cases_renamed
            .withColumn("total_cases", regexp_replace(col("total_cases"), ",", ".").cast("double"))
            .withColumn("new_cases", regexp_replace(col("new_cases"), ",", ".").cast("double"))
            .withColumn("date", to_date(col("date"), "dd/MM/yyyy"))
            .filter(col("iso_code").isNotNull())
        )
        
        df_cases_clean.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/silver/cases_cleaned")
        
        # Enriquecer dados
        df_enriched = (df_cases_clean
            .join(df_country_clean.select("iso_code", "population", "gdp_per_capita"), "iso_code", "left")
            .withColumn("cases_per_million", (col("total_cases") / col("population")) * 1000000)
            .withColumn("year", year(col("date")))
            .withColumn("month", month(col("date")))
        )
        
        df_enriched.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/silver/covid_enriched")
        
        return True, f"Silver concluído: {df_enriched.count()} registros enriquecidos"
        
    except Exception as e:
        return False, f"Erro no Silver: {str(e)}"

def executar_gold(data_processamento):
    """Executa camada Gold - agregações e métricas"""
    try:
        print(f"🏆 Iniciando Gold - {data_processamento}")
        
        df_enriched = spark.read.format("delta").load(f"{BASE_PATH}/silver/covid_enriched")
        
        # Agregados anuais
        df_annual = (df_enriched
            .filter(col("year").between(2020, 2023))
            .groupBy("iso_code", "continent", "year")
            .agg(
                sum("new_cases").alias("total_cases_annual"),
                avg("cases_per_million").alias("avg_cases_per_million"),
                first("population").alias("population"),
                first("gdp_per_capita").alias("gdp_per_capita")
            )
        )
        
        df_annual.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/gold/covid_economic_annual")
        
        # Métricas por continente
        df_continent = (df_annual
            .groupBy("continent", "year")
            .agg(
                sum("total_cases_annual").alias("total_cases"),
                sum("population").alias("total_population"),
                count("iso_code").alias("country_count")
            )
            .withColumn("cases_per_million", (col("total_cases") / col("total_population")) * 1000000)
        )
        
        df_continent.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/gold/continent_metrics")
        
        return True, f"Gold concluído: {df_annual.count()} registros anuais, {df_continent.count()} métricas continentais"
        
    except Exception as e:
        return False, f"Erro no Gold: {str(e)}"

# Endpoints
@app.route('/health', methods=['GET'])
@auth.login_required
def health_check():
    return jsonify({
        'status': 'healthy', 
        'timestamp': str(datetime.now()),
        'service': 'COVID-19 ETL API (Authenticated)',
        'version': '2.0',
        'user': auth.current_user()
    })

@app.route('/etl/bronze', methods=['POST'])
@auth.login_required
def api_bronze():
    try:
        data = request.json or {}
        data_processamento = data.get('data_processamento', datetime.now().strftime('%Y-%m-%d'))
        
        success, message = executar_bronze(data_processamento)
        
        return jsonify({
            'status': 'success' if success else 'error',
            'message': message,
            'etapa': 'bronze',
            'data_processamento': data_processamento,
            'timestamp': str(datetime.now()),
            'user': auth.current_user()
        })
    except Exception as e:
        return jsonify({
            'status': 'error', 
            'message': str(e),
            'timestamp': str(datetime.now())
        })

@app.route('/etl/silver', methods=['POST'])
@auth.login_required
def api_silver():
    try:
        data = request.json or {}
        data_processamento = data.get('data_processamento', datetime.now().strftime('%Y-%m-%d'))
        
        success, message = executar_silver(data_processamento)
        
        return jsonify({
            'status': 'success' if success else 'error',
            'message': message,
            'etapa': 'silver',
            'data_processamento': data_processamento,
            'timestamp': str(datetime.now()),
            'user': auth.current_user()
        })
    except Exception as e:
        return jsonify({
            'status': 'error', 
            'message': str(e),
            'timestamp': str(datetime.now())
        })

@app.route('/etl/gold', methods=['POST'])
@auth.login_required
def api_gold():
    try:
        data = request.json or {}
        data_processamento = data.get('data_processamento', datetime.now().strftime('%Y-%m-%d'))
        
        success, message = executar_gold(data_processamento)
        
        return jsonify({
            'status': 'success' if success else 'error',
            'message': message,
            'etapa': 'gold', 
            'data_processamento': data_processamento,
            'timestamp': str(datetime.now()),
            'user': auth.current_user()
        })
    except Exception as e:
        return jsonify({
            'status': 'error', 
            'message': str(e),
            'timestamp': str(datetime.now())
        })

@app.route('/etl/full', methods=['POST'])
@auth.login_required
def api_full_etl():
    """Executa pipeline completo"""
    try:
        data = request.json or {}
        data_processamento = data.get('data_processamento', datetime.now().strftime('%Y-%m-%d'))
        
        results = []
        
        # Bronze
        print("🎯 Executando Bronze...")
        success_bronze, msg_bronze = executar_bronze(data_processamento)
        results.append({'etapa': 'bronze', 'status': success_bronze, 'message': msg_bronze})
        
        if not success_bronze:
            return jsonify({
                'status': 'error',
                'message': 'Falha no Bronze',
                'results': results,
                'timestamp': str(datetime.now())
            })
        
        # Silver
        print("🎯 Executando Silver...")
        success_silver, msg_silver = executar_silver(data_processamento)
        results.append({'etapa': 'silver', 'status': success_silver, 'message': msg_silver})
        
        if not success_silver:
            return jsonify({
                'status': 'error', 
                'message': 'Falha no Silver',
                'results': results,
                'timestamp': str(datetime.now())
            })
        
        # Gold
        print("🎯 Executando Gold...")
        success_gold, msg_gold = executar_gold(data_processamento)
        results.append({'etapa': 'gold', 'status': success_gold, 'message': msg_gold})
        
        overall_success = all([success_bronze, success_silver, success_gold])
        
        return jsonify({
            'status': 'success' if overall_success else 'partial',
            'message': 'Pipeline completo executado',
            'results': results,
            'data_processamento': data_processamento,
            'timestamp': str(datetime.now()),
            'user': auth.current_user()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': str(datetime.now())
        })

# Iniciar servidor
if __name__ == '__main__':
    print("=" * 70)
    print("🔐 COVID-19 ETL API COM AUTENTICAÇÃO")
    print("=" * 70)
    print("📋 CREDENCIAIS:")
    print("   Usuário: covid_user")
    print("   Senha: covid123")
    print("📋 ENDPOINTS:")
    print("   - GET  /health")
    print("   - POST /etl/bronze")
    print("   - POST /etl/silver")
    print("   - POST /etl/gold") 
    print("   - POST /etl/full")
    print("=" * 70)
    
    # Obter URL pública
    from google.colab.output import eval_js
    try:
        public_url = eval_js("google.colab.kernel.proxyPort(5000)")
        print(f"🌐 URL PÚBLICA: {public_url}")
    except:
        print("ℹ️  Execute a célula separada para obter a URL")
    
    # Iniciar servidor
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
```

---
