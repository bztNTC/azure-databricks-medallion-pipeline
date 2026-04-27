# Databricks notebook source
import time
from pyspark.sql.functions import current_timestamp

# 1. INÍCIO DO CRONÔMETRO E PARÂMETROS
inicio_processamento = time.time()
dbutils.widgets.text("adf_run_id", "execucao_manual")
meu_run_id = dbutils.widgets.get("adf_run_id")
nome_notebook = "01_Ingestao_Bronze"

caminho_origem = "abfss://dados@stbrunoportfolio230608.dfs.core.windows.net/landing/"
tabela_destino = "poc_segmentation.bronze.dados_brutos"

# 2. LEITURA DA LANDING ZONE (CSV) E TRANSFORMAÇÃO BÁSICA
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .load(caminho_origem)

df_bronze = df_raw \
    .withColumnRenamed("consumption focus", "consumption_focus") \
    .withColumnRenamed("exception cases", "exception_cases") \
    .withColumn("ingestion_date", current_timestamp())

# 3. ESCRITA NA CAMADA BRONZE
df_bronze.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(tabela_destino)

# 4. GRAVAÇÃO DO LOG DE PERFORMANCE
fim_processamento = time.time()
tempo_segundos = round(fim_processamento - inicio_processamento, 2)
qtd_linhas = df_bronze.count()

spark.sql(f"""
    INSERT INTO poc_segmentation.gold.log_execucao_pipeline
    VALUES (current_timestamp(), '{meu_run_id}', '{nome_notebook}', {qtd_linhas}, {tempo_segundos}, 'SUCESSO')
""")

# 5. MENSAGENS FINAIS
print(f"Ingestão finalizada com sucesso! Tabela: {tabela_destino}")
print(f"Log gravado: {qtd_linhas} linhas em {tempo_segundos} segundos. RunID: {meu_run_id}")