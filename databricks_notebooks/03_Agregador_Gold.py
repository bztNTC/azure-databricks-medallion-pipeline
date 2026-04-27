# Databricks notebook source
import time
from pyspark.sql.functions import col, count

# 1. INÍCIO DO CRONÔMETRO E PARÂMETROS
inicio_processamento = time.time()

dbutils.widgets.text("adf_run_id", "execucao_manual")
meu_run_id = dbutils.widgets.get("adf_run_id")
nome_notebook = "03_Agregador_Gold"

# 2. LEITURA DA CAMADA SILVER
df_ml = spark.table("poc_segmentation.silver.dados_limpos_ml")
df_excecoes = spark.table("poc_segmentation.silver.dados_excecoes")
df_erros = spark.table("poc_segmentation.silver.quarentena_erros")

# 3. AGREGAÇÕES
df_gold_tendencia = df_ml \
    .groupBy("country", "dynamic", "consumption_focus", "date") \
    .agg(count("id").alias("total_estabelecimentos"))

df_gold_excecoes = df_excecoes \
    .groupBy("country", "exception_cases", "date") \
    .agg(count("id").alias("total_excecoes_negocio"))

df_gold_qualidade = df_erros \
    .groupBy("country", "status_linha", "date") \
    .agg(count("id").alias("total_erros_preenchimento"))

# 4. ESCRITA NA CAMADA GOLD
df_gold_tendencia.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("poc_segmentation.gold.tendencia_temporal")

df_gold_excecoes.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("poc_segmentation.gold.excecoes_detalhe")

df_gold_qualidade.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("poc_segmentation.gold.qualidade_dados")

# 5. OTIMIZAÇÃO DE PERFORMANCE PARA O POWER BI
spark.sql("OPTIMIZE poc_segmentation.gold.tendencia_temporal ZORDER BY (country, date)")
spark.sql("OPTIMIZE poc_segmentation.gold.excecoes_detalhe ZORDER BY (country, date)")
spark.sql("OPTIMIZE poc_segmentation.gold.qualidade_dados ZORDER BY (country, date)")

# 6. GRAVAÇÃO DO LOG DE PERFORMANCE
fim_processamento = time.time()
tempo_segundos = round(fim_processamento - inicio_processamento, 2)

# Soma as linhas de origem processadas nesta etapa
qtd_linhas_processadas = df_ml.count() + df_excecoes.count() + df_erros.count()

spark.sql(f"""
    INSERT INTO poc_segmentation.gold.log_execucao_pipeline
    VALUES (current_timestamp(), '{meu_run_id}', '{nome_notebook}', {qtd_linhas_processadas}, {tempo_segundos}, 'SUCESSO')
""")

# 7. MENSAGENS FINAIS
print("Pipeline Gold Atualizada! Granularidade definida para nível DIÁRIO com Z-Order aplicado.")
print(f"Log gravado: {qtd_linhas_processadas} linhas de origem processadas em {tempo_segundos} segundos. RunID: {meu_run_id}")