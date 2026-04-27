# Databricks notebook source
import time
import random
from pyspark.sql.functions import current_date, rand, col, split, when, lit, array, element_at

# 1. INÍCIO DO CRONÔMETRO E PARÂMETROS
inicio_processamento = time.time()
dbutils.widgets.text("adf_run_id", "execucao_manual")
meu_run_id = dbutils.widgets.get("adf_run_id")
nome_notebook = "00_Gerador"

# 2. REGRAS DE GERAÇÃO
paises_validos = ['AR', 'PY', 'BO', 'PE', 'EC', 'CO', 'PA', 'DO', 'HN', 'SV', 'MX', 'ZA']
pais_do_dia = random.choice(paises_validos)     
qtd_linhas = random.randint(3000, 4000)         

caminho_landing = "abfss://dados@stbrunoportfolio230608.dfs.core.windows.net/landing/"

combinacoes = [
    "OFF#BAKERY#", "ON#BAR#", "ON#CAFE#", "OFF#CASH AND CARRY#",
    "OFF#DISTRIBUTOR#", "ON#ENTERTAINMENT#", "OFF#FOOD STORE#",
    "OFF#GAS STATION CONVENIENCE#", "OFF#GROCERY STORE#", "ON#HOSPITALITY#",
    "OFF#MARKET#", "OFF#PHARMACY#", "ON#RESTAURANT#",
    "#OTHERS#PERMANENTLY CLOSED", "#OTHERS#INCONCLUSIVE",
    "#OTHERS#POOR QUALITY", "#OTHERS#FRAUD",
    "#BAR#", "#GROCERY STORE#", "#OTHERS#", "ON#OTHERS#"
]

array_combos = array(*[lit(c) for c in combinacoes])
total_combos = len(combinacoes)

# 3. CRIAÇÃO DO DATAFRAME
df_fake = spark.range(0, qtd_linhas) \
    .withColumn("id", (rand() * 89999999 + 10000000).cast("int")) \
    .withColumn("country", lit(pais_do_dia)) \
    .withColumn("date", current_date()) \
    .withColumn("sorteio", (rand() * total_combos + 1).cast("int")) \
    .withColumn("combo_sorteado", element_at(array_combos, col("sorteio"))) \
    .withColumn("array_temporario", split(col("combo_sorteado"), "#")) \
    .withColumn("consumption focus", col("array_temporario").getItem(0)) \
    .withColumn("dynamic", col("array_temporario").getItem(1)) \
    .withColumn("exception cases", col("array_temporario").getItem(2)) \
    .drop("sorteio", "combo_sorteado", "array_temporario")

# Tratamento de nulos
df_fake = df_fake \
    .withColumn("consumption focus", when(col("consumption focus") == "", None).otherwise(col("consumption focus"))) \
    .withColumn("exception cases", when(col("exception cases") == "", None).otherwise(col("exception cases")))

# 4. ESCRITA NA LANDING ZONE
df_fake.coalesce(1) \
    .write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(caminho_landing)

# 5. GRAVAÇÃO DO LOG DE PERFORMANCE
fim_processamento = time.time()
tempo_segundos = round(fim_processamento - inicio_processamento, 2)

# Usamos a variável qtd_linhas sorteada lá no topo para poupar o processamento de um .count()
spark.sql(f"""
    INSERT INTO poc_segmentation.gold.log_execucao_pipeline
    VALUES (current_timestamp(), '{meu_run_id}', '{nome_notebook}', {qtd_linhas}, {tempo_segundos}, 'SUCESSO')
""")

# 6. MENSAGENS FINAIS
print(f"Lote Diário Gerado na Landing Zone: País {pais_do_dia} | Linhas: {qtd_linhas}")
print(f"Log gravado: {qtd_linhas} linhas em {tempo_segundos} segundos. RunID: {meu_run_id}")