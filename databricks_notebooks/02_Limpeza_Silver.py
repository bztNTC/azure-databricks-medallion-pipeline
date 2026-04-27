# Databricks notebook source
import time
from pyspark.sql.functions import col, when, max

# 1. INÍCIO DO CRONÔMETRO E PARÂMETROS
inicio_processamento = time.time()
dbutils.widgets.text("adf_run_id", "execucao_manual")
meu_run_id = dbutils.widgets.get("adf_run_id")
nome_notebook = "02_Limpeza_Silver"

# 2. BUSCA DA MARCA D'ÁGUA
df_controle = spark.sql("SELECT ultima_data_processada FROM poc_segmentation.gold.controle_cargas WHERE nome_tabela = 'silver_dados_limpos_ml'")
ultima_data = df_controle.collect()[0][0]
print(f"Buscando dados novos na Bronze após: {ultima_data}")

# 3. LÊ APENAS O DELTA
tabela_origem = "poc_segmentation.bronze.dados_brutos"
df_bronze = spark.table(tabela_origem) \
    .filter(col("ingestion_date") > ultima_data) \
    .withColumn("date", col("date").cast("date"))

total_novos = df_bronze.count()

# 4. BIFURCAÇÃO DE PROCESSAMENTO
if total_novos == 0:
    # Grava o log mesmo se não houver dados, para a auditoria ficar completa
    fim_processamento = time.time()
    tempo_segundos = round(fim_processamento - inicio_processamento, 2)
    spark.sql(f"INSERT INTO poc_segmentation.gold.log_execucao_pipeline VALUES (current_timestamp(), '{meu_run_id}', '{nome_notebook}', 0, {tempo_segundos}, 'SUCESSO (SEM DADOS)')")
    dbutils.notebook.exit("Nenhum dado novo para processar.")

else:
    # 5. APLICA REGRAS DE NEGÓCIO E GUARDA NA MEMÓRIA
    df_classificado = df_bronze.withColumn(
        "status_linha",
        when((col("dynamic") != "OTHERS") & col("consumption_focus").isNull(), "ERRO_DQ: Foco Vazio")
        .when((col("dynamic") == "OTHERS") & col("exception_cases").isNull(), "ERRO_DQ: Excecao Vazia")
        .when((col("dynamic") == "OTHERS") & col("consumption_focus").isNotNull(), "ERRO_DQ: Foco Indevido")
        .when(col("dynamic") == "OTHERS", "EXCECAO_NEGOCIO")
        .otherwise("VALIDO_ML")
    ).cache()

    # 6. SEPARAÇÃO DOS DADOS
    df_ml = df_classificado.filter(col("status_linha") == "VALIDO_ML").drop("status_linha")
    df_excecoes = df_classificado.filter(col("status_linha") == "EXCECAO_NEGOCIO").drop("status_linha")
    df_erros = df_classificado.filter(col("status_linha").startswith("ERRO_DQ"))

    # 7. ESCRITA OTIMIZADA NAS TABELAS SILVER
    df_ml.write.format("delta").mode("append").partitionBy("date").option("mergeSchema", "true").saveAsTable("poc_segmentation.silver.dados_limpos_ml")
    df_excecoes.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("poc_segmentation.silver.dados_excecoes")
    df_erros.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("poc_segmentation.silver.quarentena_erros")

    # Limpa a memória do cluster
    df_classificado.unpersist()

    # 8. ATUALIZA A MARCA D'ÁGUA PARA O FUTURO
    nova_marca_dagua = df_bronze.select(max("ingestion_date")).collect()[0][0]
    spark.sql(f"UPDATE poc_segmentation.gold.controle_cargas SET ultima_data_processada = '{nova_marca_dagua}' WHERE nome_tabela = 'silver_dados_limpos_ml'")

    # 9. GRAVAÇÃO DO LOG DE PERFORMANCE
    fim_processamento = time.time()
    tempo_segundos = round(fim_processamento - inicio_processamento, 2)

    spark.sql(f"""
        INSERT INTO poc_segmentation.gold.log_execucao_pipeline
        VALUES (current_timestamp(), '{meu_run_id}', '{nome_notebook}', {total_novos}, {tempo_segundos}, 'SUCESSO')
    """)

    # 10. MENSAGENS FINAIS
    print(f"Processamento concluído: {total_novos} novas linhas classificadas em {tempo_segundos}s.")
    print(f"Log gravado com sucesso. RunID: {meu_run_id}")
    print(f"Marca d'água atualizada para: {nova_marca_dagua}")