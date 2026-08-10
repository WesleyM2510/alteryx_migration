# Databricks notebook source
# MAGIC %md
# MAGIC # Logs CPF Intergrall VF1 — Bronze Layer
# MAGIC
# MAGIC **Migrated from:** Alteryx workflow `Logs CPF Intergrall VF1.yxmd`
# MAGIC **Migration date:** 2026-08-06
# MAGIC **Medallion tier:** Bronze (raw ingestion)
# MAGIC **Source data:**
# MAGIC * `bacen.dbo.cat099_log_itgl` via `odbc:DSN=SYBASE` (Sybase ASE, user `U_ITMONITOR`)
# MAGIC * `Base_Funcionarios_Atualizado.xlsx`, abas `Ativos$` e `DesligadosConsolidado$`
# MAGIC   (share SMB `\\swap629\Auditoria_Continua$`)
# MAGIC
# MAGIC **Target tables:** `{bronze}.cat099_log_itgl`, `{bronze}.base_funcionarios`
# MAGIC
# MAGIC ## Alteryx Tool Mapping
# MAGIC | Step | Alteryx Tool | ToolID | PySpark Operation |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Input Data (ODBC Sybase) | 9 | `read_landing()` → Delta bronze |
# MAGIC | 2 | Input Data (Excel `Ativos$`) | 13 | `read_landing(sheet="Ativos")` |
# MAGIC | 3 | Input Data (Excel `DesligadosConsolidado$`) | 14 | `read_landing(sheet="DesligadosConsolidado")` |
# MAGIC | 4 | Union (ByPos) | 15 | `unionByName` sobre nomes posicionais |
# MAGIC
# MAGIC ## Regras desta camada
# MAGIC Nomes e tipos originais preservados; nenhuma lógica de negócio. O único desvio
# MAGIC é o Union (Tool 15), trazido para Bronze porque as duas abas são o *mesmo*
# MAGIC artefato de origem — a coluna `_source_sheet` mantém a procedência.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

BATCH_ID = f"{HOJE.isoformat()}T{__import__('datetime').datetime.now():%H%M%S}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Log de consultas Intergrall (Tool 9)
# MAGIC
# MAGIC No Alteryx a query era:
# MAGIC
# MAGIC ```sql
# MAGIC select * from [bacen].[dbo].[cat099_log_itgl]
# MAGIC where Month(Convert([DATE], log_itgl_dat, 103)) = Month(Convert([DATE], GetDate(), 103))
# MAGIC   and Year (Convert([DATE], log_itgl_dat, 103)) = Year (Convert([DATE], GetDate(), 103))
# MAGIC ```
# MAGIC
# MAGIC O `WHERE` de mês/ano é **lógica de recorte**, não de ingestão — foi movido para
# MAGIC Silver. Bronze recebe o que a ingestão Sybase → Volume entregar.
# MAGIC
# MAGIC A ingestão Sybase ASE → Volume é um job separado: ASE não é fonte suportada
# MAGIC pelo Lakeflow Connect, então exige JDBC (jTDS/jconn) com rota de rede até o
# MAGIC servidor. Ver `README.md`.

# COMMAND ----------

df_log_bronze = (
    read_landing(LOG_INPUT_PATH)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(LOG_INPUT_PATH))
    .withColumn("_batch_id", F.lit(BATCH_ID))
)

(
    df_log_bronze.write.mode("append")
    .option("delta.columnMapping.mode", "name")
    .option("delta.minReaderVersion", "2")
    .option("delta.minWriterVersion", "5")
    .saveAsTable(T_BRONZE_LOG)
)

spark.sql(
    f"COMMENT ON TABLE {T_BRONZE_LOG} IS "
    "'Bronze — log bruto de consultas de CPF no Intergrall. Origem: Sybase ASE bacen.dbo.cat099_log_itgl via DSN=SYBASE. Migrado de Logs CPF Intergrall VF1.yxmd (Tool 9).'"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Base de RH — duas abas + Union ByPos (Tools 13, 14, 15)
# MAGIC
# MAGIC O Union original era **`ByPos`**: alinha por posição e ignora os nomes. As duas
# MAGIC abas têm as mesmas 40 colunas na mesma ordem, então funciona — mas uma coluna
# MAGIC inserida numa aba e não na outra desalinha tudo em silêncio. A asserção abaixo
# MAGIC converte esse risco latente em erro explícito.
# MAGIC
# MAGIC Divergência real de tipo entre as abas: `DATA DE RESCISÃO` é `Date` em
# MAGIC `Ativos$` e texto em `DesligadosConsolidado$`. Como tudo é lido com
# MAGIC `dtype=str`, o Union não quebra; a conversão fica em Silver.

# COMMAND ----------

rh_ativos = read_landing(RH_INPUT_PATH, sheet="Ativos")
rh_deslig = read_landing(RH_INPUT_PATH, sheet="DesligadosConsolidado")

assert len(rh_ativos.columns) == len(rh_deslig.columns), (
    "Union ByPos exige o mesmo número de colunas nas duas abas: "
    f"Ativos={len(rh_ativos.columns)}, DesligadosConsolidado={len(rh_deslig.columns)}. "
    "Verifique a Base_Funcionarios_Atualizado.xlsx."
)

divergentes = [(a, d) for a, d in zip(rh_ativos.columns, rh_deslig.columns) if a != d]
if divergentes:
    print(f"AVISO — nomes divergentes na mesma posição (union ByPos): {divergentes}")

df_rh_bronze = (
    rh_ativos.withColumn("_source_sheet", F.lit("Ativos"))
    .unionByName(
        rh_deslig.toDF(*rh_ativos.columns).withColumn(
            "_source_sheet", F.lit("DesligadosConsolidado")
        )
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(RH_INPUT_PATH))
    .withColumn("_batch_id", F.lit(BATCH_ID))
)

(
    df_rh_bronze.write.mode("overwrite")  # snapshot completo: a base de RH é substituída
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .option("delta.minReaderVersion", "2")
    .option("delta.minWriterVersion", "5")
    .saveAsTable(T_BRONZE_RH)
)

spark.sql(
    f"COMMENT ON TABLE {T_BRONZE_RH} IS "
    "'Bronze — base de RH (snapshot). Union ByPos das abas Ativos$ e DesligadosConsolidado$ de Base_Funcionarios_Atualizado.xlsx. Migrado de Logs CPF Intergrall VF1.yxmd (Tools 13/14/15).'"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validação de Bronze (skill, Fase 2, passo 7)
# MAGIC
# MAGIC Contagens conferidas contra a origem. Estatísticas em um único `.select()`
# MAGIC por tabela — cada `.collect()` dispara um job (item 9 de performance).

# COMMAND ----------

log_stats = spark.table(T_BRONZE_LOG).filter(F.col("_batch_id") == BATCH_ID).select(
    F.count("*").alias("linhas"),
    F.countDistinct("log_itgl_seq").alias("seq_distintos"),
    F.min("log_itgl_dat").alias("dat_min"),
    F.max("log_itgl_dat").alias("dat_max"),
).collect()[0]

rh_stats = spark.table(T_BRONZE_RH).select(
    F.count("*").alias("linhas"),
    F.countDistinct("NOME FUNCIONÁRIO").alias("nomes_distintos"),
    F.sum(F.when(F.col("_source_sheet") == "Ativos", 1).otherwise(0)).alias("ativos"),
    F.sum(F.when(F.col("_source_sheet") != "Ativos", 1).otherwise(0)).alias("desligados"),
    F.sum(F.when(F.col("CPF").isNull(), 1).otherwise(0)).alias("cpf_nulos"),
).collect()[0]

print(f"batch {BATCH_ID}")
print(f"{T_BRONZE_LOG}: {log_stats.linhas:,} linhas | "
      f"{log_stats.seq_distintos:,} seq distintos | datas {log_stats.dat_min}..{log_stats.dat_max}")
print(f"{T_BRONZE_RH}: {rh_stats.linhas:,} linhas "
      f"({rh_stats.ativos:,} ativos + {rh_stats.desligados:,} desligados) | "
      f"{rh_stats.nomes_distintos:,} nomes distintos | {rh_stats.cpf_nulos:,} CPF nulos")

assert log_stats.linhas > 0, "log Intergrall vazio — verifique a ingestão Sybase"
assert rh_stats.linhas > 0, "base de RH vazia — verifique o caminho do .xlsx"
assert rh_stats.ativos > 0 and rh_stats.desligados > 0, (
    "Union ByPos trouxe apenas uma das abas — verifique os nomes das planilhas"
)

# COMMAND ----------

dbutils.notebook.exit(
    f"BRONZE OK | log={log_stats.linhas} | rh={rh_stats.linhas} | batch={BATCH_ID}"
)
