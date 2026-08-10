# Databricks notebook source
# MAGIC %md
# MAGIC # Logs CPF Intergrall VF1 — Silver Layer
# MAGIC
# MAGIC **Migrated from:** Alteryx workflow `Logs CPF Intergrall VF1.yxmd`
# MAGIC **Migration date:** 2026-08-06
# MAGIC **Medallion tier:** Silver (cleansed & conformed)
# MAGIC **Source tables:** `{bronze}.cat099_log_itgl`, `{bronze}.base_funcionarios`
# MAGIC **Target tables:** `{silver}.log_itgl_consultas`, `{silver}.rh_funcionarios`
# MAGIC
# MAGIC ## Alteryx Tool Mapping
# MAGIC | Step | Alteryx Tool | ToolID | PySpark Operation |
# MAGIC |---|---|---|---|
# MAGIC | 1 | DateTime (`dd/MM/yyyy`) | 24 | `F.to_date(..., 'dd/MM/yyyy')` |
# MAGIC | 2 | Formula (`Chave cons.`) | 11 | `F.concat()` |
# MAGIC | 3 | Unique → saída **Duplicates** | 12 | `row_number()` … `rn >= 2` |
# MAGIC | 4 | Formula (`/` → `-`) | 23 | `F.regexp_replace()` |
# MAGIC | 5 | Filter (`>= ontem`) | 25 | `.filter()` |
# MAGIC | 6 | Formula (`CPF_RET`, `CPF_RST`) | 16 | `.withColumn()` |
# MAGIC | 7 | Filter (`CPF_RST = "1"`) | 17 | `.filter()` |
# MAGIC
# MAGIC ## Duas correções face à documentação de análise
# MAGIC
# MAGIC O `.md` gerado pelo `bulk_migration` lê dois nós ao contrário do XML. Esta
# MAGIC migração segue o **XML**:
# MAGIC
# MAGIC | Passo | O que a doc diz | O que o XML faz |
# MAGIC |---|---|---|
# MAGIC | Unique (12) | `WHERE rn = 1` (mantém únicos) | ligado à saída **`Duplicates`** (`.yxmd:825-828`) → mantém as ocorrências **repetidas** (`rn >= 2`) |
# MAGIC | Join (18) | "matches are joined to HR" | ligado à saída **`Left`** (`.yxmd:853-856`) → **anti-join** (feito em Gold) |
# MAGIC
# MAGIC Seguir a doc produziria quase o complemento do indicador real.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Log — data convertida e chave de consulta (Tools 24, 11)
# MAGIC
# MAGIC A `Chave cons.` é montada **antes** da troca de `/` por `-` (Tool 23), então usa
# MAGIC a data com barras. A ordem importa para a chave bater com o histórico.
# MAGIC
# MAGIC O recorte de mês/ano corrente vem do `WHERE` da query Sybase (Tool 9), aplicado
# MAGIC aqui sobre a data já convertida em vez de linha a linha como no original.
# MAGIC
# MAGIC `_ordem_entrada` preserva a ordem de leitura, que é o critério do Unique do
# MAGIC Alteryx: a ferramenta é sensível à ordem das linhas e o Spark não é
# MAGIC (armadilha "Sort order changes results" do skill). `log_itgl_seq` é o
# MAGIC sequencial da origem e serve de desempate estável.

# COMMAND ----------

log_bronze = spark.table(T_BRONZE_LOG)

log_com_chave = (
    log_bronze.withColumn("data_convertida", F.to_date("log_itgl_dat", "dd/MM/yyyy"))
    .withColumn(
        "chave_cons",
        F.concat(
            F.coalesce("log_itgl_usu", F.lit("")),
            F.coalesce("log_itgl_nm", F.lit("")),
            F.coalesce("log_itgl_nm_cns", F.lit("")),
            F.coalesce("log_itgl_cpf_cns", F.lit("")),
            F.coalesce("log_itgl_dat", F.lit("")),
        ),
    )
    # recorte de mês/ano corrente (WHERE da query Sybase, Tool 9)
    .filter(
        F.date_trunc("MONTH", F.col("data_convertida"))
        == F.date_trunc("MONTH", F.current_date())
    )
    .withColumn("_ordem_entrada", F.col("log_itgl_seq").cast("bigint"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Somente as consultas REPETIDAS (Tool 12, saída `Duplicates`)
# MAGIC
# MAGIC O Alteryx manda para frente a 2ª ocorrência em diante de cada `Chave cons.` —
# MAGIC o indicador olha consultas **duplicadas** (mesmo usuário, mesmo CPF consultado,
# MAGIC mesmo dia), não a lista deduplicada. Daí `rn >= 2`.
# MAGIC
# MAGIC Note que **não** é `.dropDuplicates()`, o mapeamento usual de Unique no skill:
# MAGIC aquele guarda a 1ª ocorrência, e este fluxo descarta exatamente essa.

# COMMAND ----------

w_chave = Window.partitionBy("chave_cons").orderBy("_ordem_entrada")

log_duplicados = (
    log_com_chave.withColumn("rn", F.row_number().over(w_chave))
    .filter(F.col("rn") >= 2)
    .drop("rn")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Normalização da data e recorte do dia anterior (Tools 23, 25)
# MAGIC
# MAGIC `log_itgl_dat` passa a usar `-` em vez de `/`, e mantém-se apenas o que ocorreu
# MAGIC a partir de ontem.

# COMMAND ----------

log_silver = (
    log_duplicados.withColumn(
        "log_itgl_dat", F.regexp_replace("log_itgl_dat", "/", "-")
    )
    .filter(F.col("data_convertida") >= F.date_add(F.current_date(), -1))
    .select(
        F.col("log_itgl_seq").cast("bigint").alias("log_itgl_seq"),
        "log_itgl_usu",
        "log_itgl_nm",
        "log_itgl_nm_cns",
        "log_itgl_cpf_cns",
        "log_itgl_dat",
        "data_convertida",
        "log_itgl_hor",
        "log_itgl_obs",
        "log_itgl_arq",
        "log_itgl_dat_itg",
        "log_itgl_usu_itg",
        F.col("log_itgl_ati").cast("int").alias("log_itgl_ati"),
        "chave_cons",
        "_batch_id",
    )
)

(
    log_silver.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_SILVER_LOG)
)

spark.sql(
    f"COMMENT ON TABLE {T_SILVER_LOG} IS "
    "'Silver — consultas REPETIDAS de CPF no Intergrall (2a ocorrencia em diante da mesma Chave cons.), recortadas a partir de ontem. Migrado de Logs CPF Intergrall VF1.yxmd (Tools 24/11/12-Duplicates/23/25).'"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. RH — CPF normalizado e recorte de cargos de direção (Tools 16, 17)
# MAGIC
# MAGIC * `cpf_ret` — remove `.`, `-` e zeros à esquerda. **Nunca é usado** no fluxo
# MAGIC   original: o join é por nome. Mantido para paridade e conferência.
# MAGIC * `cpf_rst` — `'1'` quando o cargo começa com `DIR`, `MEMBRO` ou `PRES`.
# MAGIC   O filtro mantém só esses: diretores, membros de comitê e presidência.
# MAGIC
# MAGIC Colunas renomeadas para snake_case conforme as boas práticas de Silver do
# MAGIC skill — o que também elimina os espaços e acentos de `NOME FUNCIONÁRIO` /
# MAGIC `CARGO - NOME`, evitando a armadilha de column mapping no Delta.

# COMMAND ----------

rh_silver = (
    spark.table(T_BRONZE_RH)
    .select(
        F.col("NOME FUNCIONÁRIO").alias("nome_funcionario"),
        F.col("CARGO - NOME").alias("cargo_nome"),
        F.col("CPF").alias("cpf"),
        F.col("STATUS").alias("status"),
        F.col("EMPRESA").alias("empresa"),
        F.col("_source_sheet").alias("origem_aba"),
    )
    .withColumn(
        "cpf_ret",
        F.regexp_replace(
            F.regexp_replace(F.regexp_replace("cpf", r"\.", ""), "-", ""), "^0+", ""
        ),
    )
    .withColumn(
        "cpf_rst",
        F.when(
            F.upper(F.coalesce(F.col("cargo_nome"), F.lit(""))).rlike("^(DIR|MEMBRO|PRES)"),
            F.lit("1"),
        ).otherwise(F.lit("0")),
    )
    # Tool 17: [CPF_RST] = "1"
    .filter(F.col("cpf_rst") == "1")
    .drop("cpf_rst")
)

(
    rh_silver.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_SILVER_RH)
)

spark.sql(
    f"COMMENT ON TABLE {T_SILVER_RH} IS "
    "'Silver — funcionarios em cargos de direcao (DIR/MEMBRO/PRES), ativos e desligados, com CPF normalizado. Migrado de Logs CPF Intergrall VF1.yxmd (Tools 16/17).'"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data quality (skill, Fase 3, passo 10)
# MAGIC
# MAGIC Todas as estatísticas num `.select()` por tabela.

# COMMAND ----------

log_dq = spark.table(T_SILVER_LOG).select(
    F.count("*").alias("linhas"),
    F.countDistinct("chave_cons").alias("chaves_distintas"),
    F.sum(F.when(F.col("log_itgl_usu").isNull(), 1).otherwise(0)).alias("usu_nulos"),
    F.sum(F.when(F.col("data_convertida").isNull(), 1).otherwise(0)).alias("data_invalida"),
    F.min("data_convertida").alias("dt_min"),
    F.max("data_convertida").alias("dt_max"),
).collect()[0]

rh_dq = spark.table(T_SILVER_RH).select(
    F.count("*").alias("linhas"),
    F.countDistinct("nome_funcionario").alias("nomes_distintos"),
    F.sum(F.when(F.col("nome_funcionario").isNull(), 1).otherwise(0)).alias("nome_nulos"),
    F.sum(F.when(F.length("cpf_ret") != 11, 1).otherwise(0)).alias("cpf_fora_de_11"),
).collect()[0]

print(f"{T_SILVER_LOG}: {log_dq.linhas:,} linhas | {log_dq.chaves_distintas:,} chaves | "
      f"janela {log_dq.dt_min}..{log_dq.dt_max}")
print(f"  usu nulos={log_dq.usu_nulos:,}  datas inválidas={log_dq.data_invalida:,}")
print(f"{T_SILVER_RH}: {rh_dq.linhas:,} linhas | {rh_dq.nomes_distintos:,} nomes | "
      f"nome nulos={rh_dq.nome_nulos:,}  cpf_ret != 11 díg={rh_dq.cpf_fora_de_11:,}")

# datas não convertidas quebram o recorte silenciosamente — o formato mudou na origem
assert log_dq.data_invalida == 0, (
    f"{log_dq.data_invalida} registros com log_itgl_dat fora de dd/MM/yyyy. "
    "O filtro de data descartaria esses registros em silêncio — verifique a origem."
)
# a chave do join é o nome; nulo do lado do RH nunca casa e infla o anti-join
assert rh_dq.nome_nulos == 0, (
    f"{rh_dq.nome_nulos} funcionários de direção sem nome — a chave do join é o nome, "
    "então esses registros produziriam falsos positivos no anti-join."
)
if rh_dq.linhas == 0:
    print("AVISO: nenhum cargo DIR/MEMBRO/PRES no RH — o anti-join devolveria TODO o log.")

# COMMAND ----------

dbutils.notebook.exit(
    f"SILVER OK | log={log_dq.linhas} | rh_direcao={rh_dq.linhas}"
)
