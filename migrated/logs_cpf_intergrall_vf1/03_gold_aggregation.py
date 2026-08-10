# Databricks notebook source
# MAGIC %md
# MAGIC # Logs CPF Intergrall VF1 — Gold Layer
# MAGIC
# MAGIC **Migrated from:** Alteryx workflow `Logs CPF Intergrall VF1.yxmd`
# MAGIC **Migration date:** 2026-08-06
# MAGIC **Medallion tier:** Gold (indicador de negócio)
# MAGIC **Source tables:** `{silver}.log_itgl_consultas`, `{silver}.rh_funcionarios`
# MAGIC **Target:** `{gold}.logs_cpf_intergrall` + `.xlsx` datado no Volume
# MAGIC
# MAGIC ## Alteryx Tool Mapping
# MAGIC | Step | Alteryx Tool | ToolID | PySpark Operation |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Join → saída **Left** | 18 | `.join(..., how="left_anti")` |
# MAGIC | 2 | Formula (`FULLPATCH`, `AssuntoEmail`) | 19, 27 | nomes montados em `00_config` |
# MAGIC | 3 | Select | 21, 31, 32, 33 | `.select()` |
# MAGIC | 4 | BlockUntilDone | 20 | ordem sequencial das células |
# MAGIC | 5 | PortfolioComposerTable | 22 | `.toPandas()` → `.xlsx` |
# MAGIC | 6 | TextInput (`"Sem registro"`) | 28 | branch de resultado vazio |
# MAGIC | 7 | AppendFields | 29 | dispensado (ver nota) |
# MAGIC | 8 | Filter (`IsEmpty`) | 30 | `if n_resultado == 0` |
# MAGIC | 9 | Output Data (.xlsx) | 10, 34 | `.xlsx` no Volume + tabela Delta |
# MAGIC | 10 | Email | 1 | notificação do Lakeflow Job (stub) |
# MAGIC
# MAGIC ## Por que é Gold e não Silver
# MAGIC A saída é row-level, não agregada — mas é o **indicador de auditoria** que
# MAGIC alimentava o e-mail e o relatório da Auditoria Contínua, ou seja, o artefato
# MAGIC de consumo do negócio. O `LEFT ANTI JOIN` que a produz é o cálculo do
# MAGIC indicador, não uma limpeza.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Anti-join contra o RH de direção (Tool 18, saída `Left`)
# MAGIC
# MAGIC O Tool 18 está ligado pela saída **`Left`** (`.yxmd:853-856`) — os registros do
# MAGIC log que **não** casaram. Em PySpark: `how="left_anti"`.
# MAGIC
# MAGIC O lado direito é pequeno (só cargos de direção), então `broadcast` evita o
# MAGIC shuffle do lado grande (item 1 de performance do skill).
# MAGIC
# MAGIC A chave é `log_itgl_usu = nome_funcionario`, comparação exata de texto livre.
# MAGIC Ver a nota de revisão no fim do notebook.

# COMMAND ----------

log_silver = spark.table(T_SILVER_LOG)
rh_direcao = spark.table(T_SILVER_RH)

resultado = log_silver.join(
    F.broadcast(rh_direcao),
    log_silver["log_itgl_usu"] == rh_direcao["nome_funcionario"],
    how="left_anti",
).select(
    "log_itgl_seq",
    "log_itgl_usu",
    "log_itgl_nm",
    "log_itgl_nm_cns",
    "log_itgl_cpf_cns",
    "log_itgl_dat",
    "log_itgl_hor",
    "log_itgl_obs",
    "log_itgl_arq",
    "log_itgl_dat_itg",
    "log_itgl_usu_itg",
    "log_itgl_ati",
    "chave_cons",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Materializar (skill, checklist de otimização, itens 1-2)
# MAGIC
# MAGIC `resultado` é consumido por quatro ações a seguir: contagem, escrita na Delta,
# MAGIC `toPandas()` para o `.xlsx` e o profiling do notebook 04. Sem materializar, a
# MAGIC linhagem inteira — incluindo o anti-join — recomputa a cada uma.
# MAGIC
# MAGIC `materialize()` escolhe `.cache()` ou tabela Delta temporária conforme o
# MAGIC compute (Regra 8: `.cache()` falha em serverless).

# COMMAND ----------

resultado = materialize(resultado, "gold_resultado")
n_resultado = resultado.count()
print(f"registros no indicador: {n_resultado:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tabela Delta (Gold)
# MAGIC
# MAGIC Novo face ao Alteryx, que só produzia arquivos: dá histórico, lineage e permite
# MAGIC consultar o indicador em SQL. Particionada por `data_execucao`, com
# MAGIC `replaceWhere` para idempotência — reexecutar no mesmo dia substitui a partição
# MAGIC em vez de duplicar.
# MAGIC
# MAGIC `Chave cons.` virou `chave_cons`: nome com espaço e ponto quebra referência em
# MAGIC SQL. O cabeçalho original é restaurado no `.xlsx`.

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {T_GOLD} (
      data_execucao    DATE      COMMENT 'Data da execução do indicador',
      log_itgl_seq     BIGINT    COMMENT 'Sequencial do log Intergrall (origem Sybase)',
      log_itgl_usu     STRING    COMMENT 'Usuário que efetuou a consulta — chave do anti-join contra o RH',
      log_itgl_nm      STRING    COMMENT 'Nome do usuário',
      log_itgl_nm_cns  STRING    COMMENT 'Nome do cliente consultado',
      log_itgl_cpf_cns STRING    COMMENT 'CPF consultado',
      log_itgl_dat     STRING    COMMENT 'Data da consulta (dd-MM-yyyy, / trocada por -)',
      log_itgl_hor     STRING    COMMENT 'Hora da consulta',
      log_itgl_obs     STRING,
      log_itgl_arq     STRING,
      log_itgl_dat_itg TIMESTAMP,
      log_itgl_usu_itg STRING,
      log_itgl_ati     INT,
      chave_cons       STRING    COMMENT 'usu + nm + nm_cns + cpf_cns + dat — chave de deteção de repetição'
    )
    USING DELTA
    PARTITIONED BY (data_execucao)
    CLUSTER BY (log_itgl_usu, log_itgl_cpf_cns)
    COMMENT 'Gold — indicador de auditoria contínua: consultas REPETIDAS de CPF no Intergrall feitas por usuários FORA dos cargos de direção. Migrado de Logs CPF Intergrall VF1.yxmd.'
    """
)

(
    resultado.selectExpr(f"DATE('{HOJE.isoformat()}') AS data_execucao", "*")
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"data_execucao = '{HOJE.isoformat()}'")
    .saveAsTable(T_GOLD)
)

print(f"gravado em {T_GOLD} (partição {HOJE.isoformat()}): {n_resultado:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `.xlsx` no Volume de destino (Tools 10, 22, 28, 34)
# MAGIC
# MAGIC Os Outputs 10 e 34 gravavam no **mesmo** caminho `FULLPATCH` e eram mutuamente
# MAGIC exclusivos: o 34 só disparava com resultado vazio (o `Filter IsEmpty` do Tool 30
# MAGIC sobre o Append do texto `"Sem registro"`). Aqui é um `if` explícito, o que
# MAGIC dispensa o `crossJoin` do AppendFields (Tool 29) — ele só existia para levar o
# MAGIC literal `"Sem registro"` adiante num fluxo sem condicionais.
# MAGIC
# MAGIC O `.xlsx` exige `toPandas()`, então o resultado passa pelo driver — daí o teto
# MAGIC de segurança. O indicador é diário e filtrado, então o volume é pequeno; se o
# MAGIC limite estourar, o dado íntegro continua na tabela Delta.

# COMMAND ----------

import pandas as pd

MAX_LINHAS_XLSX = 500_000
CABECALHO_XLSX = {"chave_cons": "Chave cons."}

os.makedirs(OUTPUT_DIR, exist_ok=True)

if n_resultado == 0:
    # Tools 28/29/30/34: arquivo-marcador de "nada a reportar"
    pdf = pd.DataFrame({"LOG": ["Sem registro"]})
    print("resultado vazio — gravando arquivo 'Sem registro'")
else:
    if n_resultado > MAX_LINHAS_XLSX:
        raise ValueError(
            f"{n_resultado:,} registros excedem o limite de {MAX_LINHAS_XLSX:,} para o "
            f".xlsx. Os dados estão íntegros em {T_GOLD}; investigue o volume antes de "
            "gerar o arquivo."
        )
    pdf = resultado.toPandas().rename(columns=CABECALHO_XLSX)

with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    pdf.to_excel(writer, sheet_name=SHEET_NAME, index=False)

# len(pdf) em vez de .count(): o pandas já está em memória (item 10 de performance)
print(f"gravado {OUTPUT_XLSX} ({len(pdf):,} linhas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Notificação — a configurar no Job (Tool 1 + evento AfterError)
# MAGIC
# MAGIC O Alteryx enviava dois e-mails via `smtp.sendgrid.net` com a API key do SendGrid
# MAGIC **encriptada dentro do `.yxmd`** (linhas 55 e 970):
# MAGIC
# MAGIC 1. Tool 1 — resultado, assunto `AssuntoEmail`, anexo `FULLPATCH2`.
# MAGIC 2. Evento `AfterError` — `ERRO NA EXECUÇÃO: LOGS INTERGRALL`.
# MAGIC
# MAGIC Nenhuma credencial foi trazida. Configure no Lakeflow Job:
# MAGIC
# MAGIC * `email_notifications.on_success` → `auditoria.continua@bancobmg.com.br`
# MAGIC * `email_notifications.on_failure` → idem (substitui o `AfterError`)
# MAGIC
# MAGIC O anexo não tem equivalente direto: a notificação do Job leva o link da
# MAGIC execução. O arquivo fica no Volume, e o indicador pode ser lido pela tabela
# MAGIC Delta ou por um dashboard AI/BI sobre ela — o que remove a necessidade do anexo.

# COMMAND ----------

print(f"Assunto original : {ASSUNTO_EMAIL}")
print(f"Anexo original   : {OUTPUT_XLSX}")
print(f"Destinatário     : {EMAIL_TO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cleanup (skill, checklist de otimização, item 5)
# MAGIC
# MAGIC Comentado de propósito: o notebook 04 lê o resultado materializado para o
# MAGIC profiling. Descomente ao rodar 03 isoladamente, ou deixe 04 fazer a limpeza
# MAGIC quando os dois correrem no mesmo job.

# COMMAND ----------

# cleanup_temp_tables()

dbutils.notebook.exit(
    f"GOLD OK | {n_resultado} registros | {T_GOLD} | {OUTPUT_XLSX}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pontos para validar com a auditoria antes de promover
# MAGIC
# MAGIC 1. **A direção do anti-join.** O fluxo guarda consultas repetidas feitas por
# MAGIC    usuários que **não** são diretores/presidência. Para um indicador de
# MAGIC    "Monitoramento CPF Intergrall" é plausível (consultas fora da alçada
# MAGIC    executiva), mas é o inverso do que a documentação de análise descrevia, e a
# MAGIC    intenção foi inferida da ligação dos nós. Se o alvo é vigiar a própria
# MAGIC    direção, `how="left_anti"` deve virar `how="inner"`.
# MAGIC 2. **Join por nome completo.** `log_itgl_usu = nome_funcionario` é comparação
# MAGIC    exata de texto livre: acento, abreviação, espaço duplo ou caixa diferente já
# MAGIC    fazem o registro escapar do join — e, num anti-join, **escapar significa
# MAGIC    entrar no resultado**. Falsos positivos são o modo de falha esperado.
# MAGIC    `cpf_ret` já existe no RH e o log tem `log_itgl_cpf_cns`; cruzar por CPF
# MAGIC    seria bem mais robusto, e talvez fosse a intenção do `CPF_RET` que ficou sem
# MAGIC    uso no original.
# MAGIC 3. **Duas janelas de tempo empilhadas.** O log é lido do mês corrente, mas o
# MAGIC    recorte final mantém só a partir de ontem. A `Chave cons.` é avaliada sobre o
# MAGIC    mês todo, então uma consulta de ontem é marcada como repetida se houve outra
# MAGIC    igual em qualquer dia do mês — mas, no dia 1º, o histórico do mês anterior
# MAGIC    desaparece e a repetição deixa de ser detectada. Com a tabela Delta é fácil
# MAGIC    trocar por uma janela móvel de 30 dias, se a borda não for intencional.
