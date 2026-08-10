# Logs CPF Intergrall VF1 — Migration Summary

Migração do workflow Alteryx `Todos os fluxos Alteryx/2020/Logs CPF Intergrall VF1.yxmd`
(26 nós, 25 conexões) para Databricks, seguindo a skill `alteryx-to-pyspark`.

| | |
|---|---|
| **Data da migração** | 2026-08-06 |
| **Tier final** | Gold |
| **Indicador** | Auditoria contínua sobre consultas de CPF no sistema Intergrall |
| **Dono** | Auditoria Contínua — `auditoria.continua@bancobmg.com.br` |
| **Frequência original** | Diária (recorte `>= ontem`) |

## Notebooks

| Notebook | Tier | O que faz |
|---|---|---|
| `00_config.py` | — | Widgets, nomes de tabela/caminho, `read_landing()`, `materialize()` |
| `01_bronze_ingestion.py` | Bronze | Landing → Delta, metadados de ingestão, union das abas de RH |
| `02_silver_transformation.py` | Silver | Data, chave de consulta, deteção de repetidos, recorte, normalização de CPF, cargos de direção |
| `03_gold_aggregation.py` | Gold | Anti-join, tabela Delta particionada, `.xlsx` datado no Volume |
| `04_validation.py` | — | `validate_migration()` + profiling e invariantes |

Executar em ordem. `00_config` é incluído pelos outros via `%run ./00_config`.

## Arquitetura

```
Sybase ASE  bacen.dbo.cat099_log_itgl ──┐
  (DSN=SYBASE, U_ITMONITOR)             │  job de ingestão separado
                                        ▼
                          /Volumes/…/landing/cat099_log_itgl/
\\swap629\…\Base_Funcionarios_          │
  Atualizado.xlsx (2 abas) ─────────────┤
                                        ▼
                        bronze.cat099_log_itgl   bronze.base_funcionarios
                                        │
                                        ▼
                     silver.log_itgl_consultas   silver.rh_funcionarios
                        (repetidos, >= ontem)      (cargos DIR/MEMBRO/PRES)
                                        │
                                        ▼  LEFT ANTI JOIN
                              gold.logs_cpf_intergrall
                                        │
                        ┌───────────────┴───────────────┐
                        ▼                               ▼
        Volume: Resultado/<Ano>/<Mês>/           notificação do
              dd-MM-yyyy.xlsx                     Lakeflow Job
```

## Mapeamento Alteryx → PySpark

| ToolID | Alteryx | Tier | PySpark |
|---|---|---|---|
| 9 | Input Data (ODBC Sybase) | Bronze | `read_landing()` → Delta |
| 13, 14 | Input Data (Excel, 2 abas) | Bronze | `read_landing(sheet=…)` |
| 15 | Union (ByPos) | Bronze | `unionByName` posicional + asserção |
| 24 | DateTime `dd/MM/yyyy` | Silver | `F.to_date(…, 'dd/MM/yyyy')` |
| 11 | Formula `Chave cons.` | Silver | `F.concat()` |
| 12 | Unique → **`Duplicates`** | Silver | `row_number()` … `rn >= 2` |
| 23 | Formula `/` → `-` | Silver | `F.regexp_replace()` |
| 25 | Filter `>= ontem` | Silver | `.filter()` |
| 16 | Formula `CPF_RET`, `CPF_RST` | Silver | `.withColumn()` |
| 17 | Filter `CPF_RST = "1"` | Silver | `.filter(rlike '^(DIR\|MEMBRO\|PRES)')` |
| 18 | Join → **`Left`** | Gold | `.join(how="left_anti")` + `broadcast` |
| 19, 27 | Formula `FULLPATCH`, `AssuntoEmail` | Gold | construído em `00_config` |
| 21, 31, 32, 33 | Select | Gold | `.select()` |
| 20 | BlockUntilDone | Gold | ordem sequencial das células |
| 22 | PortfolioComposerTable | Gold | `.toPandas()` → `.xlsx` |
| 28, 29, 30 | TextInput + AppendFields + Filter | Gold | `if n_resultado == 0` |
| 10, 34 | Output Data (.xlsx) | Gold | `.xlsx` no Volume + Delta |
| 1 | Email (SendGrid) | Gold | notificação do Lakeflow Job (stub) |

## Duas correções face à documentação de análise

O `.md` gerado pelo `bulk_migration` — e o prompt de Lakeflow Designer que ele contém —
lê dois nós ao contrário do XML. Esta migração segue o **XML**.

| Nó | Doc de análise | XML (`.yxmd`) |
|---|---|---|
| Unique (12) | `WHERE rn = 1`, mantém únicos | ligado a **`Duplicates`** (linhas 825-828) → `rn >= 2`, mantém **repetidos** |
| Join (18) | "matches are joined to HR" | ligado a **`Left`** (linhas 853-856) → **`LEFT ANTI JOIN`** |

Seguir a documentação produziria quase o complemento do indicador real. Vale corrigir
`files/Todos_os_fluxos_Alteryx_2020_Logs_CPF_Intergrall_VF1.md` — e verificar se o
mesmo erro de leitura de anchor afeta os outros 197 workflows do estate, já que é
provavelmente sistemático no gerador (`_engine/recibido_analysis.py`), não pontual.

## Validação — RISCO ABERTO

**Não há baseline do Alteryx.** Conforme a Regra 3 da skill, o risco fica documentado
e a validação é por profiling + invariantes.

O que foi verificado localmente (PySpark 4.2, dados sintéticos com respostas conhecidas,
cenários desenhados para exercitar cada filtro):

```
BRONZE   log=10  rh=4 (2 ativos + 2 desligados)
SILVER   log_silver=3  rh_direcao=3  cargos=[DIRETOR EXECUTIVO, MEMBRO DO COMITE, PRESIDENTE]
GOLD     expected seq [2, 3] -> got [2, 3]  OK
INVARIANTES  6/6 PASS
xlsx round-trip OK — cabeçalho 'Chave cons.' restaurado
ALL ASSERTIONS PASSED
```

O que isto **não** prova: paridade numérica com o Alteryx. Sobrevivem a este teste a
divergência de intenção (ponto 1 abaixo) e a divergência de dados na origem.

**Para fechar o risco:** rodar o workflow Alteryx original uma vez, guardar o
`dd-MM-yyyy.xlsx` num Volume e preencher o widget `baseline_path` no `04_validation`.
Os 6 checks do skill ativam-se sozinhos.

## Pontos para validar com a auditoria

1. **A direção do anti-join.** O fluxo guarda consultas repetidas de usuários que
   **não** são diretores/presidência. Plausível para um indicador de consultas fora da
   alçada executiva, mas é o inverso do que a documentação descrevia e a intenção foi
   inferida da ligação dos nós. Se o alvo é vigiar a própria direção, `left_anti` → `inner`.
2. **Join por nome completo.** `log_itgl_usu = nome_funcionario` é comparação exata de
   texto livre. Acento, abreviação, espaço duplo ou caixa diferente fazem o registro
   escapar do join — e num anti-join **escapar significa entrar no resultado**. Falsos
   positivos são o modo de falha esperado. `cpf_ret` já existe no RH e o log tem
   `log_itgl_cpf_cns`; cruzar por CPF seria mais robusto, e talvez fosse a intenção do
   `CPF_RET` que ficou sem uso no original.
3. **Duas janelas de tempo empilhadas.** O log é lido do mês corrente, mas o recorte
   final mantém só a partir de ontem. No dia 1º, o histórico do mês anterior desaparece
   e a repetição deixa de ser detectada. Com a Delta é fácil trocar por janela móvel de 30 dias.

## Pendências de infraestrutura

* **Ingestão Sybase ASE.** Não é fonte suportada pelo Lakeflow Connect. Precisa de JDBC
  (jTDS/jconn) com rota de rede até o servidor. O `.yxmd` só traz o DSN (`SYBASE`), não
  o host — o endereço tem de vir da equipe de infra ou do `odbc.ini` da máquina Alteryx.
  O estate usa também `SYBASE4099`, provavelmente o mesmo ASE noutra porta.
* **Base de RH.** Enquanto for `.xlsx`, o Bronze depende de `pd.read_excel`
  (single-threaded, no driver). Converter para Delta na origem quando possível.
* **Credenciais.** O `.yxmd` carrega senha ODBC e API key do SendGrid encriptadas
  (linhas 74, 55, 970). A encriptação do Alteryx é reversível na máquina de origem —
  trate o arquivo como material sensível e **não** o suba para repositório aberto.
  Nada disso foi trazido para os notebooks.

## Deploy — Databricks Asset Bundle

```
databricks.yml                          bundle, variáveis, targets dev/prod
resources/logs_cpf_intergrall_job.yml   job de 4 tarefas + schedule + notificações
resources/unity_catalog.yml             schemas bronze/silver/gold + volumes
```

```bash
cd bulk_migration/migrated/logs_cpf_intergrall_vf1

databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run logs_cpf_intergrall -t dev     # execução manual

databricks bundle deploy -t prod                     # schedule ativo só em prod
```

**Antes do primeiro deploy:**

1. Descomente e preencha `workspace.profile` nos dois targets do `databricks.yml`.
2. Crie o catálogo (`bmg_auditoria` / `bmg_auditoria_dev`) — o bundle cria schemas e
   volumes, mas **não** o catálogo, porque isso exige privilégio de metastore.
3. Confirme que o grupo `users` serve para `CAN_VIEW`, ou troque pelo grupo da auditoria.
4. Em prod, considere `run_as.service_principal_name` em vez do usuário que faz deploy.

**Comportamento por target**

| | dev | prod |
|---|---|---|
| `mode` | `development` | `production` |
| Nome do job | `[dev <user>] Logs CPF Intergrall…` | `[prod] Logs CPF Intergrall…` |
| Schedule | **pausado automaticamente** | ativo (`0 0 6 * * ?`, America/Sao_Paulo) |
| Notificações | para o próprio usuário | `auditoria.continua@bancobmg.com.br` |
| Catálogo | `bmg_auditoria_dev` | `bmg_auditoria` |

O job substitui o Email tool (Tool 1) e o evento `AfterError` por
`email_notifications.on_success` / `on_failure` — **sem nenhuma credencial de SMTP**.
O anexo do e-mail original não tem equivalente: a notificação leva o link da execução
e o `.xlsx` fica no Volume.

### E-mail customizado com a contagem — SQL Alert

`email_notifications` do job usa um **template fixo**: diz que a execução passou ou
falhou, e não há como injetar o número de ocorrências no corpo. Para isso existe
`resources/logs_cpf_intergrall_alert.yml` — um **SQL Alert v2**, cujo `custom_summary`
e `custom_description` aceitam template mustache:

```
Assunto: Monitoramento CPF Intergrall — 47 consulta(s) repetida(s)

O monitoramento contínuo identificou 47 consulta(s) repetida(s) de CPF no
sistema Intergrall na execução de hoje.

Detalhamento da apuração:
  registros | usuarios_distintos | cpfs_consultados | primeira | ultima
         47 |                 12 |               31 | 09-08…   | 10-08…
```

Variáveis mustache usadas: `{{QUERY_RESULT_VALUE}}` (a contagem avaliada),
`{{QUERY_RESULT_TABLE}}` (a linha inteira da query), `{{ALERT_NAME}}`,
`{{ALERT_STATE}}`, `{{ALERT_URL}}`. Databricks envia o e-mail — **nenhuma credencial
de SMTP e nenhuma API key do SendGrid**.

**Os dois mecanismos são complementares, não alternativos:**

| | O que avisa | Quando |
|---|---|---|
| `email_notifications` do job | a **execução** falhou | pipeline quebrou |
| SQL Alert | o **resultado** do indicador | há ocorrências (`registros > 0`) |

Um alert nunca dispara se o job falhou antes de gravar — por isso não substitui o
`on_failure`.

**Antes de usar o alert:**

1. Preencha `warehouse_id` no `databricks.yml` (ou troque por
   `lookup: {warehouse: "<nome>"}`). É obrigatório e não tem default útil.
2. `alert_pause_status` está `PAUSED` em dev e `UNPAUSED` em prod. Isto é explícito
   porque **`mode: development` pausa o schedule do job mas NÃO o do alert** — sem
   isso, um deploy de dev mandaria e-mail.
3. O alert roda por cron 1h depois do job (07:00 vs 06:00), **não** acoplado ao fim
   dele. Se as 4 tarefas passarem de 1h, o alert lê a partição do dia anterior e
   reporta o número errado. Para acoplar de verdade, troque por uma 5ª task no job
   com `sql_task.alert` — aí executa na ordem, sem janela de corrida.

⚠️ Alert v2 (`custom_summary`, `custom_description`) está em **Public Preview**.
Confirme a disponibilidade no workspace da BMG antes de depender disto em produção.

⚠️ `databricks bundle destroy -t dev` apaga os schemas e volumes geridos pelo bundle
**com os dados dentro**. Se em prod esses objetos já existirem e forem geridos por
outro processo, remova `resources/unity_catalog.yml` para não haver dois donos.

### Validação executada

```
SCHEMA VALID — job + alert + UC conformes ao schema do Databricks CLI v1.7.0
notebook paths            4/4 OK
job parameters ↔ widgets  8/8 exatos, sem sobras
%run ./00_config          4/4 OK
variáveis DAB             16 declaradas, 0 referências órfãs, 0 não usadas
query_text do alert       executada contra o DDL da tabela Gold:
                            partição do dia isolada, contagens conferem,
                            dia sem ocorrências devolve 0 → não dispara
```

Um erro real foi encontrado e corrigido nesta validação: `evaluation.source.aggregation:
FIRST` não existe. Os valores aceitos são `SUM|COUNT|COUNT_DISTINCT|AVG|MEDIAN|MIN|MAX|
STDDEV`; omitir o campo já equivale a "First row", que é o correto aqui porque a query
devolve uma linha só.

`databricks bundle validate` contra o workspace ainda não foi executado — o token
local está expirado (`databricks auth login`). A validação acima foi feita contra o
schema oficial exportado por `databricks bundle schema`, o que cobre estrutura e
tipos, mas **não** a existência de catálogo, grupos ou volumes no workspace.
