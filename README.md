# alteryx_migration

Migração de workflows Alteryx para Databricks (PySpark + Lakeflow Jobs), seguindo
arquitetura medallion com validação de saída.

## Conteúdo

| Caminho | O que é |
|---|---|
| `migrated/SKILL.md` | Skill de migração Alteryx → PySpark: checklist, mapeamento de ferramentas, framework de validação |
| `migrated/logs_cpf_intergrall_vf1/` | Fluxo de referência migrado — 5 notebooks + Databricks Asset Bundle |

## Fluxo de referência: Logs CPF Intergrall VF1

Indicador de auditoria contínua sobre consultas repetidas de CPF no sistema
Intergrall. Ver [`migrated/logs_cpf_intergrall_vf1/README.md`](migrated/logs_cpf_intergrall_vf1/README.md)
para o mapeamento completo ferramenta-a-ferramenta, o resultado das validações e os
pontos abertos.

```
bronze.cat099_log_itgl   bronze.base_funcionarios
            │                      │
            ▼                      ▼
   silver.log_itgl_consultas   silver.rh_funcionarios
            └──────────┬───────────┘
                       ▼  LEFT ANTI JOIN
            gold.logs_cpf_intergrall
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
   .xlsx datado no Volume     SQL Alert (e-mail
                               com a contagem)
```

Deploy:

```bash
cd migrated/logs_cpf_intergrall_vf1
databricks bundle validate -t dev
databricks bundle deploy   -t dev
```

## O que NÃO está neste repositório

Os arquivos `.yxmd` originais **não são versionados** (ver `.gitignore`). Eles contêm
credenciais que o Alteryx "encripta" de forma reversível na máquina de origem:

* senha ODBC do Sybase ASE (usuário `U_ITMONITOR`, `DSN=SYBASE`)
* API key do SendGrid (`smtp.sendgrid.net`, usuário `apikey`)
* topologia interna (compartilhamentos `\\swap629`, tabelas `bacen.dbo.*`)

Os workflows de origem devem circular apenas pelo canal de transferência autorizado
do banco. **O código migrado neste repositório não contém nenhuma credencial** — as
configurações vêm de widgets do bundle, e o e-mail é enviado pelo próprio Databricks
(notificações do Job e SQL Alert), sem SMTP.

## Pendências

* Validação de paridade: sem baseline de saída do Alteryx, a validação é por
  profiling + invariantes. Ver o README do fluxo.
* Ingestão Sybase ASE → Volume: não é fonte suportada pelo Lakeflow Connect; requer
  JDBC (jTDS/jconn) com rota de rede até o servidor.
* Rotação de credenciais: a API key do SendGrid aparece em 68 instâncias de Email
  tool no estate. Candidata a rotação, independentemente da migração.
