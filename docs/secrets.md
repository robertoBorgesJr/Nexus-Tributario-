# Secrets GitHub Actions — Nexus Tributário

Os workflows `dbt_ci.yml` e `dbt_cd.yml` requerem os seguintes secrets configurados em:
**GitHub → Settings → Secrets and variables → Actions → New repository secret**

## Secrets necessários

| Secret | Descrição | Como obter |
|---|---|---|
| `DATABRICKS_HOST` | Hostname do workspace Databricks (sem `https://`) | Databricks workspace → Settings → Workspace settings → URL (ex: `adb-7405612202710045.5.azuredatabricks.net`) |
| `DATABRICKS_HTTP_PATH` | HTTP Path do SQL Warehouse | Databricks → SQL Warehouses → selecione o warehouse → Connection details → HTTP path (ex: `/sql/1.0/warehouses/ef38fcac5564a917`) |
| `DATABRICKS_TOKEN` | Personal Access Token para autenticação | Databricks → Settings → Developer → Access tokens → Generate new token |
| `DATABRICKS_CATALOG` | Nome do Unity Catalog de produção | Databricks → Data → Catalogs (ex: `nexus_tributario_prod`) |

## Rotação de tokens

Os tokens Databricks expiram conforme a política do workspace (padrão: 90 dias).
Ao renovar o token, atualize o secret `DATABRICKS_TOKEN` antes da expiração para evitar falha nos workflows.

## Verificação

Após configurar todos os secrets, abra um PR com uma alteração mínima em `dbt/` para acionar o CI.
O job `Validar modelos dbt` deve passar com `dbt compile` e `dbt build` bem-sucedidos.
