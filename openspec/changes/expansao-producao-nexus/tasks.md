# Tasks

## 1. Materialização Incremental das Facts

- [x] 1.1 Atualizar o `config()` de `fact_nfe_itens.sql` para `materialized='incremental'`, `incremental_strategy='merge'`, `unique_key='item_key'`, e adicionar bloco `{% if is_incremental() %}` filtrando por `competencia >= (select max(competencia) from {{ this }}) - 1` — verificar com `dbt compile --select fact_nfe_itens` sem erros de sintaxe

- [x] 1.2 Atualizar o `config()` de `fact_apuracao_icms.sql` para incremental com `unique_key='apuracao_key'` e filtro de competência com lookback de 1 mês — verificar com `dbt compile --select fact_apuracao_icms`

- [x] 1.3 Atualizar o `config()` de `fact_apuracao_pis_cofins.sql` para incremental com `unique_key='apuracao_key'` e filtro de competência — verificar com `dbt compile --select fact_apuracao_pis_cofins`

- [x] 1.4 Atualizar o `config()` de `fact_conciliacao_produto.sql` para incremental com `unique_key='conciliacao_key'` e filtro de competência — verificar com `dbt compile --select fact_conciliacao_produto`

- [ ] 1.5 Executar `dbt run --full-refresh --select tag:facts` (ou `path:models/marts/facts`) para migrar as tabelas existentes para Delta incremental — verificar que todos os 4 modelos materializaram sem erro

- [ ] 1.6 Executar `dbt test --select path:models/marts/facts` e confirmar que todos os testes de unique, not_null e relationships passam após o full-refresh

- [ ] 1.7 Executar `dbt run --select path:models/marts/facts` (sem `--full-refresh`) e confirmar que os modelos executam em modo incremental (log mostra "merge" em vez de "create table")

## 2. Alerta de Conciliação PIS/COFINS

- [x] 2.1 Criar `dbt/models/marts/alerts/alert_conciliacao_pis_cofins.sql` consumindo `fact_apuracao_pis_cofins` com os campos `apuracao_key`, `empresa_key`, `cnpj_empresa`, `competencia`, valores de PIS/COFINS NF-e vs EFD, `divergencia_pis`, `divergencia_cofins`, `severidade_pis`, `severidade_cofins`, `severidade_geral`, `tipo_divergencia_pis`, `tipo_divergencia_cofins`, `dt_alerta` — verificar com `dbt compile --select alert_conciliacao_pis_cofins`

- [x] 2.2 Adicionar entradas no `dbt/models/marts/alerts/schema.yml` (ou criar arquivo equivalente para o novo modelo) com testes `unique` e `not_null` na PK, `accepted_values` para `severidade_geral` (`['BAIXO','MEDIO','ALTO','CRITICO']`) e `tipo_divergencia_pis`/`tipo_divergencia_cofins` — verificar com `dbt compile`

- [ ] 2.3 Executar `dbt run --select alert_conciliacao_pis_cofins` seguido de `dbt test --select alert_conciliacao_pis_cofins` e confirmar que pelo menos um registro de alerta é gerado (dados sintéticos têm divergências intencionais) e todos os testes passam

## 3. Ativação do Pipeline CI/CD

- [x] 3.1 Criar `docs/secrets.md` com a lista dos 4 secrets (`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, `DATABRICKS_CATALOG`), descrição de cada um e onde obter o valor no workspace Databricks — verificar que o arquivo existe e está legível

- [x] 3.2 Executar `git add .github/` para rastrear os dois workflows e confirmar com `git status` que `dbt_ci.yml` e `dbt_cd.yml` aparecem como staged (não mais `??`)

- [ ] 3.3 Configurar os 4 secrets no repositório GitHub em Settings → Secrets and variables → Actions — verificar que todos aparecem listados (valores não são exibidos, mas os nomes devem aparecer)

- [ ] 3.4 Abrir um PR de teste com uma alteração mínima em `dbt/` (ex: adicionar comentário em `dbt_project.yml`) e verificar que o job `Validar modelos dbt` é disparado e passa com `dbt compile` + `dbt build` bem-sucedidos

## 4. API REST Fiscal

- [x] 4.1 Adicionar `fastapi>=0.115`, `uvicorn[standard]>=0.30`, `databricks-sql-connector>=3.4` ao `requirements.txt` e verificar instalação com `pip install -r requirements.txt` sem erros de conflito

- [x] 4.2 Criar `api/db.py` com função `get_connection()` que retorna uma conexão ao Databricks via `databricks.sql.connect()` lendo `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH` e `DATABRICKS_TOKEN` de variáveis de ambiente — verificar com um script de smoke-test que executa `SELECT 1` e retorna resultado

- [x] 4.3 Criar `api/routers/nfe.py` com `GET /api/v1/nfe-itens` aceitando `cnpj_empresa`, `competencia`, `limit` (max 1000, default 100) e `offset` (default 0); retornar `{"data": [...], "total": N}` — verificar com `curl "http://localhost:8000/api/v1/nfe-itens?limit=5"` retornando JSON válido

- [x] 4.4 Criar `api/routers/apuracao.py` com `GET /api/v1/apuracao-icms` e `GET /api/v1/apuracao-pis-cofins` com os mesmos parâmetros de paginação e filtro — verificar que ambos retornam HTTP 200 com `data` e `total`

- [x] 4.5 Criar `api/routers/alertas.py` com `GET /api/v1/alertas/icms` e `GET /api/v1/alertas/pis-cofins` adicionando parâmetro `severidade` com `accepted_values=['BAIXO','MEDIO','ALTO','CRITICO']`; retornar HTTP 422 para valor inválido — verificar com `curl "http://localhost:8000/api/v1/alertas/icms?severidade=URGENTE"` retornando 422 e com `severidade=ALTO` retornando somente alertas dessa severidade

- [x] 4.6 Criar `api/main.py` registrando todos os routers e expondo `/docs` (Swagger UI automático do FastAPI) — verificar que `uvicorn api.main:app --reload` inicia sem erros e `/docs` carrega no navegador com todos os endpoints listados

## 5. Verificação de Integração

- [ ] 5.1 Executar `dbt build` completo (staging + dimensions + facts + alerts) e confirmar que todos os modelos compilam e todos os testes passam, incluindo os novos modelos incrementais e `alert_conciliacao_pis_cofins`

- [ ] 5.2 Com a API rodando localmente, consultar `GET /api/v1/alertas/pis-cofins` e confirmar que os registros retornados batem com o resultado de `SELECT * FROM gold.alert_conciliacao_pis_cofins LIMIT 5` consultado diretamente no Databricks

## Workflow follow-up

- Após aprovação do PR, o CD faz `dbt build --target prod` e salva `manifest.json` automaticamente — verificar na aba Actions que o artifact `dbt-manifest-prod` foi gerado.
- Arquivar a change com `/opsx:archive` após a validação em produção.
