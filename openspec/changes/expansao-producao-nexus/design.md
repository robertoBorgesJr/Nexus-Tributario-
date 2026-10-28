# Design

## Context

Ver `proposal.md – Why` para a motivação completa. Estado atual relevante para as decisões técnicas:

- **Facts**: todas com `materialized = 'table'` em `dbt_project.yml` (linhas 25-28). Surrogate keys já existentes: `item_key` (fact_nfe_itens), `apuracao_key` (fact_apuracao_icms / fact_apuracao_pis_cofins), `conciliacao_key` (fact_conciliacao_produto), geradas por `dbt_utils.generate_surrogate_key`.
- **CI/CD**: `.github/workflows/dbt_ci.yml` e `dbt_cd.yml` estão escritos e funcionais, mas o diretório `.github/` não está rastreado pelo git (aparece como `??` no `git status`). A lógica de Slim CI com `state:modified+` já existe; falta apenas commitar e configurar secrets.
- **Alertas**: `alert_conciliacao_icms` existe em `dbt/models/marts/alerts/`. `fact_apuracao_pis_cofins` já possui `divergencia_pis` e `divergencia_cofins`, prontos para consumo.
- **API**: nenhuma camada de acesso externo existe; consumidores precisam de acesso direto ao Databricks Unity Catalog.

## Goals / Non-Goals

**Goals:**
- Reduzir tempo e custo de execução do pipeline com cargas incrementais nas facts
- Habilitar deploys automatizados via GitHub Actions sem intervenção manual
- Oferecer acesso REST às gold tables sem replicar lógica fiscal
- Paridade de alertas: PIS/COFINS com a mesma cobertura já existente para ICMS

**Non-Goals:**
- SCD Type 2 nas dimensões (escopo separado)
- Deploy de dbt docs site
- Configuração de ferramenta de BI (Power BI, Tableau, Looker)
- Monitoramento/observabilidade do pipeline (data freshness, quality alerts)
- Autenticação/autorização na API (fora do MVP)

## Decisions

### 1. Estratégia incremental: `merge` em vez de `insert_overwrite`

**Escolha:** `incremental_strategy = 'merge'` com `unique_key = <surrogate_key>`.

**Alternativa considerada:** `insert_overwrite` por partição de `competencia`. Rejeitado porque o SPED admite reapuração retroativa — uma nota pode ter seu `cod_sit` alterado de erro para `'00'` em competências fechadas, e um append ou partition overwrite ingênuo duplicaria ou perderia esses registros.

**Justificativa:** Merge sobre surrogate key garante idempotência: reprocessar o mesmo período é seguro. O custo extra do merge sobre Delta é aceitável dado o volume (max ~30 NF-e × 3 empresas × 6 meses = 540 linhas em facts de NF-e).

### 2. Filtro de janela incremental por `max(competencia)` da tabela destino

**Escolha:** Dentro do bloco `{% if is_incremental() %}`, filtrar a CTE de staging por `competencia >= (select max(competencia) from {{ this }})`.

**Alternativa considerada:** Filtrar por `dt_carga >= (select max(dt_carga) from {{ this }})`. Rejeitado porque `dt_carga` é timestamp de processamento, não de competência fiscal — uma reapuração retroativa teria `dt_carga` recente mas `competencia` antiga, e seria ignorada.

**Justificativa:** Competência fiscal (YYYYMM) é o identificador natural de períodos de apuração no SPED/EFD. Filtrar por `max(competencia) - 1 mês` (safe window) garante que reapurações do mês anterior também sejam capturadas.

### 3. FastAPI + Databricks SQL Connector para a camada de API

**Escolha:** FastAPI (Python) lendo Delta tables via `databricks-sql-connector`.

**Alternativa considerada:** Databricks SQL Serverless endpoint direto (sem API intermediária). Rejeitado porque expõe credenciais do Databricks a todos os consumidores e não permite controle de parâmetros, paginação padronizada ou validação de inputs.

**Alternativa considerada:** Flask. Rejeitado em favor do FastAPI pela validação automática via Pydantic, documentação OpenAPI gerada automaticamente e suporte nativo a async (útil para múltiplas queries simultâneas).

**Justificativa:** FastAPI mantém a lógica fiscal no dbt (única fonte de verdade) e oferece uma interface limpa para consumidores externos com schema autodocumentado.

### 4. alert_conciliacao_pis_cofins espelha a estrutura do ICMS

**Escolha:** Novo modelo em `dbt/models/marts/alerts/alert_conciliacao_pis_cofins.sql` consumindo `fact_apuracao_pis_cofins`, com colunas `severidade_pis`, `severidade_cofins`, `severidade_geral`, `tipo_divergencia_pis`, `tipo_divergencia_cofins`.

**Justificativa:** `fact_apuracao_pis_cofins` já calcula `divergencia_pis` e `divergencia_cofins`. Separar severidade por tributo (em vez de uma severidade combinada) preserva granularidade para auditores que precisam identificar se o problema é em PIS, COFINS ou ambos.

## Risks / Trade-offs

- **Warm-up do SQL warehouse:** Queries via Databricks SQL Connector podem ter latência de 20-30s se o warehouse estiver parado. → Mitigação: documentar uso de warehouse com auto-start; considerar serverless warehouse no futuro.
- **Janela de competência insuficiente:** `max(competencia)` como lower bound perde reapurações de competências anteriores à última processada. → Mitigação: aplicar `max(competencia) - 1` (lookback de 1 mês) para cobrir o período de ajuste fiscal mais comum; documentar limitação.
- **Secrets do GitHub Actions requerem configuração manual:** Os workflows dependem de 4 secrets configurados no GitHub. Uma execução sem os secrets falha imediatamente no step `Criar profiles.yml`. → Mitigação: documentar em `docs/secrets.md`; o passo de `dbt compile` falha de forma explícita antes de qualquer acesso ao Databricks.
- **API sem autenticação no MVP:** Os endpoints são públicos para quem tiver acesso à rede. → Trade-off aceito para o MVP; a camada de auth (OAuth2/API key) é extensão futura documentada em Non-Goals.

## Migration Plan

1. **Incremental facts:** alterar `dbt_project.yml` + cada `.sql` de fact; executar `dbt run --full-refresh` uma única vez para migrar as tabelas existentes para Delta incremental; execuções subsequentes usarão merge.
2. **CI/CD:** `git add .github/` + commit; configurar os 4 secrets no GitHub repo settings; validar com um PR de teste.
3. **API:** criar `api/` na raiz; instalar dependências; testar localmente com `uvicorn api.main:app --reload`; deploy como Databricks Job ou container separado.
4. **alert_pis_cofins:** criar `.sql` + `.yml`; executar `dbt run --select alert_conciliacao_pis_cofins` isoladamente para validar antes do próximo CD.

Rollback: todos os itens são aditivos (nova materialização, novo modelo, novo serviço). A única mudança destrutiva potencial é `--full-refresh` nas facts — executar apenas após validação em ambiente dev.
