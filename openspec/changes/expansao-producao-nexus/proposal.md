# Proposal

## Why

O MVP do Nexus Tributário conclui com sucesso o ciclo completo de geração, ingestão e transformação dos dados fiscais (NF-e, SPED Fiscal, EFD Contribuições) na camada gold. No entanto, quatro lacunas impedem a transição para produção: as fact tables reconstruídas integralmente a cada execução tornarão o pipeline inviável à medida que o volume crescer; os workflows de CI/CD estão escritos mas nunca foram comitados, bloqueando deploys automatizados; não existe camada de acesso para consumidores externos (ferramentas de BI, auditores fiscais) sem acesso direto ao Databricks; e divergências de PIS/COFINS calculadas em `fact_apuracao_pis_cofins` não geram alertas, diferentemente do ICMS.

## What Changes

- **Materialização incremental (gold):** as 4 fact tables (`fact_nfe_itens`, `fact_apuracao_icms`, `fact_apuracao_pis_cofins`, `fact_conciliacao_produto`) passam de `table` (rebuild completo) para `incremental` com estratégia `merge`, usando a surrogate key existente como `unique_key`.
- **Ativação do CI/CD:** o diretório `.github/` (atualmente não rastreado pelo git) é comitado; os secrets necessários (`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, `DATABRICKS_CATALOG`) são documentados; o CD passa a salvar `manifest.json` como artifact para habilitar Slim CI nos PRs seguintes.
- **API REST fiscal:** novo serviço FastAPI que expõe as gold tables em endpoints RESTpaginados, com filtragem por `cnpj_empresa`, `competencia` e `severidade`. Não duplica lógica de negócio — lê diretamente as tabelas Delta via Databricks SQL Connector.
- **Alerta PIS/COFINS (gold):** novo modelo `alert_conciliacao_pis_cofins` espelhando a estrutura de `alert_conciliacao_icms`, consumindo `fact_apuracao_pis_cofins` e aplicando thresholds de severidade para `divergencia_pis` e `divergencia_cofins`.

## Capabilities

### New Capabilities

- `incrementalizacao-facts`: As fact tables da camada gold passam a suportar cargas incrementais via merge, evitando rebuild completo a cada execução do pipeline.
- `pipeline-cicd`: Ativação e documentação dos workflows GitHub Actions de CI (Slim CI com `state:modified+`) e CD (deploy em produção com upload do `manifest.json`).
- `api-rest-fiscal`: Serviço FastAPI para consulta às gold tables (`fact_nfe_itens`, `fact_apuracao_icms`, `fact_apuracao_pis_cofins`, `alert_conciliacao_icms`, `alert_conciliacao_pis_cofins`) via endpoints REST.
- `alerta-conciliacao-pis-cofins`: Modelo dbt na camada gold/alerts que detecta e classifica divergências entre PIS/COFINS das NF-e emitidas e o declarado na EFD Contribuições.

### Modified Capabilities

_(nenhuma — todas as capacidades existentes permanecem com os mesmos requisitos de comportamento)_

## Impact

- **Camada gold (dbt):** 4 fact tables modificadas (materialização); 1 modelo novo (`alert_conciliacao_pis_cofins`); `dbt_project.yml` atualizado para registrar a estratégia incremental no grupo `facts`.
- **Repositório git:** `.github/workflows/dbt_ci.yml` e `dbt_cd.yml` passam a ser rastreados; novo arquivo `docs/secrets.md` documenta os secrets requeridos.
- **Nova dependência:** FastAPI + Databricks SQL Connector Python (`databricks-sql-connector`) adicionados ao `requirements.txt`.
- **Novo diretório:** `api/` na raiz do projeto, contendo o serviço FastAPI.
- **Fora do escopo:** SCD Type 2 nas dimensões, site do dbt docs, configuração de ferramenta de BI, monitoramento/observabilidade, integração com certificado digital real.
