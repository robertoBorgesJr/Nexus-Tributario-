# Spec Delta

## Purpose

Permite que as fact tables da camada gold sejam atualizadas via merge incremental, substituindo o rebuild completo a cada execução e reduzindo custo computacional à medida que o volume de dados cresce.

## ADDED Requirements

### Requirement: Facts suportam carga incremental por merge

As 4 fact tables (`fact_nfe_itens`, `fact_apuracao_icms`, `fact_apuracao_pis_cofins`, `fact_conciliacao_produto`) SHALL usar `materialized = 'incremental'` com `incremental_strategy = 'merge'`. A `unique_key` de cada fact SHALL ser a surrogate key já existente gerada por `dbt_utils.generate_surrogate_key`.

#### Scenario: Execução incremental insere apenas registros novos

- **WHEN** o pipeline executa `dbt run` e existem registros com `competencia` não presentes na tabela Delta destino
- **THEN** somente esses registros são inseridos, sem reprocessar competências anteriores

#### Scenario: Execução incremental atualiza registros alterados

- **WHEN** um registro com `unique_key` já existente é reprocessado com valores diferentes (ex: reapuração de nota)
- **THEN** a linha existente é atualizada via merge sem duplicação

#### Scenario: Full refresh forçado reconstrói a tabela

- **WHEN** `dbt run --full-refresh` é executado
- **THEN** a tabela é completamente reconstruída e todos os registros são substituídos

### Requirement: Leitura incremental filtrada por janela de competência

Nas execuções incrementais, as facts que leem staging views SHALL incluir um filtro `WHERE competencia >= <competencia_minima>` dentro do bloco `{% if is_incremental() %}`, garantindo que a query na camada bronze/silver não varre todo o histórico a cada run.

#### Scenario: Filtro ativo em execução incremental

- **WHEN** `is_incremental()` retorna `true`
- **THEN** a query de origem aplica filtro de competência baseado no máximo já presente na tabela destino (`select max(competencia) from {{ this }}`)

#### Scenario: Filtro inativo em first-run ou full-refresh

- **WHEN** `is_incremental()` retorna `false`
- **THEN** a query de origem lê todo o histórico sem filtro de competência
