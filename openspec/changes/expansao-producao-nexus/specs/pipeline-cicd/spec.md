# Spec Delta

## Purpose

Automatiza a validação e o deploy dos modelos dbt via GitHub Actions, garantindo que apenas código validado chegue à produção e que o Slim CI possa comparar cada PR com o estado real de produção.

## ADDED Requirements

### Requirement: CI valida modelos afetados em todo Pull Request para main

O workflow de CI SHALL ser disparado em Pull Requests com destino à branch `main` quando arquivos em `dbt/models/`, `dbt/macros/`, `dbt/tests/` ou `dbt/dbt_project.yml` forem alterados. O job SHALL executar `dbt compile` seguido de `dbt build` restrito a `state:modified+` quando um `manifest.json` de produção estiver disponível.

#### Scenario: Slim CI com manifest de produção disponível

- **WHEN** o PR altera ao menos um arquivo dbt e o artifact `dbt-manifest-prod` existe da última execução de CD
- **THEN** o job executa `dbt build --select state:modified+ --defer --state ./artifacts` e falha se algum model ou teste falhar

#### Scenario: Build completo na ausência de manifest

- **WHEN** o PR é o primeiro do repositório e o artifact `dbt-manifest-prod` não existe
- **THEN** o job executa `dbt build` completo sem `--select state:modified+`

#### Scenario: CI não dispara para mudanças fora de dbt

- **WHEN** o PR altera somente arquivos fora de `dbt/` (ex: `README.md`, `api/`, `generators/`)
- **THEN** o workflow de CI dbt NÃO é executado

### Requirement: CD faz deploy completo em produção após merge na main

O workflow de CD SHALL ser disparado em pushes para `main` nos mesmos path filters do CI. O job SHALL executar `dbt build --target prod` e, ao final com sucesso, SHALL salvar `dbt/target/manifest.json` como artifact GitHub Actions com nome `dbt-manifest-prod` e retenção de 90 dias.

#### Scenario: Deploy bem-sucedido salva manifest

- **WHEN** o CD executa `dbt build --target prod` sem erros
- **THEN** `manifest.json` é salvo como artifact `dbt-manifest-prod` e fica disponível para o próximo CI

#### Scenario: Falha no CD não salva manifest

- **WHEN** `dbt build --target prod` falha em ao menos um model ou teste
- **THEN** o job retorna exit code não-zero e o step de upload do manifest não é executado

### Requirement: Secrets do Databricks documentados para operadores

Os secrets necessários para execução dos workflows SHALL estar documentados em `docs/secrets.md` com nome, descrição e onde obter cada valor.

#### Scenario: Documentação lista todos os secrets requeridos

- **WHEN** um operador acessa `docs/secrets.md`
- **THEN** encontra a lista de `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` e `DATABRICKS_CATALOG` com descrição de cada um
