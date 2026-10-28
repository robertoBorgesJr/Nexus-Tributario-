# Spec Delta

## Purpose

Expõe as gold tables do Nexus Tributário via endpoints REST paginados, permitindo que ferramentas de BI, auditores fiscais e sistemas externos consultem dados fiscais sem acesso direto ao Databricks.

## ADDED Requirements

### Requirement: Endpoints de consulta às facts retornam JSON paginado

O serviço SHALL expor os endpoints `GET /api/v1/nfe-itens`, `GET /api/v1/apuracao-icms` e `GET /api/v1/apuracao-pis-cofins`. Todos SHALL aceitar os parâmetros de query `cnpj_empresa`, `competencia`, `limit` (padrão 100, máximo 1000) e `offset` (padrão 0). A resposta SHALL ser JSON com campos `data` (array) e `total` (contagem sem paginação).

#### Scenario: Consulta filtrada por CNPJ e competência

- **WHEN** `GET /api/v1/apuracao-icms?cnpj_empresa=11111111000191&competencia=202507`
- **THEN** retorna HTTP 200 com array `data` contendo somente registros da empresa e período informados

#### Scenario: Paginação funciona com limit e offset

- **WHEN** `GET /api/v1/nfe-itens?limit=10&offset=20`
- **THEN** retorna HTTP 200 com no máximo 10 registros a partir do 21º e `total` com a contagem global

#### Scenario: Parâmetro inválido retorna erro 422

- **WHEN** `GET /api/v1/nfe-itens?limit=9999`
- **THEN** retorna HTTP 422 com mensagem indicando que `limit` não pode exceder 1000

### Requirement: Endpoints de alertas filtráveis por severidade

O serviço SHALL expor `GET /api/v1/alertas/icms` e `GET /api/v1/alertas/pis-cofins`. Além de `cnpj_empresa` e `competencia`, SHALL aceitar `severidade` (valores aceitos: `BAIXO`, `MEDIO`, `ALTO`, `CRITICO`). Retorna JSON paginado com os mesmos campos `data` e `total`.

#### Scenario: Filtro por severidade ALTO e CRITICO

- **WHEN** `GET /api/v1/alertas/icms?severidade=ALTO`
- **THEN** retorna somente alertas com severidade `ALTO`; registros `BAIXO`, `MEDIO` e `CRITICO` são excluídos

#### Scenario: Severidade inválida retorna erro 422

- **WHEN** `GET /api/v1/alertas/icms?severidade=URGENTE`
- **THEN** retorna HTTP 422 listando os valores aceitos

### Requirement: API não duplica lógica de negócio fiscal

A API SHALL ler os dados diretamente das tabelas Delta da camada gold via Databricks SQL Connector. Cálculos de alíquota, divergência e severidade SHALL permanecer exclusivamente nos modelos dbt. A API não SHALL recalcular nenhum valor fiscal.

#### Scenario: Campo severidade vem direto da tabela gold

- **WHEN** `GET /api/v1/alertas/icms` retorna um registro
- **THEN** o campo `severidade` reflete exatamente o valor armazenado em `alert_conciliacao_icms.severidade` sem transformação adicional
