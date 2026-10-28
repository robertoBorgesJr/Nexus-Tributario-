# Spec Delta

## Purpose

Detecta e classifica divergências entre o PIS/COFINS das NF-e emitidas e o declarado na EFD Contribuições por empresa e competência, espelhando a cobertura de alertas já existente para ICMS.

## ADDED Requirements

### Requirement: Modelo alert_conciliacao_pis_cofins filtra divergências relevantes

O modelo `alert_conciliacao_pis_cofins` SHALL ser materializado como `table` na camada gold/alerts, consumindo `fact_apuracao_pis_cofins`. Somente registros onde `abs(divergencia_pis) > 50` OR `abs(divergencia_cofins) > 50` OR `abs(divergencia_pis_pct) > 0.5` OR `abs(divergencia_cofins_pct) > 0.5` SHALL ser incluídos.

#### Scenario: Registros abaixo dos thresholds não aparecem no alerta

- **WHEN** uma empresa tem `divergencia_pis = 10` e `divergencia_cofins = 5` em uma competência
- **THEN** esse registro NÃO aparece em `alert_conciliacao_pis_cofins`

#### Scenario: Divergência relevante em PIS gera alerta

- **WHEN** `abs(divergencia_pis) = 300` mesmo que `divergencia_cofins = 0`
- **THEN** o registro aparece em `alert_conciliacao_pis_cofins`

### Requirement: Severidade calculada separadamente para PIS e COFINS

O modelo SHALL calcular `severidade_pis` e `severidade_cofins` independentemente com os thresholds: `CRITICO` se `abs > 1000` ou `abs_pct > 5`; `ALTO` se `abs > 200` ou `abs_pct > 2`; `MEDIO` se `abs > 50` ou `abs_pct > 0.5`; caso contrário `BAIXO`. A `severidade_geral` SHALL ser o nível máximo entre `severidade_pis` e `severidade_cofins`.

#### Scenario: Severidade geral reflete o pior tributo

- **WHEN** `severidade_pis = 'MEDIO'` e `severidade_cofins = 'CRITICO'`
- **THEN** `severidade_geral = 'CRITICO'`

#### Scenario: Ambos BAIXO resulta em severidade geral BAIXO

- **WHEN** `severidade_pis = 'BAIXO'` e `severidade_cofins = 'BAIXO'`
- **THEN** `severidade_geral = 'BAIXO'`

### Requirement: Tipo de divergência PIS e COFINS classificados individualmente

O modelo SHALL incluir `tipo_divergencia_pis` e `tipo_divergencia_cofins`, cada um com valores `NF_E_MAIOR_QUE_EFD`, `EFD_MAIOR_QUE_NF_E` ou `OK`, baseados no sinal de `divergencia_pis` e `divergencia_cofins` respectivamente.

#### Scenario: NF-e maior que EFD em PIS

- **WHEN** `divergencia_pis > 0` (NF-e emitiu mais PIS do que o declarado na EFD)
- **THEN** `tipo_divergencia_pis = 'NF_E_MAIOR_QUE_EFD'`

#### Scenario: EFD maior que NF-e em COFINS

- **WHEN** `divergencia_cofins < 0` (EFD declarou mais COFINS do que as NF-e)
- **THEN** `tipo_divergencia_cofins = 'EFD_MAIOR_QUE_NF_E'`
