{{
  config(
    materialized = 'table',
    description  = 'Alerta: divergências de PIS/COFINS entre NF-e emitidas e EFD Contribuições declarado. '
                   'Diferença absoluta > R$50 ou percentual > 0,5% em qualquer tributo gera registro.'
  )
}}

with base as (
    select
        apuracao_key,
        empresa_key,
        cnpj_empresa,
        competencia,

        vl_pis_efd,
        v_pis_nfe,
        divergencia_pis,
        round(abs(divergencia_pis) / nullif(v_pis_nfe, 0) * 100, 2)    as divergencia_pis_pct,

        vl_cofins_efd,
        v_cofins_nfe,
        divergencia_cofins,
        round(abs(divergencia_cofins) / nullif(v_cofins_nfe, 0) * 100, 2) as divergencia_cofins_pct

    from {{ ref('fact_apuracao_pis_cofins') }}
    where abs(divergencia_pis) > 50
       or abs(divergencia_cofins) > 50
       or round(abs(divergencia_pis)    / nullif(v_pis_nfe,    0) * 100, 2) > 0.5
       or round(abs(divergencia_cofins) / nullif(v_cofins_nfe, 0) * 100, 2) > 0.5
)

select
    apuracao_key,
    empresa_key,
    cnpj_empresa,
    competencia,

    vl_pis_efd,
    v_pis_nfe,
    divergencia_pis,
    divergencia_pis_pct,

    vl_cofins_efd,
    v_cofins_nfe,
    divergencia_cofins,
    divergencia_cofins_pct,

    case
        when abs(divergencia_pis) > 1000 or divergencia_pis_pct > 5   then 'CRITICO'
        when abs(divergencia_pis) > 200  or divergencia_pis_pct > 2   then 'ALTO'
        when abs(divergencia_pis) > 50   or divergencia_pis_pct > 0.5 then 'MEDIO'
        else 'BAIXO'
    end as severidade_pis,

    case
        when abs(divergencia_cofins) > 1000 or divergencia_cofins_pct > 5   then 'CRITICO'
        when abs(divergencia_cofins) > 200  or divergencia_cofins_pct > 2   then 'ALTO'
        when abs(divergencia_cofins) > 50   or divergencia_cofins_pct > 0.5 then 'MEDIO'
        else 'BAIXO'
    end as severidade_cofins,

    case
        when (abs(divergencia_pis) > 1000 or divergencia_pis_pct > 5)
          or (abs(divergencia_cofins) > 1000 or divergencia_cofins_pct > 5)   then 'CRITICO'
        when (abs(divergencia_pis) > 200  or divergencia_pis_pct > 2)
          or (abs(divergencia_cofins) > 200  or divergencia_cofins_pct > 2)   then 'ALTO'
        when (abs(divergencia_pis) > 50   or divergencia_pis_pct > 0.5)
          or (abs(divergencia_cofins) > 50   or divergencia_cofins_pct > 0.5) then 'MEDIO'
        else 'BAIXO'
    end as severidade_geral,

    case
        when divergencia_pis > 0 then 'NF_E_MAIOR_QUE_EFD'
        when divergencia_pis < 0 then 'EFD_MAIOR_QUE_NF_E'
        else 'OK'
    end as tipo_divergencia_pis,

    case
        when divergencia_cofins > 0 then 'NF_E_MAIOR_QUE_EFD'
        when divergencia_cofins < 0 then 'EFD_MAIOR_QUE_NF_E'
        else 'OK'
    end as tipo_divergencia_cofins,

    current_timestamp() as dt_alerta

from base
