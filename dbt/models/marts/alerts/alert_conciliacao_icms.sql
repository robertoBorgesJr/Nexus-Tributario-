{{
  config(
    materialized = 'table',
    description  = 'Alerta: divergências de ICMS entre NF-e emitidas e SPED Fiscal declarado. '
                   'Diferença absoluta > R$50 ou percentual > 2% gera registro.'
  )
}}

select
    apuracao_key,
    empresa_key,
    cnpj_empresa,
    competencia,
    uf,

    vl_icms_sped        as icms_declarado_sped,
    v_icms_nfe          as icms_nas_notas_nfe,
    divergencia_icms,
    divergencia_pct,

    case
        when abs(divergencia_icms) > 1000 or abs(divergencia_pct) > 5  then 'CRITICO'
        when abs(divergencia_icms) > 200  or abs(divergencia_pct) > 2  then 'ALTO'
        when abs(divergencia_icms) > 50   or abs(divergencia_pct) > 0.5 then 'MEDIO'
        else 'BAIXO'
    end as severidade,

    case
        when divergencia_icms > 0 then 'NF_E_MAIOR_QUE_SPED'
        when divergencia_icms < 0 then 'SPED_MAIOR_QUE_NF_E'
        else 'OK'
    end as tipo_divergencia,

    current_timestamp() as dt_alerta

from {{ ref('fact_apuracao_icms') }}
where abs(divergencia_icms) > 50
   or abs(divergencia_pct)  > 0.5
