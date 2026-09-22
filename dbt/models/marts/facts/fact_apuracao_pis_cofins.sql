{{
  config(
    materialized = 'table',
    description  = 'Grão: período × empresa. Apuração PIS/COFINS da EFD Contribuições.'
  )
}}

with efd as (
    select * from {{ ref('stg_efd_contrib__apuracao') }}
),

nfe as (
    select
        cnpj_emitente as cnpj_empresa,
        competencia,
        sum(v_pis)    as v_pis_nfe,
        sum(v_cofins) as v_cofins_nfe,
        sum(v_prod)   as v_receita_bruta
    from {{ ref('stg_nfe__itens') }}
    group by cnpj_emitente, competencia
),

dim_empresa as (select cnpj, empresa_key from {{ ref('dim_empresa') }})

select
    {{ dbt_utils.generate_surrogate_key(['e.cnpj_empresa', 'e.competencia']) }} as apuracao_key,

    emp.empresa_key,
    e.cnpj_empresa,
    e.competencia,

    -- PIS
    e.vl_pis_devido,
    e.vl_bc_pis_total,
    e.vl_pis_notas           as vl_pis_efd,
    n.v_pis_nfe,
    round(n.v_pis_nfe - e.vl_pis_notas, 2)  as divergencia_pis,

    -- COFINS
    e.vl_cofins_devido,
    e.vl_bc_cofins_total,
    e.vl_cofins_notas        as vl_cofins_efd,
    n.v_cofins_nfe,
    round(n.v_cofins_nfe - e.vl_cofins_notas, 2) as divergencia_cofins,

    -- Receita bruta (das NF-e)
    n.v_receita_bruta,
    round(e.vl_pis_devido / nullif(n.v_receita_bruta, 0) * 100, 2)    as carga_pis_pct,
    round(e.vl_cofins_devido / nullif(n.v_receita_bruta, 0) * 100, 2) as carga_cofins_pct,

    e.qtd_documentos,
    current_timestamp() as dt_carga

from efd e
left join nfe n
    on  n.cnpj_empresa = e.cnpj_empresa
    and n.competencia  = e.competencia
left join dim_empresa emp on emp.cnpj = e.cnpj_empresa
