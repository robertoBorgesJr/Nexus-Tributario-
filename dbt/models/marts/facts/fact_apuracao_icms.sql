{{
  config(
    materialized = 'table',
    description  = 'Grão: período × empresa × UF. Apuração ICMS do SPED Fiscal com totais das NF-e para conciliação.'
  )
}}

with sped as (
    select * from {{ ref('stg_sped_fiscal__apuracao') }}
),

nfe as (
    select
        cnpj_emitente  as cnpj_empresa,
        uf_emitente    as uf,
        competencia,
        sum(v_bc_icms) as v_bc_icms_nfe,
        nullif(sum(v_icms), 0) as v_icms_nfe,
        count(distinct chv_nfe) as qtd_nfe
    from {{ ref('stg_nfe__itens') }}
    group by cnpj_emitente, uf_emitente, competencia
),

dim_empresa as (select cnpj, empresa_key, uf from {{ ref('dim_empresa') }})

select
    {{ dbt_utils.generate_surrogate_key(['s.cnpj_empresa', 's.competencia']) }} as apuracao_key,

    e.empresa_key,
    s.cnpj_empresa,
    s.competencia,
    e.uf,

    -- Valores do SPED (apuração declarada)
    s.vl_debitos_icms,
    s.vl_creditos_icms,
    s.vl_icms_recolher,
    s.vl_saldo_credor,
    s.vl_bc_icms_total   as vl_bc_icms_sped,
    s.vl_icms_notas      as vl_icms_sped,
    s.qtd_documentos     as qtd_docs_sped,

    -- Valores das NF-e (emitidas)
    n.v_bc_icms_nfe,
    n.v_icms_nfe,
    n.qtd_nfe,

    -- Divergência (base para o alerta)
    round(n.v_icms_nfe - s.vl_icms_notas, 2) as divergencia_icms,
    round(
        abs(n.v_icms_nfe - s.vl_icms_notas) / n.v_icms_nfe * 100, 2
    )                                          as divergencia_pct,

    current_timestamp() as dt_carga

from sped s
left join nfe n
    on  n.cnpj_empresa = s.cnpj_empresa
    and n.competencia  = s.competencia
left join dim_empresa e on e.cnpj = s.cnpj_empresa
