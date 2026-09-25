{{
  config(
    materialized = 'table',
    description  = 'Grão: empresa × competência × produto. Cruza NF-e emitidas com SPED Fiscal '
                   'e EFD Contribuições no nível de produto, habilitando drill-down '
                   'das divergências detectadas em fact_apuracao_icms.'
  )
}}

with nfe as (
    select
        cnpj_emitente                     as cnpj_empresa,
        competencia,
        cod_produto,
        count(distinct chv_nfe)           as qtd_nfe,
        sum(v_prod)                       as v_prod_nfe,
        -- nullif nas agregações: denominadores seguros para cálculo de pct
        nullif(sum(v_icms),   0)          as v_icms_nfe,
        nullif(sum(v_pis),    0)          as v_pis_nfe,
        nullif(sum(v_cofins), 0)          as v_cofins_nfe
    from {{ ref('stg_nfe__itens') }}
    group by cnpj_emitente, competencia, cod_produto
),

-- SPED: agrega sobre CFOPs para alinhar ao grão produto
sped as (
    select
        cnpj_empresa,
        competencia,
        cod_produto,
        sum(vl_bc_icms_sped)   as vl_bc_icms_sped,
        sum(vl_icms_sped)      as vl_icms_sped,
        sum(vl_ipi_sped)       as vl_ipi_sped,
        sum(vl_bc_pis_sped)    as vl_bc_pis_sped,
        sum(vl_pis_sped)       as vl_pis_sped,
        sum(vl_bc_cofins_sped) as vl_bc_cofins_sped,
        sum(vl_cofins_sped)    as vl_cofins_sped,
        sum(qtd_total)         as qtd_sped,
        sum(qtd_linhas_c170)   as linhas_c170_sped
    from {{ ref('stg_sped_fiscal__itens') }}
    group by cnpj_empresa, competencia, cod_produto
),

efd as (
    select * from {{ ref('stg_efd_contrib__itens') }}
),

dim_empresa as (select cnpj, empresa_key from {{ ref('dim_empresa') }}),
dim_produto  as (select cod_produto, produto_key from {{ ref('dim_produto') }})

select
    {{ dbt_utils.generate_surrogate_key([
        'nfe.cnpj_empresa', 'nfe.competencia', 'nfe.cod_produto'
    ]) }}                                          as conciliacao_key,

    e.empresa_key,
    p.produto_key,
    nfe.cnpj_empresa,
    nfe.competencia,
    nfe.cod_produto,

    -- NF-e (emitido)
    nfe.qtd_nfe,
    nfe.v_prod_nfe,
    nfe.v_icms_nfe,
    nfe.v_pis_nfe,
    nfe.v_cofins_nfe,

    -- SPED Fiscal (declarado)
    sped.vl_bc_icms_sped,
    sped.vl_icms_sped,
    sped.vl_ipi_sped,
    sped.vl_bc_pis_sped,
    sped.vl_pis_sped,
    sped.vl_bc_cofins_sped,
    sped.vl_cofins_sped,
    sped.qtd_sped,
    sped.linhas_c170_sped,

    -- EFD Contribuições (declarado)
    efd.vl_bc_pis_efd,
    efd.vl_pis_efd,
    efd.vl_bc_cofins_efd,
    efd.vl_cofins_efd,
    efd.qtd_linhas_c170                            as linhas_c170_efd,

    -- Divergências ICMS (NF-e vs SPED) — divisão segura: v_icms_nfe já é nullif(sum, 0)
    round(nfe.v_icms_nfe - coalesce(sped.vl_icms_sped, 0), 2)        as div_icms,
    round(
        abs(nfe.v_icms_nfe - coalesce(sped.vl_icms_sped, 0))
        / nfe.v_icms_nfe * 100, 2
    )                                                                  as div_icms_pct,

    -- Divergências PIS (NF-e vs EFD)
    round(nfe.v_pis_nfe - coalesce(efd.vl_pis_efd, 0), 2)            as div_pis,
    round(
        abs(nfe.v_pis_nfe - coalesce(efd.vl_pis_efd, 0))
        / nfe.v_pis_nfe * 100, 2
    )                                                                  as div_pis_pct,

    -- Divergências COFINS (NF-e vs EFD)
    round(nfe.v_cofins_nfe - coalesce(efd.vl_cofins_efd, 0), 2)      as div_cofins,
    round(
        abs(nfe.v_cofins_nfe - coalesce(efd.vl_cofins_efd, 0))
        / nfe.v_cofins_nfe * 100, 2
    )                                                                  as div_cofins_pct,

    -- Flag de anomalia: produto emitido sem declaração correspondente
    case
        when sped.cod_produto is null then 'SEM_DECLARACAO_SPED'
        when efd.cod_produto  is null then 'SEM_DECLARACAO_EFD'
        else 'OK'
    end                                                                as status_declaracao,

    current_timestamp()                                                as dt_carga

from nfe
left join sped
    on  sped.cnpj_empresa = nfe.cnpj_empresa
    and sped.competencia  = nfe.competencia
    and sped.cod_produto  = nfe.cod_produto
left join efd
    on  efd.cnpj_empresa = nfe.cnpj_empresa
    and efd.competencia  = nfe.competencia
    and efd.cod_produto  = nfe.cod_produto
left join dim_empresa e on e.cnpj = nfe.cnpj_empresa
left join dim_produto  p on p.cod_produto = nfe.cod_produto
