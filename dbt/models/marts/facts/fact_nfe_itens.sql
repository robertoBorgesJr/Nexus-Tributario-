{{
  config(
    materialized = 'table',
    description  = 'Grão: item de NF-e. Principal fato do modelo analítico.'
  )
}}

with stg as (
    select * from {{ ref('stg_nfe__itens') }}
),

dim_empresa as (select cnpj, empresa_key from {{ ref('dim_empresa') }}),
dim_produto  as (select cod_produto, produto_key from {{ ref('dim_produto') }}),
dim_cfop     as (select cfop from {{ ref('dim_cfop') }}),
dim_periodo  as (select competencia from {{ ref('dim_periodo') }}),
dim_uf_emit  as (select uf, uf as uf_emitente from {{ ref('dim_uf') }}),
dim_uf_dest  as (select uf, uf as uf_destinatario from {{ ref('dim_uf') }})

select
    {{ dbt_utils.generate_surrogate_key(['s.chv_nfe', 's.num_item']) }} as item_key,

    -- Chaves estrangeiras
    e.empresa_key,
    p.produto_key,
    s.cfop,
    s.competencia,
    s.uf_emitente,
    s.uf_destinatario,

    -- Chave natural
    s.chv_nfe,
    s.num_nf,
    s.num_item,
    s.dt_emissao,

    -- Quantidades e valores
    s.qtd,
    s.v_unit,
    s.v_prod,

    -- ICMS
    s.cst_icms,
    s.v_bc_icms,
    s.aliq_icms,
    s.v_icms,
    round(s.v_icms / nullif(s.v_prod, 0) * 100, 2) as carga_icms_pct,

    -- IPI
    s.v_ipi,

    -- PIS / COFINS
    s.cst_pis,
    s.v_pis,
    s.cst_cofins,
    s.v_cofins,
    round((s.v_pis + s.v_cofins) / nullif(s.v_prod, 0) * 100, 2) as carga_pis_cofins_pct,

    -- Total da nota
    s.v_nf,
    round((s.v_icms + s.v_ipi + s.v_pis + s.v_cofins) / nullif(s.v_prod, 0) * 100, 2)
        as carga_tributaria_total_pct,

    current_timestamp() as dt_carga

from stg s
left join dim_empresa e on e.cnpj = s.cnpj_emitente
left join dim_produto  p on p.cod_produto = s.cod_produto
