{{
  config(
    materialized = 'view',
    description  = 'Staging: cruza cabeçalho e itens da NF-e, normaliza tipos e renomeia colunas'
  )
}}

with cabecalho as (
    select
        chv_nfe,
        cnpj_emitente,
        uf_emitente,
        cnpj_destinatario,
        uf_destinatario,
        to_date(dt_emissao, 'yyyyMMdd')  as dt_emissao,
        cast(serie as int)               as serie,
        cast(num_nf as int)              as num_nf,
        cast(tp_nf as int)               as tp_nf,      -- 0=entrada 1=saída
        crt
    from {{ source('bronze_nfe', 'nfe_cabecalho') }}
),

itens as (
    select
        chv_nfe,
        cast(num_item as int)            as num_item,
        cod_produto,
        ncm,
        cfop,
        ucom,
        cast(qtd as decimal(18, 4))      as qtd,
        cast(v_unit as decimal(18, 4))   as v_unit,
        cast(v_prod as decimal(18, 2))   as v_prod,
        cst_icms,
        cast(v_bc_icms as decimal(18, 2)) as v_bc_icms,
        cast(aliq_icms as decimal(7, 2)) as aliq_icms,
        cast(v_icms as decimal(18, 2))   as v_icms,
        cast(v_ipi as decimal(18, 2))    as v_ipi,
        cst_pis,
        cast(v_pis as decimal(18, 2))    as v_pis,
        cst_cofins,
        cast(v_cofins as decimal(18, 2)) as v_cofins,
        cast(v_nf as decimal(18, 2))     as v_nf
    from {{ source('bronze_nfe', 'nfe_itens') }}
)

select
    i.chv_nfe,
    c.cnpj_emitente,
    c.uf_emitente,
    c.cnpj_destinatario,
    c.uf_destinatario,
    c.dt_emissao,
    date_format(c.dt_emissao, 'yyyyMM')   as competencia,
    c.num_nf,
    c.crt,
    i.num_item,
    i.cod_produto,
    i.ncm,
    i.cfop,
    i.ucom,
    i.qtd,
    i.v_unit,
    i.v_prod,
    i.cst_icms,
    i.v_bc_icms,
    i.aliq_icms,
    i.v_icms,
    i.v_ipi,
    i.cst_pis,
    i.v_pis,
    i.cst_cofins,
    i.v_cofins,
    i.v_nf
from itens i
inner join cabecalho c using (chv_nfe)
