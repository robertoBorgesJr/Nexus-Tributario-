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
        to_date(nullif(dt_emissao, ''), 'yyyyMMdd') as dt_emissao,
        try_cast(nullif(serie,  '') as int)          as serie,
        try_cast(nullif(num_nf, '') as int)          as num_nf,
        crt
    from {{ source('bronze_nfe', 'nfe_cabecalho') }}
    where chv_nfe is not null
      and length(chv_nfe) = 44
),

itens as (
    select
        chv_nfe,
        try_cast(nullif(num_item, '') as int)                                              as num_item,
        cod_produto,
        ncm,
        cfop,
        ucom,
        greatest(try_cast(nullif(qtd,    '') as decimal(18,4)), 0.0000)                    as qtd,
        greatest(try_cast(nullif(v_unit, '') as decimal(18,4)), 0.0000)                    as v_unit,
        -- v_prod é denominador nos cálculos de carga tributária; NULLIF(0) habilita divisão segura
        nullif(greatest(try_cast(nullif(v_prod,   '') as decimal(18,2)), 0.00), 0.00)      as v_prod,
        cst_icms,
        greatest(try_cast(nullif(v_bc_icms, '') as decimal(18,2)), 0.00)                   as v_bc_icms,
        greatest(try_cast(nullif(aliq_icms, '') as decimal(7,2)),  0.00)                   as aliq_icms,
        greatest(try_cast(nullif(v_icms,    '') as decimal(18,2)), 0.00)                   as v_icms,
        greatest(try_cast(nullif(v_ipi,     '') as decimal(18,2)), 0.00)                   as v_ipi,
        cst_pis,
        greatest(try_cast(nullif(v_pis,     '') as decimal(18,2)), 0.00)                   as v_pis,
        cst_cofins,
        greatest(try_cast(nullif(v_cofins,  '') as decimal(18,2)), 0.00)                   as v_cofins,
        greatest(try_cast(nullif(v_nf,      '') as decimal(18,2)), 0.00)                   as v_nf
    from {{ source('bronze_nfe', 'nfe_itens') }}
    where chv_nfe is not null
)

select
    i.chv_nfe,
    c.cnpj_emitente,
    c.uf_emitente,
    c.cnpj_destinatario,
    c.uf_destinatario,
    c.dt_emissao,
    date_format(c.dt_emissao, 'yyyyMM') as competencia,
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
where c.dt_emissao is not null
