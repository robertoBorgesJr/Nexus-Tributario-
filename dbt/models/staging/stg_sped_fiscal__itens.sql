{{
  config(
    materialized = 'view',
    description  = 'Staging: itens declarados no SPED Fiscal (C170), '
                   'agregados por empresa × competência × produto × CFOP'
  )
}}

select
    cnpj_empresa,
    competencia,
    cod_item                                                                                          as cod_produto,
    cst_icms,
    cfop,
    count(*)                                                                                          as qtd_linhas_c170,
    cast(sum(greatest(try_cast(nullif(qtd,          '') as decimal(18,4)), 0.0000)) as decimal(18,4)) as qtd_total,
    cast(sum(greatest(try_cast(nullif(vl_item,      '') as decimal(18,2)), 0.00))   as decimal(18,2)) as vl_item_total,
    cast(sum(greatest(try_cast(nullif(vl_bc_icms,   '') as decimal(18,2)), 0.00))   as decimal(18,2)) as vl_bc_icms_sped,
    cast(sum(greatest(try_cast(nullif(vl_icms,      '') as decimal(18,2)), 0.00))   as decimal(18,2)) as vl_icms_sped,
    cast(sum(greatest(try_cast(nullif(vl_ipi,       '') as decimal(18,2)), 0.00))   as decimal(18,2)) as vl_ipi_sped,
    cast(sum(greatest(try_cast(nullif(vl_bc_pis,    '') as decimal(18,2)), 0.00))   as decimal(18,2)) as vl_bc_pis_sped,
    cast(sum(greatest(try_cast(nullif(vl_pis,       '') as decimal(18,2)), 0.00))   as decimal(18,2)) as vl_pis_sped,
    cast(sum(greatest(try_cast(nullif(vl_bc_cofins, '') as decimal(18,2)), 0.00))   as decimal(18,2)) as vl_bc_cofins_sped,
    cast(sum(greatest(try_cast(nullif(vl_cofins,    '') as decimal(18,2)), 0.00))   as decimal(18,2)) as vl_cofins_sped,
    max(greatest(try_cast(nullif(aliq_icms,         '') as decimal(7,2)),  0.00))                    as aliq_icms
from {{ source('bronze_sped_fiscal', 'sped_fiscal_c170') }}
where ind_mov      = '0'
  and cnpj_empresa is not null
  and competencia  is not null
  and cod_item     is not null
group by cnpj_empresa, competencia, cod_item, cst_icms, cfop
