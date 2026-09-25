{{
  config(
    materialized = 'view',
    description  = 'Staging: apuração mensal de ICMS do SPED Fiscal (registro E110) com totais de C100'
  )
}}

with c100 as (
    select
        cnpj_empresa,
        competencia,
        chv_nfe,
        greatest(try_cast(nullif(vl_bc_icms, '') as decimal(18,2)), 0.00) as vl_bc_icms_sped,
        greatest(try_cast(nullif(vl_icms,    '') as decimal(18,2)), 0.00) as vl_icms_sped,
        greatest(try_cast(nullif(vl_pis,     '') as decimal(18,2)), 0.00) as vl_pis_sped,
        greatest(try_cast(nullif(vl_cofins,  '') as decimal(18,2)), 0.00) as vl_cofins_sped
    from {{ source('bronze_sped_fiscal', 'sped_fiscal_c100') }}
    where cod_sit      = '00'
      and cnpj_empresa is not null
      and competencia  is not null
),

e110 as (
    select
        cnpj_empresa,
        competencia,
        greatest(try_cast(nullif(vl_tot_debitos,      '') as decimal(18,2)), 0.00) as vl_debitos_icms,
        greatest(try_cast(nullif(vl_tot_creditos,     '') as decimal(18,2)), 0.00) as vl_creditos_icms,
        greatest(try_cast(nullif(vl_icms_recolher,    '') as decimal(18,2)), 0.00) as vl_icms_recolher,
        greatest(try_cast(nullif(vl_sld_credor_transp,'') as decimal(18,2)), 0.00) as vl_saldo_credor
    from {{ source('bronze_sped_fiscal', 'sped_fiscal_e110') }}
    where cnpj_empresa is not null
      and competencia  is not null
)

select
    e.cnpj_empresa,
    e.competencia,
    e.vl_debitos_icms,
    e.vl_creditos_icms,
    e.vl_icms_recolher,
    e.vl_saldo_credor,
    sum(c.vl_bc_icms_sped)    as vl_bc_icms_total,
    sum(c.vl_icms_sped)       as vl_icms_notas,
    sum(c.vl_pis_sped)        as vl_pis_sped,
    sum(c.vl_cofins_sped)     as vl_cofins_sped,
    count(distinct c.chv_nfe) as qtd_documentos
from e110 e
left join c100 c
    on  c.cnpj_empresa = e.cnpj_empresa
    and c.competencia  = e.competencia
group by
    e.cnpj_empresa, e.competencia,
    e.vl_debitos_icms, e.vl_creditos_icms,
    e.vl_icms_recolher, e.vl_saldo_credor
