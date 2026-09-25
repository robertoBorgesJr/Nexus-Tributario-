{{
  config(
    materialized = 'view',
    description  = 'Staging: apuração mensal de PIS/COFINS da EFD Contribuições (M200 + M600)'
  )
}}

with m200 as (
    select
        cnpj_empresa,
        competencia,
        greatest(try_cast(nullif(vl_cont_cump, '') as decimal(18,2)), 0.00) as vl_pis_devido
    from {{ source('bronze_efd_contrib', 'efd_contrib_m200') }}
    where cnpj_empresa is not null
      and competencia  is not null
),

m600 as (
    select
        cnpj_empresa,
        competencia,
        greatest(try_cast(nullif(vl_cont_cump, '') as decimal(18,2)), 0.00) as vl_cofins_devido
    from {{ source('bronze_efd_contrib', 'efd_contrib_m600') }}
    where cnpj_empresa is not null
      and competencia  is not null
),

c100 as (
    select
        cnpj_empresa,
        competencia,
        chv_nfe,
        greatest(try_cast(nullif(vl_bc_pis,    '') as decimal(18,2)), 0.00) as vl_bc_pis,
        greatest(try_cast(nullif(vl_pis,       '') as decimal(18,2)), 0.00) as vl_pis,
        greatest(try_cast(nullif(vl_bc_cofins, '') as decimal(18,2)), 0.00) as vl_bc_cofins,
        greatest(try_cast(nullif(vl_cofins,    '') as decimal(18,2)), 0.00) as vl_cofins
    from {{ source('bronze_efd_contrib', 'efd_contrib_c100') }}
    where cod_sit      = '00'
      and cnpj_empresa is not null
      and competencia  is not null
)

select
    coalesce(m2.cnpj_empresa, m6.cnpj_empresa) as cnpj_empresa,
    coalesce(m2.competencia,  m6.competencia)  as competencia,
    m2.vl_pis_devido,
    m6.vl_cofins_devido,
    sum(c.vl_bc_pis)          as vl_bc_pis_total,
    sum(c.vl_pis)             as vl_pis_notas,
    sum(c.vl_bc_cofins)       as vl_bc_cofins_total,
    sum(c.vl_cofins)          as vl_cofins_notas,
    count(distinct c.chv_nfe) as qtd_documentos
from m200 m2
full outer join m600 m6
    on  m2.cnpj_empresa = m6.cnpj_empresa
    and m2.competencia  = m6.competencia
left join c100 c
    on  c.cnpj_empresa = coalesce(m2.cnpj_empresa, m6.cnpj_empresa)
    and c.competencia  = coalesce(m2.competencia,  m6.competencia)
group by
    coalesce(m2.cnpj_empresa, m6.cnpj_empresa),
    coalesce(m2.competencia,  m6.competencia),
    m2.vl_pis_devido,
    m6.vl_cofins_devido
