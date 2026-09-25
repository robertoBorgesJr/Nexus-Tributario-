{{
  config(
    materialized = 'view',
    description  = 'Staging: itens declarados na EFD Contribuições (C170), '
                   'agregados por empresa × competência × produto'
  )
}}

select
    cnpj_empresa,
    competencia,
    cod_item                                                                                          as cod_produto,
    cst_pis,
    cst_cofins,
    count(*)                                                                                          as qtd_linhas_c170,
    cast(sum(greatest(try_cast(nullif(vl_item,      '') as decimal(18,2)), 0.00)) as decimal(18,2))  as vl_item_total,
    cast(sum(greatest(try_cast(nullif(vl_bc_pis,    '') as decimal(18,2)), 0.00)) as decimal(18,2))  as vl_bc_pis_efd,
    cast(sum(greatest(try_cast(nullif(vl_pis,       '') as decimal(18,2)), 0.00)) as decimal(18,2))  as vl_pis_efd,
    cast(sum(greatest(try_cast(nullif(vl_bc_cofins, '') as decimal(18,2)), 0.00)) as decimal(18,2))  as vl_bc_cofins_efd,
    cast(sum(greatest(try_cast(nullif(vl_cofins,    '') as decimal(18,2)), 0.00)) as decimal(18,2))  as vl_cofins_efd
from {{ source('bronze_efd_contrib', 'efd_contrib_c170') }}
where cnpj_empresa is not null
  and competencia  is not null
  and cod_item     is not null
group by cnpj_empresa, competencia, cod_item, cst_pis, cst_cofins
