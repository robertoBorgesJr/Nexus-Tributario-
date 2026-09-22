{{ config(materialized='table') }}

with base as (
    select distinct cfop
    from {{ ref('stg_nfe__itens') }}
)

select
    cfop,
    substr(cfop, 1, 1)  as digito_operacao,
    case substr(cfop, 1, 1)
        when '1' then 'Entrada dentro do estado'
        when '2' then 'Entrada de outro estado'
        when '3' then 'Entrada do exterior'
        when '5' then 'Saída dentro do estado'
        when '6' then 'Saída para outro estado'
        when '7' then 'Saída para o exterior'
    end                 as tipo_operacao,
    case when substr(cfop, 1, 1) in ('1','2','3') then 'Entrada' else 'Saída' end
                        as sentido
from base
