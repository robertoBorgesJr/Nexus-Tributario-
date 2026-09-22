{{ config(materialized='table') }}

with base as (
    select distinct competencia
    from {{ ref('stg_nfe__itens') }}
)

select
    competencia,
    cast(substr(competencia, 1, 4) as int) as ano,
    cast(substr(competencia, 5, 2) as int) as mes,
    date_format(
        to_date(competencia || '01', 'yyyyMMdd'), 'MMMM'
    )                                      as nome_mes,
    case cast(substr(competencia, 5, 2) as int)
        when 1  then 'T1' when 2  then 'T1' when 3  then 'T1'
        when 4  then 'T2' when 5  then 'T2' when 6  then 'T2'
        when 7  then 'T3' when 8  then 'T3' when 9  then 'T3'
        when 10 then 'T4' when 11 then 'T4' when 12 then 'T4'
    end                                    as trimestre
from base
