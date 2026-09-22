{{ config(materialized='table') }}

with base as (
    select distinct
        cnpj_emitente                 as cnpj,
        uf_emitente                   as uf,
        crt,
        case crt
            when '1' then 'Simples Nacional'
            when '2' then 'Simples Nacional - Excesso'
            when '3' then 'Regime Normal'
        end                           as regime_tributario
    from {{ ref('stg_nfe__itens') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['cnpj']) }} as empresa_key,
    cnpj,
    uf,
    crt,
    regime_tributario,
    current_timestamp()               as dt_carga
from base
