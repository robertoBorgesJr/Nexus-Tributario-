{{ config(materialized='table') }}

with base as (
    select distinct
        cod_produto,
        ncm,
        -- NCM de 2 dígitos identifica o capítulo da tabela TIPI
        substr(ncm, 1, 2)  as cap_ncm,
        ucom
    from {{ ref('stg_nfe__itens') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['cod_produto']) }} as produto_key,
    cod_produto,
    ncm,
    cap_ncm,
    ucom,
    current_timestamp() as dt_carga
from base
