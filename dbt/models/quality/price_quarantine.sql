{{ config(materialized='table') }}

with supplier_data as (
    select 
        source_file,
        sku,
        coalesce(
            try_cast(columns('effective_at|valid_from|effective_date') as date),
            current_date
        ) as valid_from,
        cast(coalesce(columns('cost_sar|cost|unit_cost'), '0') as varchar) as raw_cost,
        cost_sar
    from {{ source('raw', 'supplier_prices') }}
),

product_ref as (
    select trim(sku) as sku from {{ source('raw', 'products') }}
)

select
    source_file,
    sku,
    valid_from,
    raw_cost,
    case
        when trim(sku) not in (select sku from product_ref) then 'unknown_sku'
        else 'invalid_cost'
    end as reason
from supplier_data
where trim(sku) not in (select sku from product_ref)
   or cost_sar is null 
   or cost_sar <= 0
   