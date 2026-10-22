{{ config(materialized='table') }}

with cleaned_prices as (
    select
        trim(cast(sku as varchar)) as sku_code,
        try_cast(columns('cost_sar|cost|unit_cost') as decimal(18,2)) as cost_val,
        try_cast(columns('effective_at|valid_from|effective_date') as date) as valid_date,
        source_file
    from {{ source('raw', 'supplier_prices') }}
),

valid_products as (
    select trim(cast(sku as varchar)) as sku_code from {{ source('raw', 'products') }}
),

filtered_prices as (
    select distinct
        p.sku_code,
        p.cost_val as unit_cost_sar,
        p.valid_date as valid_from,
        p.source_file
    from cleaned_prices p
    join valid_products vp on p.sku_code = vp.sku_code
    where p.cost_val is not null 
      and p.cost_val > 0
      and p.valid_date is not null
),

latest_per_sku_date as (
    select 
        sku_code,
        unit_cost_sar,
        valid_from,
        row_number() over (partition by sku_code, valid_from order by source_file desc) as rn
    from filtered_prices
),

deduped_prices as (
    select sku_code, unit_cost_sar, valid_from
    from latest_per_sku_date
    where rn = 1
),

ordered_prices as (
    select
        sku_code,
        unit_cost_sar,
        valid_from,
        lag(unit_cost_sar) over (partition by sku_code order by valid_from) as prev_cost
    from deduped_prices
),

changes_marked as (
    select
        sku_code,
        unit_cost_sar,
        valid_from,
        case when prev_cost is null or prev_cost != unit_cost_sar then 1 else 0 end as is_change
    from ordered_prices
),

filtered_changes as (
    select
        sku_code,
        unit_cost_sar,
        valid_from
    from changes_marked
    where is_change = 1
),

scd2 as (
    select
        sku_code as sku,
        unit_cost_sar,
        valid_from,
        lead(valid_from) over (partition by sku_code order by valid_from) as valid_to
    from filtered_changes
)

select
    sku,
    round(unit_cost_sar, 2) as unit_cost_sar,
    valid_from,
    valid_to,
    case when valid_to is null then true else false end as is_current
from scd2
