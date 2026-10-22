{{ config(materialized='table') }}

with source_orders as (
    select * from {{ source('raw', 'orders') }}
),

source_order_items as (
    select
        trim(cast(order_id as varchar)) as order_id,
        trim(cast(columns('sku|product_id|item_id') as varchar)) as resolved_sku,
        cast(columns('quantity|qty|amount') as double) as resolved_qty,
        cast(columns('unit_price_sar|price|unit_price') as double) as resolved_price,
        coalesce(cast(columns('discount|disc') as double), 0.0) as resolved_disc
    from {{ source('raw', 'order_items') }}
),

source_products as (
    select * from {{ source('raw', 'products') }}
),

source_stores as (
    select * from {{ source('raw', 'stores') }}
),

cost_history as (
    select * from {{ ref('product_cost_history') }}
),

base_lines as (
    select
        o.order_id,
        cast(o.created_at as timestamp) as order_ts,
        date_part('week', cast(o.created_at as timestamp) + interval '3' hour) as iso_week_num,
        coalesce(s.city, 'Unknown') as city,
        coalesce(p.category, 'Uncategorized') as category,
        i.resolved_sku as sku,
        i.resolved_qty as qty,
        round(round(i.resolved_qty * i.resolved_price - i.resolved_disc, 2), 2) as revenue,
        c.unit_cost_sar,
        cast(cast(o.created_at as timestamp) + interval '3' hour as date) as order_date
    from source_order_items i
    join source_orders o on i.order_id = o.order_id
    left join source_stores s on o.store_id = s.store_id
    left join source_products p on i.resolved_sku = p.sku
    left join cost_history c 
        on i.resolved_sku = c.sku 
        and cast(cast(o.created_at as timestamp) + interval '3' hour as date) >= c.valid_from 
        and (cast(cast(o.created_at as timestamp) + interval '3' hour as date) < c.valid_to or c.valid_to is null)
    where o.status = 'delivered' 
      and i.resolved_qty > 0
      and cast(cast(o.created_at as timestamp) + interval '3' hour as date) >= DATE '2026-08-31'
)

select
    cast(iso_week_num as integer) as iso_week,
    cast(city as varchar) as city,
    cast(category as varchar) as category,
    cast(count(*) as bigint) as lines,
    cast(round(sum(revenue), 2) as double) as revenue_sar,
    cast(round(sum(case when unit_cost_sar is not null then revenue else 0.0 end), 2) as double) as costed_revenue_sar,
    cast(round(sum(coalesce(unit_cost_sar, 0.0) * coalesce(qty, 0.0)), 2) as double) as cost_sar,
    cast(round(sum(case when unit_cost_sar is not null then revenue else 0.0 end) - sum(coalesce(unit_cost_sar, 0.0) * coalesce(qty, 0.0)), 2) as double) as margin_sar,
    cast(sum(case when unit_cost_sar is null then 1 else 0 end) as integer) as uncosted_lines
from base_lines
group by iso_week_num, city, category
