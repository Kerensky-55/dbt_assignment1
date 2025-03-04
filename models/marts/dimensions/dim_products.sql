{{ config(
    cluster_by=['stock_code']
    )
}}

with latest_unit_price as (
    select
        stock_code,
        description,
        unit_price,
        invoice_date,
        row_number() over (partition by stock_code order by invoice_date desc) as row_num
    from {{ ref("fct_sales") }}
),
total_sold as (
    select
        stock_code,
        sum(quantity) as total_quantity_sold,
        round(sum(revenue), 2) as total_revenue
    from {{ ref("fct_sales") }}
    group by stock_code
),
products as (
    select 
        l.stock_code,
        l.description,
        l.unit_price as latest_unit_price,
        t.total_quantity_sold,
        t.total_revenue
    from latest_unit_price l
    left join total_sold t on l.stock_code = t.stock_code
    where l.row_num = 1
)

select * from products