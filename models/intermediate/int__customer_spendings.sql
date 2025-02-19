with customer_spendings as (
    select
        customer_id,
        country,
        ROUND(SUM(quantity*unit_price), 2) as total_amount_spent,
        COUNT(DISTINCT invoice_no) as total_orders
    from {{ ref("stg_raw_retail_schema__raw_retail_data") }}
    group by customer_id,country,invoice_no
    order by total_amount_spent desc
)

select * from customer_spendings