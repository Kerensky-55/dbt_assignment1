with customer_spendings as (
    select
        customer_id,
        country,
        round(sum(quantity*unit_price), 2) as total_amount_spent,
        count(distinct invoice_no) as total_orders
    from {{ ref("stg_raw_retail_schema__raw_retail_data") }}
    group by customer_id,country
)

select * from customer_spendings