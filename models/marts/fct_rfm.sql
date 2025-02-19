with customer_spending as (
    select * from {{ ref("fct_customer_spendings") }}
),
latest_purchase as (
    select
        customer_id,
        latest_purchase
    from {{ ref("fct_customer_lifetime") }}
),
current_date as (
    select max(invoice_date) as today from {{ ref('stg_raw_retail_schema__raw_retail_data') }}
),
rfm as (
    select
        c.customer_id,
        datediff(day, l.latest_purchase, cd.today) as recency,
        c.total_orders as frequency,
        c.total_amount_spent as monetary_value,
    from customer_spending c
    join latest_purchase l on c.customer_id = l.customer_id
    join current_date cd on 1=1
)

select * from rfm
