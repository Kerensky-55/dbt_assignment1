with ranked_customers as (
    select
        customer_id,
        country,
        invoice_date,
        row_number() over (partition by customer_id order by invoice_date desc) as row_num
    from
        {{ ref('stg_raw_retail_schema__raw_retail_data') }}
),
customer as (
    select
        customer_id,
        country
    from
        ranked_customers
    where
        row_num = 1
    order by customer_id
)

select * from customer
