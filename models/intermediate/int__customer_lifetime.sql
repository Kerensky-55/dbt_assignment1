with customer_lifetime as (
    select
        customer_id,
        min(invoicedate) as first_purchase,
        max(invoicedate) as latest_purchase,
        datediff(day, min(invoicedate), max(invoicedate)) as customer_lifetime_days
    from {{ ref('stg_raw_retail_schema__raw_retail_data') }}
    group by customer_id
)

select * from customer_lifetime