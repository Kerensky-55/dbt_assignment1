with customers as (
    select
        distinct customer_id,
        country
    from {{ ref('stg_raw_retail_schema__raw_retail_data') }}
)

select * from customers
