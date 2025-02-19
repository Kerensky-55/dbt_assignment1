with products as (
    select
        distinct stock_code,
        description,
        unit_price
    from {{ ref('stg_raw_retail_schema__raw_retail_data') }}
)

select * from products
