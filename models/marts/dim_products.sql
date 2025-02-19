with ranked_products as (
    select
        stock_code,
        description,
        unit_price,
        invoice_date,
        row_number() over (partition by stock_code order by invoice_date desc) as row_num
    from
        {{ ref('stg_raw_retail_schema__raw_retail_data') }}
),
products as (
    select
        stock_code,
        description,
        unit_price
    from
        ranked_products
    where
        row_num = 1
)

select * from products
