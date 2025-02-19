with sales as (
    select
        invoice_no,
        customer_id,
        stock_code,
        invoicedate,
        quantity,
        unit_price,
        round((quantity * unit_price), 2) as revenue
    from {{ ref('stg_raw_retail_schema__raw_retail_data') }}
)

select * from sales
