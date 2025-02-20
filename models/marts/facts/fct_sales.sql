{{ config(
    materialized='incremental',
    unique_key=['invoice_no', 'stock_code']
) }}

with sales as (
    select
        invoice_no,
        stock_code,
        description,
        customer_id,
        invoice_date,
        quantity,
        unit_price,
        round((quantity * unit_price), 2) as revenue
    from {{ ref('stg_raw_retail_schema__raw_retail_data') }}
    {% if is_incremental() %}
    where invoice_date > (select max(invoice_date) from {{ this }})
    {% endif %}
)

select * from sales
