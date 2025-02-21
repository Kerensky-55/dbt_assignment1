{{ config(
    materialized='incremental',
    unique_key=['invoice_no', 'stock_code']
    ) 
}}
{%- set table_name = 'src_raw_retail_schema__raw_retail_data' -%}
{%- set columns = get_column_names(table_name) -%}
select
    invoiceno as invoice_no, 
    stockcode as stock_code, 
    TRIM(description) AS description, 
    quantity, 
    TO_DATE(invoicedate) AS invoice_date,
    unitprice as unit_price, 
    customerid as customer_id, 
    country
from {{ ref("src_raw_retail_schema__raw_retail_data") }}
where
    LEFT(Invoice_No, 1) != 'C' and
    quantity > 0 and
    unit_price > 0 and
{%- for column in columns %}
    {{ column }} is not null
    {% if not loop.last -%}
        and
    {%- endif -%}
{%- endfor -%}
    and 
    stock_code rlike '^[0-9]+$'
    {% if is_incremental() %}
        and invoice_date >= (select max(invoice_date) from {{ this }})
    {% endif %}