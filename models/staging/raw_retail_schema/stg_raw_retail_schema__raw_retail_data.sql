{%- set table_name = 'base_raw_retail_schema__raw_retail_data' -%}
{%- set columns = get_column_names(table_name) -%}
select
    invoiceno as invoice_no, 
    stockcode as stock_code, 
    TRIM(description) AS description, 
    quantity, 
    TO_DATE(invoicedate) AS invoicedate,
    unitprice as unit_price, 
    customerid as customer_id, 
    country
from {{ ref("base_raw_retail_schema__raw_retail_data") }}
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