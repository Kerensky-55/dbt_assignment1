{%- set table_name = 'base_raw_retail_schema__raw_retail_data' -%}
{%- set columns = get_column_names(table_name) -%}
select
    invoiceno, 
    stockcode, 
    TRIM(description) AS description, 
    quantity, 
    TO_DATE(invoicedate) AS invoicedate,
    unitprice, 
    customerid, 
    country
from {{ ref(table_name) }}
where
    quantity >= 0 and
{%- for column in columns %}
    {{ column }} is not null
    {% if not loop.last -%}
        and
    {%- endif -%}
{% endfor %}
