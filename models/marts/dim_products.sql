select
    stock_code,
    description,
    unit_price
from {{ ref("stg_raw_retail_schema__raw_retail_data") }}