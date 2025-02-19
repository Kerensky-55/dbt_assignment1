select
    distinct customer_id,
    country
from {{ ref("stg_raw_retail_schema__raw_retail_data")}}
order by customer_id