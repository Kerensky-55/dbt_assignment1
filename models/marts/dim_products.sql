WITH ranked_products AS (
    SELECT
        stock_code,
        description,
        unit_price,
        invoicedate,
        ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY invoicedate DESC) AS row_num
    FROM
        {{ ref('stg_raw_retail_schema__raw_retail_data') }}
),
final as (
    SELECT
        stock_code,
        description,
        unit_price
    FROM
        ranked_products
    WHERE
        row_num = 1
)

select * from final
