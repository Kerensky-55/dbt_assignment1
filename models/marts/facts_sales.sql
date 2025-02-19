SELECT
    invoice_no,
    invoicedate,
    customer_id,
    ROUND(SUM(quantity * unit_price), 2) AS total_sales,
    ROUND(COUNT(DISTINCT invoice_no), 2) AS total_orders,
    ROUND(AVG(quantity * unit_price), 2) AS avg_order_value,
    stock_code
FROM {{ ref('stg_raw_retail_schema__raw_retail_data') }}
GROUP BY invoice_no, invoicedate, customer_id, stock_code
