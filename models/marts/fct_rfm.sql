with customer_spending as (
    select * from {{ ref('int__customer_metrics') }}
),
latest_purchase as (
    select
        customer_id,
        latest_purchase
    from {{ ref("int__customer_lifetime") }}
),
current_date as (
    select MAX(InvoiceDate) as today from {{ ref('stg_raw_retail_schema__raw_retail_data') }}
),
rfm as (
    select
        c.customer_id,
        DATEDIFF(day, l.latest_purchase, cd.today) AS recency,
        c.total_orders AS frequency,
        c.total_amount_spent AS monetary_value,
    FROM customer_spending c
    JOIN latest_purchase l ON c.Customer_id = l.Customer_id
    JOIN current_date cd ON 1=1
)

SELECT * FROM rfm
