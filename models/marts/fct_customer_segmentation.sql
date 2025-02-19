WITH customer_spending AS (
    SELECT * FROM {{ ref('int__customer_spendings') }}
),
latest_purchase AS (
    SELECT
        customer_id,
        latest_purchase
    FROM {{ ref("int__customer_lifetime") }}
),
current_date AS (
    SELECT MAX(InvoiceDate) AS today FROM {{ ref('stg_raw_retail_schema__raw_retail_data') }}
),
rfm_tiers AS (
    SELECT
        customer_id,
        CAST(SUBSTRING(recency_tier, 8) AS INTEGER) AS recency_tier, 
        CAST(SUBSTRING(frequency_tier, 8) AS INTEGER) AS frequency_tier,
        CAST(SUBSTRING(monetary_tier, 8) AS INTEGER) AS monetary_tier,
        recency,
        frequency,
        monetary_value
    FROM {{ ref("fct_rfm_tiers") }}
),
customer_segments AS (
    SELECT
        customer_id,
        CASE 
            WHEN recency_tier = 1 AND frequency_tier = 1 AND monetary_tier = 1 THEN 'Best Customers'
            WHEN recency_tier = 1 AND frequency_tier = 4 AND monetary_tier IN (1, 2) THEN 'High-spending New Customers'
            WHEN recency_tier = 1 AND frequency_tier = 1 AND monetary_tier IN (3, 4) THEN 'Lowest-Spending Active Loyal Customers'
            WHEN recency_tier = 4 AND frequency_tier IN (1, 2) AND monetary_tier IN (1, 2) THEN 'Churned Best Customers'
            ELSE 'Other Customers'
        END AS customer_segment
    FROM rfm_tiers
)

SELECT * FROM customer_segments