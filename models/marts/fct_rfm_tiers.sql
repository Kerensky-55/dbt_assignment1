with customer_spending as (
    select * from {{ ref("int__customer_spendings") }}
),
latest_purchase as (
    select
        customer_id,
        latest_purchase
    from {{ ref("int__customer_lifetime") }}
),
current_date AS (
    SELECT MAX(InvoiceDate) AS today FROM {{ ref("stg_raw_retail_schema__raw_retail_data") }}
),
rfm as (
    select * from {{ ref("fct_rfm") }}
),
rfm_tiers AS (
    SELECT 
        customer_id,
        recency,
        frequency,
        monetary_value,
        CAST(NTILE(4) OVER (ORDER BY recency ASC) AS STRING) AS recency_tier, 
        CAST(NTILE(4) OVER (ORDER BY frequency DESC) AS STRING) AS frequency_tier,
        CAST(NTILE(4) OVER (ORDER BY monetary_value DESC) AS STRING) AS monetary_tier
    FROM rfm
),
final as (
    select
        customer_id,
        recency, 
        CONCAT('R-Tier-', recency_tier) AS recency_tier,
        frequency, 
        CONCAT('F-Tier-', frequency_tier) AS frequency_tier,
        monetary_value,
        CONCAT('M-Tier-', monetary_tier) AS monetary_tier
    FROM rfm_tiers

)

select * from final
