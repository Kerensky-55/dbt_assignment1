with rfm_score as (
    select
        customer_id,
        recency,
        frequency,
        monetary_value,
        cast(ntile(4) over (order by recency asc) as string) as recency_score, 
        cast(ntile(4) over (order by frequency desc) as string) as frequency_score,
        cast(ntile(4) over (order by monetary_value desc) as string) as monetary_score
    from {{ ref("fct_rfm") }}
),
rfm_tiers AS (
    select
        customer_id,
        recency, 
        CONCAT('R-Tier-', recency_score) AS recency_tier,
        frequency, 
        CONCAT('F-Tier-', frequency_score) AS frequency_tier,
        monetary_value,
        CONCAT('M-Tier-', monetary_score) AS monetary_tier
    FROM rfm_score
)

select * from rfm_tiers
