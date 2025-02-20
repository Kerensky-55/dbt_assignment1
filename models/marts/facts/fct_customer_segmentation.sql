with rfm_tiers as (
    select
        customer_id,
        cast(substring(recency_tier, 8) as integer) as recency_tier, 
        cast(substring(frequency_tier, 8) as integer) as frequency_tier,
        cast(substring(monetary_tier, 8) as integer) as monetary_tier,
    from {{ ref("fct_rfm_tiers") }}
),
customer_segments as (
    select
        customer_id,
        case 
            when recency_tier = 1 and frequency_tier = 1 and monetary_tier = 1 then 'Best Customer'
            when recency_tier = 1 and frequency_tier = 4 and monetary_tier in (1, 2) then 'High-spending New Customer'
            when recency_tier = 1 and frequency_tier = 1 and monetary_tier in (3, 4) then 'Lowest-Spending Active Loyal Customer'
            when recency_tier = 4 and frequency_tier in (1, 2) and monetary_tier in (1, 2) then 'Churned Best Customer'
            else 'Other Customer'
        end as customer_segment
    from rfm_tiers
)

select * from customer_segments
WHERE customer_id = '12370'