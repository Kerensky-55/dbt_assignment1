with rfm_tiers as (
    select * from {{ ref("fct_rfm_tiers") }}
),
customer_segments as (
    select
        customer_id,
        case 
            when recency_tier = 1 and frequency_tier = 1 and monetary_tier = 1 then 'Best Customers'
            when recency_tier = 1 and frequency_tier = 4 and monetary_tier in (1, 2) then 'High-spending New Customers'
            when recency_tier = 1 and frequency_tier = 1 and monetary_tier in (3, 4) then 'Lowest-Spending Active Loyal Customers'
            when recency_tier = 4 and frequency_tier in (1, 2) and monetary_tier in (1, 2) then 'Churned Best Customers'
            else 'Other Customers'
        end as customer_segment
    from rfm_tiers
)

select * from customer_segments