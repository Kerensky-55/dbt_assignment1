with rfm as (
    select * from {{ ref('fct_rfm') }}
),
customer_spending as (
    select * from {{ ref('int__customer_spendings') }}
),
rfm_segments as (
    select
        r.customer_id,
        r.recency,
        r.frequency,
        r.monetary_value,
        c.country,
        case
            when r.recency <= 30 AND r.frequency >= 15 then 'VIP Customers'
            when r.recency > 90 AND r.frequency < 3 then 'Churned Customers'
            when r.frequency >= 10 then 'Loyal Customers'
            else 'Occasional Buyers'
        end as customer_segment
    from rfm r
    JOIN customer_spending c ON r.customer_id = c.customer_id
)

SELECT * FROM rfm_segments
