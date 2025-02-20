WITH customer_lifetime AS (
    SELECT
        customer_id,
        first_purchase,
        latest_purchase
    FROM {{ ref("int__customer_lifetime") }}
),
customer_spendings AS (
    SELECT
        customer_id,
        latest_country,
        total_amount_spent,
        total_orders
    FROM {{ ref("int__customer_spendings") }}
),
customer_segments AS (
    SELECT
        customer_id,
        customer_segment
    FROM {{ ref("fct_customer_segmentation") }}
),
customers AS (
    SELECT
        cl.customer_id,
        cl.first_purchase,
        cl.latest_purchase,
        cs.latest_country,
        cs.total_amount_spent,
        cs.total_orders,
        seg.customer_segment
    FROM customer_lifetime cl
    LEFT JOIN customer_spendings cs ON cl.customer_id = cs.customer_id
    LEFT JOIN customer_segments seg ON cl.customer_id = seg.customer_id
)
SELECT * FROM customers
