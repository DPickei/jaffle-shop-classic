WITH customer_orders AS (
    SELECT
        customer_id,
        SUM(order_value) AS total_spent,
        COUNT(order_id) AS num_orders
    FROM {{ ref('orders') }}
    GROUP BY customer_id
)

SELECT
    customer_id,
    total_spent,
    num_orders,
    CASE
        WHEN num_orders = 0 THEN 0
        ELSE total_spent / num_orders
    END AS avg_order_value,
    CASE
        WHEN num_orders = 0 THEN 0
        ELSE total_spent * 0.1  -- Assume 10% margin for CLV calculation
    END AS lifetime_value
FROM customer_orders