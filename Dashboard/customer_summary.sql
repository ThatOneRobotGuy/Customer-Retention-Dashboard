CREATE OR REPLACE VIEW customer_summary AS
WITH cte AS (
    SELECT
        c.customer_unique_id,
        MIN(DATE_TRUNC('day', oe.order_purchase_timestamp)) as first_purchase_date,
        COUNT(DISTINCT oe.order_id) as total_orders,
        SUM(payment_value) as total_revenue
    FROM customers AS c
    JOIN orders o ON o.customer_id = c.customer_id
    JOIN orders_enriched oe ON oe.order_id = o.order_id
    GROUP BY c.customer_unique_id
    ORDER BY total_revenue DESC
),

customer_decile AS (
    SELECT 
        customer_unique_id,
        FLOOR(row_number() OVER () * 10.0 / COUNT(*) OVER ()) * 10 + 10 as decile
    FROM cte
),

customer_activity AS (
    SELECT DISTINCT
        c.customer_unique_id,
        o.order_purchase_timestamp AS order_day
    FROM orders o
    JOIN customers c 
        ON o.customer_id = c.customer_id
),

ranked_purchases AS (
    SELECT
        customer_unique_id,
        order_day,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY order_day
        ) AS order_rank
    FROM customer_activity
),

first_and_second_purchases AS (
    SELECT 
        *,
        MAX(order_rank) OVER (PARTITION BY customer_unique_id) as total_orders
    FROM ranked_purchases
    WHERE order_rank <= 2
    ORDER BY customer_unique_id, order_rank
),

filtered_purchases AS (
    SELECT
        customer_unique_id, 
        order_day,
        order_rank
    FROM first_and_second_purchases
    WHERE total_orders = 2
),

first_purchases_with_delay_to_second_day AS (
    SELECT
        customer_unique_id,
        order_day,
        order_rank,
        order_day - LAG(order_day) OVER(PARTITION BY customer_unique_id) as days_to_second_purchase
    FROM filtered_purchases
),

purchases_with_delays AS (
    SELECT
        customer_unique_id,
        date_trunc('day', days_to_second_purchase) as days_to_second_purchase
    FROM first_purchases_with_delay_to_second_day
    WHERE order_rank = 2
)

SELECT
    cte.customer_unique_id,
    first_purchase_date,
    total_orders,
    total_revenue,
    total_revenue / total_orders as avg_order_value,
    decile,
    CASE
        WHEN total_orders > 1 THEN 1
        ELSE 0
    END as repeat_purchase_flag,
    CASE
        WHEN total_orders = 1 THEN '1 Order'
        WHEN total_orders = 2 THEN '2 Orders'
        WHEN total_orders = 3 THEN '3 Orders'
        ELSE '4+ Orders'
    END as total_orders_level,
    days_to_second_purchase
FROM cte
JOIN customer_decile cd ON cd.customer_unique_id = cte.customer_unique_id
LEFT JOIN purchases_with_delays pd ON pd.customer_unique_id = cd.customer_unique_id 
