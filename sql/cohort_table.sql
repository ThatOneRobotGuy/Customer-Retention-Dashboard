-- What cohort is each customer in
CREATE OR REPLACE VIEW cohort_table AS
WITH customer_cohort AS (
    SELECT 
        c.customer_unique_id,
        DATE_TRUNC('month', MIN(o.order_purchase_timestamp)) AS cohort_month
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
),

-- customers and what months they're active (making an order)
customer_activity AS (
    SELECT 
        c.customer_unique_id,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS order_month
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
),

-- Customers, their cohort, and how many months after their first purchase they're active
cohort_data AS (
    SELECT 
        cc.cohort_month,
        ca.order_month,
        EXTRACT(YEAR FROM AGE(ca.order_month, cc.cohort_month)) * 12 +
        EXTRACT(MONTH FROM AGE(ca.order_month, cc.cohort_month)) AS month_number,
        ca.customer_unique_id
    FROM customer_cohort cc
    JOIN customer_activity ca 
        ON cc.customer_unique_id = ca.customer_unique_id
),

-- How many people in each cohort
cohort_size AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_unique_id) AS cohort_size
    FROM cohort_data
    WHERE month_number = 0
    GROUP BY cohort_month
)

SELECT
    cd.cohort_month,
    cd.month_number as months_since_first_purchase,
    COUNT(DISTINCT cd.customer_unique_id) as active_customers,
    cs.cohort_size,
    COUNT(DISTINCT cd.customer_unique_id)::float / cs.cohort_size as retention_rate
FROM cohort_data cd
JOIN cohort_size cs ON cs.cohort_month = cd.cohort_month
GROUP BY cd.cohort_month, cd.month_number, cs.cohort_size
ORDER BY cd.cohort_month, cd.month_number;
