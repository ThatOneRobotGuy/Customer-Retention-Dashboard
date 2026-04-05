CREATE OR REPLACE VIEW orders_enriched AS
WITH orders_almost_enriched AS (
    SELECT
        o.order_id,
        c.customer_unique_id,
        order_purchase_timestamp,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        date_trunc('day', o.order_delivered_customer_date) - date_trunc('day', order_purchase_timestamp) as delivery_days,
        date_trunc('day', order_delivered_customer_date) - date_trunc('day', order_estimated_delivery_date) as delay_days,
        ROW_NUMBER() OVER (PARTITION by c.customer_unique_id ORDER BY o.order_purchase_timestamp) as customer_order_number,
        customer_state
    FROM orders o
    JOIN customers c ON c.customer_id = o.customer_id
)

SELECT 
    oae.order_id,
    customer_unique_id,
    order_purchase_timestamp,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    delivery_days,
    delay_days,
    CASE
        WHEN order_delivered_customer_date is NULL then NULL
        WHEN EXTRACT(DAY from delay_days) > 0 THEN 1
        ELSE 0
    END as late_flag,
    customer_order_number,
    op.payment_value,
    customer_state
FROM orders_almost_enriched AS oae
JOIN order_payments op on op.order_id = oae.order_id
ORDER BY order_purchase_timestamp DESC
