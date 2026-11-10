-- Top 3 customers with more pending (open) orders, ties for third place included.
-- RANK gives tied customers the same place and skips the places after them, so every
-- customer tied for third is kept. The left join keeps a customer that is not in
-- dim_customer yet, with a null name.
SELECT
    top_customers.customer_id,
    c.customer_name,
    top_customers.pending_orders
FROM (
    SELECT
        o.customer_id,
        count(*) AS pending_orders,
        rank() OVER (ORDER BY count(*) DESC) AS place
    FROM gold.current_orders AS o
    JOIN gold.dim_order_status AS s ON s.status_key = o.status_key
    WHERE s.is_open
    GROUP BY o.customer_id
) AS top_customers
LEFT JOIN gold.dim_customer AS c
    ON c.customer_id = top_customers.customer_id AND c._is_current
WHERE top_customers.place <= 3
ORDER BY top_customers.pending_orders DESC, top_customers.customer_id;
