-- Top 3 customers with more pending (open) orders, ties for third place included.
-- WITH TIES compares rows on the whole ORDER BY, so the inner query orders by the count
-- alone and the outer query adds the customer for a stable output order. The left join
-- keeps a customer that is not in dim_customer yet, with a null name.
SELECT
    top_customers.customer_id,
    c.customer_name,
    top_customers.pending_orders
FROM (
    SELECT
        o.customer_id,
        count(*) AS pending_orders
    FROM gold.current_orders AS o
    JOIN gold.dim_order_status AS s ON s.status_key = o.status_key
    WHERE s.is_open AND NOT o._is_deleted
    GROUP BY o.customer_id
    ORDER BY pending_orders DESC
    FETCH FIRST 3 ROWS WITH TIES
) AS top_customers
LEFT JOIN gold.dim_customer AS c
    ON c.customer_id = top_customers.customer_id AND c._is_current
ORDER BY top_customers.pending_orders DESC, top_customers.customer_id;
