-- Number of open orders by delivery date and status.
SELECT
    o.delivery_date,
    s.status_code,
    count(*) AS open_orders
FROM gold.current_orders AS o
JOIN gold.dim_order_status AS s ON s.status_key = o.status_key
WHERE s.is_open
GROUP BY o.delivery_date, s.status_code
ORDER BY o.delivery_date, s.status_code;
