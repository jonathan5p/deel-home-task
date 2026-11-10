-- Number of open pending items by product: items of open orders.
SELECT
    i.product_id,
    count(*) AS pending_items,
    sum(i.quantity) AS pending_quantity
FROM gold.current_order_items AS i
JOIN gold.current_orders AS o ON o.order_id = i.order_id
JOIN gold.dim_order_status AS s ON s.status_key = o.status_key
WHERE s.is_open
GROUP BY i.product_id
ORDER BY i.product_id;
