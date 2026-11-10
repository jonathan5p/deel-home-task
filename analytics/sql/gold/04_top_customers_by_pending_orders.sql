-- Top 3 customers with more pending (open) orders, ties for third place included.
SELECT customer_id, customer_name, pending_orders
FROM gold.top_customers_by_pending_orders
ORDER BY pending_orders DESC, customer_id;
