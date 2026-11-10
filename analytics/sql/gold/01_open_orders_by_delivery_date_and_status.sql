-- Number of open orders by delivery date and status.
SELECT delivery_date, status_code, open_orders
FROM gold.open_orders_by_delivery_date_and_status
ORDER BY delivery_date, status_code;
