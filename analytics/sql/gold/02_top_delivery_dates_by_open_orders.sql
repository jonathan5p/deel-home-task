-- Top 3 delivery dates with more open orders, ties for third place included.
SELECT delivery_date, open_orders
FROM gold.top_delivery_dates_by_open_orders
ORDER BY open_orders DESC, delivery_date;
