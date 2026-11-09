-- Top 3 delivery dates with more open orders, ties for third place included.
-- WITH TIES compares rows on the whole ORDER BY, so the inner query orders by the count
-- alone and the outer query adds the date for a stable output order.
SELECT delivery_date, open_orders
FROM (
    SELECT
        o.delivery_date,
        count(*) AS open_orders
    FROM gold.current_orders AS o
    JOIN gold.dim_order_status AS s ON s.status_key = o.status_key
    WHERE s.is_open AND NOT o._is_deleted
    GROUP BY o.delivery_date
    ORDER BY open_orders DESC
    FETCH FIRST 3 ROWS WITH TIES
) AS top_dates
ORDER BY open_orders DESC, delivery_date;
