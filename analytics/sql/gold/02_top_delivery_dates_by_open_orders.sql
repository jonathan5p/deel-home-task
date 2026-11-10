-- Top 3 delivery dates with more open orders, ties for third place included.
-- RANK gives tied dates the same place and skips the places after them, so every date
-- tied for third is kept and no more than three places are reported.
SELECT delivery_date, open_orders
FROM (
    SELECT
        o.delivery_date,
        count(*) AS open_orders,
        rank() OVER (ORDER BY count(*) DESC) AS place
    FROM gold.current_orders AS o
    JOIN gold.dim_order_status AS s ON s.status_key = o.status_key
    WHERE s.is_open
    GROUP BY o.delivery_date
) AS ranked_dates
WHERE place <= 3
ORDER BY open_orders DESC, delivery_date;
