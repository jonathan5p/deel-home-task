-- The gold views in the lake over the silver tables the silver scenario leaves.

SELECT 'calendar', count(*) FROM lake.gold.dim_date;

SELECT 'statuses', count(*) FROM lake.gold.dim_order_status;

SELECT 'freshness', entity, last_changed_at IS NOT NULL
FROM lake.gold.data_freshness
ORDER BY entity;
