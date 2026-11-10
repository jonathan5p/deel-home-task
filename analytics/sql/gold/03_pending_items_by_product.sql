-- Number of open pending items by product: items of open orders.
SELECT product_id, pending_items, pending_quantity
FROM gold.pending_items_by_product
ORDER BY product_id;
