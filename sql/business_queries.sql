-- 1. Daily sales performance
SELECT
  transaction_date,
  total_orders,
  unique_customers,
  items_sold,
  daily_revenue,
  average_order_value
FROM daily_sales_summary
ORDER BY transaction_date;

-- 2. Monthly revenue and sales growth trend
SELECT
  year_month,
  monthly_revenue,
  previous_month_revenue,
  revenue_growth_pct
FROM monthly_revenue_summary
ORDER BY year_month;

-- 3. Top-selling products by revenue
SELECT
  product_id,
  product_name,
  category,
  units_sold,
  total_revenue,
  orders_count,
  sales_rank
FROM top_selling_products
ORDER BY total_revenue DESC
LIMIT 10;

-- 4. High-value customer behavior
SELECT
  customer_id,
  customer_name,
  gender,
  age,
  orders_count,
  total_spent,
  avg_order_value,
  last_purchase_date,
  unique_products_purchased
FROM customer_behavior_summary
ORDER BY total_spent DESC
LIMIT 10;

-- 5. Category-level monthly revenue trend
SELECT
  year_month,
  category,
  ROUND(SUM(total_amount), 2) AS category_revenue,
  SUM(quantity) AS units_sold
FROM sales_enriched
GROUP BY year_month, category
ORDER BY year_month, category_revenue DESC;

-- 6. Store performance summary
SELECT
  store_id,
  store_name,
  region,
  orders_count,
  total_sales,
  items_sold,
  customer_count,
  avg_order_value
FROM store_sales_summary
ORDER BY total_sales DESC;

