#Creation of Customers Table.
  
CREATE TABLE ecom_data.customers AS
SELECT 
  ROW_NUMBER() OVER () AS customer_id,
  ship_postal_code as postal_code,
  COUNT(DISTINCT order_id) AS num_orders,
  ROUND(SUM(amount),2) AS total_spent
FROM `ecom-dbt-project.ecom_data.Amazon_sales_report` 
GROUP BY ship_postal_code

#Cancelled Orders by customer_ids.
SELECT 
  customer_id,
  Status,
  COUNT(Order_ID) as num_cancelled,
  COALESCE(ROUND(SUM(Amount),2),0) as total_cancelled_amount
#Changed nulls to 0 as the customer has not received a refund yet.
FROM {{ref ('stg_customers')}} as customers
JOIN {{ref ('stg_amazon_sales')}} as sales 
ON customers.postal_code = sales.ship_postal_code
WHERE status = 'Cancelled'
GROUP BY
  customer_id,
  Status
ORDER BY num_cancelled DESC

#Product sales by SKU codes/Inventory.
SELECT 
  SKU_Code, 
  ROUND(SUM(Amount),2) as Item_sales,
  ROUND(AVG(Amount),2) as Avg_price,
  inventory.Category, 
  COUNT(Order_ID) as Num_sold, 
  inventory.Size, 
  Color
FROM {{ ref ('stg_inventory')}} as inventory
JOIN {{ ref ('stg_amazon_sales')}} as sales 
ON inventory.SKU_CODE = sales.SKU
WHERE Amount != 0
GROUP BY
  SKU_CODE, 
  inventory.Category, 
  inventory.Size, 
  Color
ORDER BY num_sold DESC

# High-value Customers by sales sums and averages.
SELECT
    customer_id,
    COUNT(*) AS num_purchases,
    SUM(Amount) AS total_spent,
    ROUND(AVG(Amount),2) AS avg_order
FROM {{ref ('stg_customers')}} AS customers
JOIN {{ref ('stg_amazon_sales')}} AS sales
ON customers.postal_code = sales.ship_postal_code
WHERE 
    Amount != 0 AND Status != 'Cancelled'
GROUP BY customer_id
HAVING total_spent > 10000
ORDER BY total_spent DESC
