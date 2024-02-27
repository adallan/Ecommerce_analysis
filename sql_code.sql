#Creation of Customers Table
  
CREATE TABLE ecom_data.customers AS
SELECT 
    ROW_NUMBER() OVER () AS customer_id,
    ship_postal_code as postal_code,
    COUNT(DISTINCT order_id) AS num_orders,
    ROUND(SUM(amount),2) AS total_spent
FROM `ecom-dbt-project.ecom_data.Amazon_sales_report` 
GROUP BY ship_postal_code
