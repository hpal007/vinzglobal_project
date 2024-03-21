# Databricks notebook source
# MAGIC %md
# MAGIC ###01. Calculate orders by date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Order_Date, COUNT( DISTINCT Order_ID) AS Order_Count
# MAGIC FROM vinz_presentation.orders
# MAGIC GROUP BY Order_Date
# MAGIC ORDER BY Order_Date;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# Read table data into a DataFrame
orders_df = spark.read.table("vinz_presentation.orders")

# Calculate orders by date
# orders_by_date = table_df.groupBy("Order_Date").agg(count("*").alias("Order_Count")).orderBy("Order_Date")
orders_by_date = table_df.groupBy("Order_Date").agg(countDistinct("Order_ID")).orderBy("Order_Date")

# Show the result
orders_by_date.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###02. Order total by date

# COMMAND ----------


order_df = spark.read.table("vinz_presentation.orders")
product_df =  spark.read.table("vinz_presentation.products")
group_df= order_df.join(product_df, on="Product_ID")
order_total_by_date  = group_df.groupBy("Order_Date").agg(expr("sum(Price * Quantity) as Order_Total"))
order_total_by_date.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select ord.Order_Date, ord.Order_ID, SUM(pro.Price * ord.Quantity) as total_order
# MAGIC from vinz_presentation.orders ord
# MAGIC join vinz_presentation.products pro
# MAGIC ON ord.Product_ID == pro.Product_ID
# MAGIC group by ord.Order_Date, ord.Order_ID
# MAGIC order by ord.Order_Date

# COMMAND ----------

# MAGIC %md
# MAGIC ###03. Total orders by product per week

# COMMAND ----------

from pyspark.sql.functions import expr, date_trunc

# Truncate Order_Date to the nearest week boundary
group_df = group_df.withColumn("Order_Week", date_trunc("week", "Order_Date"))

# Calculate the order total per week
order_total_by_week = group_df.groupBy("Order_Week").agg(expr("sum(Price * Quantity) as Order_Total")).orderBy("Order_Week")

# Show the result
order_total_by_week.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select DATE_TRUNC('WEEK',ord.Order_Date) as week_start_date, SUM(pro.Price * ord.Quantity) as total_order
# MAGIC from vinz_presentation.orders ord
# MAGIC join vinz_presentation.products pro
# MAGIC ON ord.Product_ID == pro.Product_ID
# MAGIC group by week_start_date
# MAGIC order by week_start_date

# COMMAND ----------

# MAGIC %md
# MAGIC ###04. Most sold product in last month

# COMMAND ----------

from pyspark.sql.functions import expr, month, year, current_date

# Filter data for the last month
last_month_orders = group_df.filter(
    (year("Order_Date") == year(current_date())) &
    (month("Order_Date") == month(current_date()) - 1)).select("Product_Name","Product_ID","Quantity","Price","Order_ID", "Order_Item_ID")

# last_month_orders.show()
# Calculate the total quantity sold for each product
product_total_quantity = last_month_orders.select("Product_Name","Product_ID","Quantity","Price","Order_ID", "Order_Item_ID").groupBy("Product_ID").agg(
    expr("sum(Quantity) as Total_Quantity")
)

# Find the most sold product in the last month
most_sold_product = product_total_quantity.orderBy("Total_Quantity", ascending=False).limit(1)
most_sold_product_detail = most_sold_product.join(product_df,on="Product_ID", how='inner')

# Show the result
most_sold_product_detail.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC WITH LastMonthOrders AS (
# MAGIC     SELECT o.Order_ID,o.Product_ID,p.Product_Name,o.Quantity
# MAGIC     FROM vinz_presentation.orders o
# MAGIC     JOIN vinz_presentation.products p ON o.Product_ID = p.Product_ID
# MAGIC     WHERE YEAR(Order_Date) = YEAR(CURRENT_DATE) AND MONTH(Order_Date) = MONTH(CURRENT_DATE) - 1
# MAGIC ),
# MAGIC ProductTotalQuantity AS (
# MAGIC     SELECT Product_ID, SUM(Quantity) AS Total_Quantity
# MAGIC     FROM LastMonthOrders
# MAGIC     GROUP BY Product_ID
# MAGIC )
# MAGIC
# MAGIC SELECT p.*, ptq.Total_Quantity
# MAGIC FROM ProductTotalQuantity ptq
# MAGIC JOIN vinz_presentation.products p ON ptq.Product_ID = p.Product_ID
# MAGIC ORDER BY ptq.Total_Quantity DESC
# MAGIC LIMIT 1;
# MAGIC