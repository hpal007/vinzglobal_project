# Databricks notebook source
path = 'dbfs:/FileStore/vinzfiles/orders.json'

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DateType, StructField, StructType, LongType, ArrayType


# Wrap line_schema onto orders schema
orders_schema = StructType([
    StructField('Order_ID', IntegerType(), True),
    StructField('Customer_ID', IntegerType(), True),
    StructField('Order_Date', DateType(), True),
    StructField('Status', StringType(), True),
    StructField('Line_Items', ArrayType(StructType([
        StructField('Order_Item_ID', IntegerType(), True),
        StructField('Product_ID', IntegerType(), True),
        StructField('Quantity', IntegerType(), True),
        StructField('Status', StringType(), True)
    ])), True)
])


# COMMAND ----------

df =(spark
     .read
     .schema(orders_schema)
     .json(path))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import explode,current_timestamp
# Explode the Line_Items array column
exploded_df = df.withColumn("Line_Item", explode("Line_Items")).drop("Line_Items")

final_df = exploded_df.withColumn("Order_Item_ID", exploded_df["Line_Item"].getField("Order_Item_ID")) \
                     .withColumn("Product_ID", exploded_df["Line_Item"].getField("Product_ID")) \
                     .withColumn("Quantity", exploded_df["Line_Item"].getField("Quantity")) \
                     .withColumn("Item_Status", exploded_df["Line_Item"].getField("Status")) \
                    .withColumn('ingestion_date',current_timestamp()) \
                     .drop("Line_Item","Status")

final_df.show(truncate=False)

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("vinz_processed.orders")

# COMMAND ----------

spark.sql(f"""
              CREATE TABLE IF NOT EXISTS vinz_presentation.orders
              (
              Order_ID INT,
              Customer_ID STRING,
              Order_Date STRING,
              Order_Item_ID INT,
              Product_ID INT,
              Quantity INT,
              Item_Status STRING,
              ingestion_date TIMESTAMP
              )
              USING DELTA
""")


# COMMAND ----------
# New data 
spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW vinz_cust_vw
              AS
              SELECT * FROM vinz_processed.orders 
""")

# COMMAND ----------

# UPSERT to Final table 
spark.sql(f"""
              MERGE INTO vinz_presentation.orders tgt
              USING vinz_cust_vw upd
              ON (tgt.Order_ID = upd.Order_ID AND tgt.Order_Item_ID = upd.Order_Item_ID )
              WHEN MATCHED AND tgt.Item_Status !=upd.Item_Status  THEN
                UPDATE SET tgt.Item_Status = upd.Item_Status
              WHEN NOT MATCHED
                THEN INSERT *
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vinz_presentation.orders;

# COMMAND ----------

# Remove the prossesed file from the location
dbutils.fs.rm(path)

# COMMAND ----------

