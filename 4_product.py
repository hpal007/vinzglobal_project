# Databricks notebook source
path = 'dbfs:/FileStore/vinzfiles/product.csv'

# COMMAND ----------

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema",True) \
    .load(path)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df = df.withColumn('ingestion_date',current_timestamp())


# COMMAND ----------

df.show()

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("vinz_processed.products")

# COMMAND ----------

# Final Table products
spark.sql(f"""
              CREATE TABLE IF NOT EXISTS vinz_presentation.products
              (
              Product_ID INT,
              Product_Name STRING,
              Description STRING,
              Price INT,
              ingestion_date TIMESTAMP
              )
              USING DELTA
""")


# New Data
spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW vinz_cust_vw
              AS
              SELECT * FROM vinz_processed.products 
""")

# UPSERT New Data in Final table 
spark.sql(f"""
              MERGE INTO vinz_presentation.products tgt
              USING vinz_cust_vw upd
              ON (tgt.Product_ID = upd.Product_ID)
              WHEN MATCHED THEN
                UPDATE SET tgt.Product_Name = upd.Product_Name,
                           tgt.Description = upd.Description,
                           tgt.Price = upd.Price,
                           tgt.ingestion_date = upd.ingestion_date
              WHEN NOT MATCHED
                THEN INSERT *
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vinz_presentation.products;

# COMMAND ----------

# Remove the prossesed file from the location
# dbutils.fs.rm(path)

# COMMAND ----------

# dbutils.fs.rm("/mnt/vinz", recurse=True)
