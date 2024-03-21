# Databricks notebook source
path = 'dbfs:/FileStore/vinzfiles/customer.csv'

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

df.write.mode("overwrite").format("delta").saveAsTable("vinz_processed.customer")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Final customer table 
spark.sql(f"""
              CREATE TABLE IF NOT EXISTS vinz_presentation.customer
              (
              Customer_ID INT,
              First_Name STRING,
              Last_Name STRING,
              Email STRING,
              Phone LONG,
              ingestion_date TIMESTAMP
              )
              USING DELTA
""")


# New Data
spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW vinz_cust_vw
              AS
              SELECT * FROM vinz_processed.customer 
""")

# UPSERT New Data in Final table 
spark.sql(f"""
              MERGE INTO vinz_presentation.customer tgt
              USING vinz_cust_vw upd
              ON (tgt.Customer_ID = upd.Customer_ID)
              WHEN MATCHED THEN
                UPDATE SET tgt.First_Name = upd.First_Name,
                           tgt.Last_Name = upd.Last_Name,
                           tgt.Email = upd.Email,
                           tgt.Phone = upd.Phone,
                           tgt.ingestion_date = upd.ingestion_date
              WHEN NOT MATCHED
                THEN INSERT *
       """)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vinz_presentation.customer;

# COMMAND ----------

# Remove the prossesed file from the location
# dbutils.fs.rm(path)