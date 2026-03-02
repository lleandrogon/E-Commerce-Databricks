# Databricks notebook source
raw_path = "/Volumes/e_commerce/logistics/source/raw"
bronze_path = "/Volumes/e_commerce/logistics/source/ETL/bronze"

# COMMAND ----------

df_customer = spark.read.csv(
    f"{raw_path}/customer.csv", \
        header = True,
        sep = ","
)

# COMMAND ----------

df_customer.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{bronze_path}/customer")