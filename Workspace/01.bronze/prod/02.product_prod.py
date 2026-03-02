# Databricks notebook source
raw_path = "/Volumes/e_commerce/logistics/source/raw"
bronze_path = "/Volumes/e_commerce/logistics/source/ETL/bronze"

# COMMAND ----------

df_product = spark.read.csv(
    f"{raw_path}/product.csv", \
        header = True,
        sep = ","
)

# COMMAND ----------

df_product.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{bronze_path}/product")