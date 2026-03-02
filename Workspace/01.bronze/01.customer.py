# Databricks notebook source
display(dbutils.fs.ls("/Volumes/e_commerce/logistics/source/raw/"))

# COMMAND ----------

raw_path = "/Volumes/e_commerce/logistics/source/raw"
bronze_path = "/Volumes/e_commerce/logistics/source/ETL/bronze"

# COMMAND ----------

df_customer = spark.read.csv(
    f"{raw_path}/customer.csv", \
        header = True,
        sep = ","
)

# COMMAND ----------

display(df_customer.limit(10))

# COMMAND ----------

df_customer.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{bronze_path}/customer")