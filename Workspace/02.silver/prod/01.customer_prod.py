# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

bronze_path = "/Volumes/e_commerce/logistics/source/ETL/bronze"
silver_path = "/Volumes/e_commerce/logistics/source/ETL/silver"

# COMMAND ----------

df_customer = spark.read.format("delta").load(
    f"{bronze_path}/customer"
)

# COMMAND ----------

schema_map = {
    "customer_id": "int",
    "first_name": "string",
    "last_name": "string",
    "username": "string",
    "email": "string",
    "gender": "string",
    "birthdate": "date",
    "device_type": "string",
    "device_id": "string",
    "device_version": "string",
    "home_location_lat": "double",
    "home_location_long": "double",
    "home_location": "string",
    "home_country": "string",
    "first_join_date": "date",
}

for column, data_type in schema_map.items():
    df_customer = df_customer.withColumn(column, df_customer[column].cast(data_type))

# COMMAND ----------

df_customer = df_customer.dropDuplicates(["customer_id"])

# COMMAND ----------

df_customer = df_customer.withColumn(
    "name",
    concat(df_customer["first_name"], lit(" "), df_customer["last_name"])
).drop("first_name", "last_name")

# COMMAND ----------

df_customer.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{silver_path}/customer")

# COMMAND ----------

df_customer.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("e_commerce.logistics.silver_customer")