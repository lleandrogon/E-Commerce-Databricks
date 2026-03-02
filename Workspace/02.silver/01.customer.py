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

display(df_customer.limit(50))

# COMMAND ----------

df_customer.printSchema()

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

df_customer.printSchema()

# COMMAND ----------

df_customer = df_customer.dropDuplicates(["customer_id"])

# COMMAND ----------

df_customer = df_customer.withColumn(
    "name",
    concat(df_customer["first_name"], lit(" "), df_customer["last_name"])
).drop("first_name", "last_name")

# COMMAND ----------

display(df_customer.limit(5))

# COMMAND ----------

display(
    df_customer.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df_customer.columns
    ])
)

# COMMAND ----------

display(df_customer.groupBy("email").count().filter("count > 1"))

# COMMAND ----------

display(
    df_customer.filter(
        df_customer["email"] != lower(df_customer["email"])
    )
)

# COMMAND ----------

display(
    df_customer.select("gender").distinct()
)

# COMMAND ----------

display(df_customer.select("device_type").distinct())

# COMMAND ----------

display(df_customer.select("device_type").distinct())

# COMMAND ----------

display(df_customer.select("home_country").distinct())

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