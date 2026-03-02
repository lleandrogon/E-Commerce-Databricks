# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

bronze_path = "/Volumes/e_commerce/logistics/source/ETL/bronze"
silver_path = "/Volumes/e_commerce/logistics/source/ETL/silver"

# COMMAND ----------

df_transactions = spark.read.format("delta").load(
    f"{bronze_path}/transactions"
)

# COMMAND ----------

df_transactions = df_transactions.withColumn(
    "transaction_id",
    monotonically_increasing_id() + 1
)

# COMMAND ----------

schema = ArrayType(
    StructType([
        StructField("product_id", LongType(), True),
        StructField("quantity", LongType(), True),
        StructField("item_price", LongType(), True)
    ])
)

df_transactions = df_transactions.withColumn(
    "product_metadata_array",
    from_json(df_transactions["product_metadata"], schema)
)

# COMMAND ----------

df_transactions = df_transactions.withColumn(
    "product",
    explode(col("product_metadata_array"))
)

# COMMAND ----------

df_transactions = df_transactions.select(
    "transaction_id",
    "customer_id",
    "booking_id",
    "session_id",
    col("product.product_id").alias("product_id"),
    col("product.quantity").alias("quantity"),
    col("product.item_price").alias("item_price"),
    "payment_method",
    "payment_status",
    "promo_amount",
    "promo_code",
    "shipment_fee",
    "shipment_date_limit",
    "shipment_location_lat",
    "shipment_location_long",
    "total_amount",
    "created_at"
)

# COMMAND ----------

schema_map = {
    "transaction_id": "long",
    "customer_id": "int",
    "booking_id": "string",
    "session_id": "string",
    "product_id": "int",
    "quantity": "int",
    "item_price": "double",
    "payment_method": "string",
    "payment_status": "string",
    "promo_amount": "double",
    "promo_code": "string",
    "shipment_fee": "double",
    "shipment_date_limit": "timestamp",
    "shipment_location_lat": "double",
    "shipment_location_long": "double",
    "total_amount": "double",
    "created_at": "timestamp"
}

for column, data_type in schema_map.items():
    df_transactions = df_transactions.withColumn(column, df_transactions[column].cast(data_type))

# COMMAND ----------

df_transactions.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{silver_path}/transactions")

# COMMAND ----------

df_transactions.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("e_commerce.logistics.silver_transactions")