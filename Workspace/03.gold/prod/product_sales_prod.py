# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

gold_path = "/Volumes/e_commerce/logistics/source/ETL/gold"

# COMMAND ----------

df = spark.sql("""
    SELECT
        p.product_id,
        p.product_display_name AS product,
        SUM(t.quantity) AS total_quantity_sold,
        SUM(t.total_amount) AS total_revenue,
        COUNT(*) AS total_orders,
        p.gender,
        p.master_category AS category,
        p.sub_category,
        p.article_type
    FROM e_commerce.logistics.silver_product AS p
    INNER JOIN e_commerce.logistics.silver_transactions AS t
    ON p.product_id = t.product_id
    WHERE t.payment_status = 'Success'
    GROUP BY
        p.product_id,
        p.product_display_name,
        p.gender,
        p.master_category,
        p.sub_category,
        p.article_type
    ORDER BY total_quantity_sold DESC;
""")

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{gold_path}/product_sales")

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("e_commerce.logistics.gold_product_sales")