# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

gold_path = "/Volumes/e_commerce/logistics/source/ETL/gold"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM e_commerce.logistics.silver_product LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM e_commerce.logistics.silver_transactions LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     p.product_id,
# MAGIC     p.product_display_name AS product,
# MAGIC     SUM(t.quantity) AS total_quantity_sold,
# MAGIC     SUM(t.total_amount) AS total_revenue,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     p.gender,
# MAGIC     p.master_category AS category,
# MAGIC     p.sub_category,
# MAGIC     p.article_type
# MAGIC FROM e_commerce.logistics.silver_product AS p
# MAGIC INNER JOIN e_commerce.logistics.silver_transactions AS t
# MAGIC ON p.product_id = t.product_id
# MAGIC WHERE t.payment_status = 'Success'
# MAGIC GROUP BY
# MAGIC   p.product_id,
# MAGIC   p.product_display_name,
# MAGIC   p.gender,
# MAGIC   p.master_category,
# MAGIC   p.sub_category,
# MAGIC   p.article_type
# MAGIC ORDER BY total_quantity_sold DESC
# MAGIC LIMIT 50;
# MAGIC
# MAGIC

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