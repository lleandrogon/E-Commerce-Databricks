# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

gold_path = "/Volumes/e_commerce/logistics/source/ETL/gold"

# COMMAND ----------

df = spark.sql("""
    SELECT
        c.customer_id,
        c.name,
        c.gender,
        c.birthdate,
        c.home_location,
        c.home_country,
        c.home_location_lat,
        c.home_location_long,

        COUNT(CASE 
            WHEN t.payment_status = 'Success' THEN t.transaction_id
            ELSE NULL
        END) AS total_orders,

        SUM(CASE
            WHEN t.payment_status = 'Success' THEN t.total_amount
            ELSE 0
        END) AS total_spent,

        AVG(CASE
            WHEN t.payment_status = 'Success' THEN t.total_amount
            ELSE NULL
        END) AS avg_ticket,

        MIN(CASE
            WHEN t.payment_status = 'Success' THEN t.created_at
            ELSE NULL
        END) AS first_purchase_date,

        MAX(CASE
            WHEN t.payment_status = 'Success' THEN t.created_at
            ELSE NULL
        END) AS last_purchase_date,

        DATEDIFF(CURRENT_DATE(), c.first_join_date) AS customer_lifetime_days,

        DENSE_RANK() OVER (
            ORDER BY
                SUM(CASE
                    WHEN t.payment_status = 'Success' THEN t.total_amount
                    ELSE 0
                END) DESC
        ) AS customer_rank_by_revenue

    FROM e_commerce.logistics.silver_customer AS c
    LEFT JOIN e_commerce.logistics.silver_transactions AS t
    ON c.customer_id = t.customer_id
    GROUP BY
        c.customer_id,
        c.name,
        c.gender,
        c.birthdate,
        c.home_location,
        c.home_country,
        c.home_location_lat,
        c.home_location_long,
        c.first_join_date
    ORDER BY total_spent DESC        
""")

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{gold_path}/customer_rank")

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("e_commerce.logistics.gold_customer_rank")