# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

bronze_path = "/Volumes/e_commerce/logistics/source/ETL/bronze"
silver_path = "/Volumes/e_commerce/logistics/source/ETL/silver"

# COMMAND ----------

df_product = spark.read.format("delta").load(
    f"{bronze_path}/product"
)

# COMMAND ----------

display(df_product.limit(50))

# COMMAND ----------

df_product.printSchema()

# COMMAND ----------

schema_map = {
    "id": "int",
    "gender": "string",
    "masterCategory": "string",
    "subCategory": "string",
    "articleType": "string",
    "baseColour": "string",
    "season": "string",
    "year": "int",
    "usage": "string",
    "productDisplayName": "string"
}

for column, data_type in schema_map.items():
    df_product = df_product.withColumn(column, df_product[column].cast(data_type))

# COMMAND ----------

df_product.printSchema()

# COMMAND ----------

df_product = df_product.withColumnRenamed("id", "product_id")
df_product = df_product.withColumnRenamed("masterCategory", "master_category")
df_product = df_product.withColumnRenamed("subCategory", "sub_category")
df_product = df_product.withColumnRenamed("articleType", "article_type")
df_product = df_product.withColumnRenamed("baseColour", "base_colour")
df_product = df_product.withColumnRenamed("productDisplayName", "product_display_name")

# COMMAND ----------

df_product.printSchema()

# COMMAND ----------

display(
    df_product.groupBy("product_id") \
        .count() \
        .filter("count > 1")
)

# COMMAND ----------

display(df_product.select("gender").distinct())

# COMMAND ----------

df_product = df_product.withColumn(
    "gender",
    when(df_product["gender"].isin("Men", "Boys"), "M")
    .when(df_product["gender"].isin("Women", "Girls"), "F")
    .when(df_product["gender"].isin("Unisex"), "U")
    .otherwise("Undefined")
)

# COMMAND ----------

display(df_product.select("gender").distinct())

# COMMAND ----------

display(df_product.select("master_category").distinct())

# COMMAND ----------

display(df_product.select("sub_category").distinct())

# COMMAND ----------

display(
    df_product.select(["product_display_name", "sub_category"]) \
        .filter(
            (df_product["sub_category"] == "Skin") | (df_product["sub_category"] == "Skin Care")
        )
)

# COMMAND ----------

df_product = df_product.withColumn(
    "sub_category",
    when(df_product["sub_category"] == "Skin", "Skin Care") \
        .otherwise(df_product["sub_category"])
)

# COMMAND ----------

display(
    df_product.select(["product_display_name", "sub_category"]) \
        .filter(
            (df_product["sub_category"] == "Perfumes") | (df_product["sub_category"] == "Fragrance")
        )
)

# COMMAND ----------

display(
    df_product.select(["product_display_name", "sub_category"]) \
        .filter(df_product["sub_category"] == "Perfumes")
)

# COMMAND ----------

df_product = df_product.withColumn(
    "sub_category",
    when(df_product["sub_category"] == "Perfumes", "Fragrance") \
        .otherwise(df_product["sub_category"])
)

# COMMAND ----------

display(
    df_product.select(["product_display_name", "sub_category"]) \
        .filter(df_product["sub_category"] == "Fragrance")
)

# COMMAND ----------

display(df_product.select("sub_category").distinct())

# COMMAND ----------

df_product = df_product.withColumn(
    "sub_category",
    when(df_product["sub_category"] == "Apparel Set", "Apparel Sets")
    .when(df_product["sub_category"] == "Dress", "Dresses")
    .when(df_product["sub_category"] == "Saree", "Sarees")
    .when(df_product["sub_category"] == "Water Bottle", "Water Bottles")
    .when(df_product["sub_category"] == "Sandal", "Sandals")
    .when(df_product["sub_category"] == "Fragrance", "Fragrances")
    .otherwise(df_product["sub_category"])
)

# COMMAND ----------

display(df_product.limit(50))

# COMMAND ----------

display(df_product.select("article_type").distinct())

# COMMAND ----------

df_product = df_product.withColumn(
    "article_type",
    when(df_product["article_type"] == "Tshirts", "T-Shirts")
    .when(df_product["article_type"] == "Night suits", "Night Suits")
    .when(df_product["article_type"] == "Key chain", "Key Chain")
    .when(df_product["article_type"] == "Ipad", "iPad")
    .when(df_product["article_type"] == "Mens Grooming Kit", "Men's Grooming Kit")
    .otherwise(df_product["article_type"])
)

# COMMAND ----------

display(df_product.select("base_colour").distinct())

# COMMAND ----------

df_product = df_product.withColumn(
    "base_colour",
    when(df_product["base_colour"] == "NA", None)
    .otherwise(df_product["base_colour"])
)

# COMMAND ----------

display(df_product.select("season").distinct())

# COMMAND ----------

display(df_product.select("year").distinct())

# COMMAND ----------

display(df_product.select("usage").distinct())

# COMMAND ----------

df_product = df_product.withColumn(
    "usage",
    when(df_product["usage"] == "NA", None)
    .otherwise(df_product["usage"])
)

# COMMAND ----------

df_product.filter(df_product["product_display_name"].isNull()).count()

# COMMAND ----------

df_product.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{silver_path}/product")

# COMMAND ----------

df_product.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("e_commerce.logistics.silver_product")