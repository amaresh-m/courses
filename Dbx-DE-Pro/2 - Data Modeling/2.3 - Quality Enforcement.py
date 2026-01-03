# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_silver
# MAGIC VALUES ('1', '2022-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('2', '2019-05-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('3', '2023-01-01 00:00:00.000', 'C00001', 0, 0, NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC WHERE order_id IN ('1', '2', '3')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT valid_quantity CHECK (quantity > 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC where quantity <= 0

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
        .filter("quantity > 0")
     .writeStream
        .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()


# Imports PySpark SQL functions as F.
# Defines a JSON schema for order records, including nested book details.
# Reads streaming data from the bronze table, filtering for rows where topic is 'orders'.
# Parses the JSON in the value column using the defined schema.
# Selects all parsed fields and filters for records with quantity > 0.
# Writes the filtered streaming data to the orders_silver table, using a checkpoint for fault tolerance and availableNow trigger for batch-style streaming.
# Awaits termination to ensure the streaming write completes.
# This code ensures only valid orders (quantity > 0) are ingested into the silver table, with robust schema enforcement and streaming reliability.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver DROP CONSTRAINT timestamp_within_range;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_silver

# COMMAND ----------

dbutils.fs.rm(f"{bookstore.checkpoint_path}/orders_silver", True)