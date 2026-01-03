# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

(spark.readStream
      .table("bronze")
      .createOrReplaceTempView("bronze_tmp"))


# This code reads a streaming table named "bronze" using Spark Structured Streaming, and creates or replaces a temporary view called "bronze_tmp".

# COMMAND ----------

# Explicitly set the checkpoint location for streaming display, as implicit temporary checkpoint locations are not supported in the current workspace.

orders_silver_df = spark.sql("""
    SELECT v.*
    FROM (
    SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
    FROM bronze_tmp
    WHERE topic = "orders")
""")

display(orders_silver_df, checkpointLocation = f"{bookstore.checkpoint_path}/tmp/orders_silver_{time.time()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC     FROM bronze_tmp
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

query = (spark.table("orders_silver_tmp")
               .writeStream
               .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
               .trigger(availableNow=True)
               .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
     .writeStream
        .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC # Step-by-step Documentation: Streaming Orders Pipeline
# MAGIC
# MAGIC This notebook demonstrates a complete pipeline for ingesting, transforming, and storing streaming order data using Databricks and Spark Structured Streaming. Below is a step-by-step explanation of each major part of the code:
# MAGIC
# MAGIC * **1. Dataset Preparation**
# MAGIC   * The notebook begins by running a setup script (`%run ../Includes/Copy-Datasets`) to ensure required datasets are available.
# MAGIC
# MAGIC * **2. Inspecting Raw Streaming Data**
# MAGIC   * SQL queries are used to preview the raw data from the `bronze` streaming table, casting key and value columns to strings for easier inspection.
# MAGIC   * Another SQL query parses the JSON structure in the `value` column for records where `topic = 'orders'`, extracting order details and nested book information.
# MAGIC
# MAGIC * **3. Creating a Temporary Streaming View**
# MAGIC   * Python code reads the `bronze` streaming table and registers it as a temporary view (`bronze_tmp`). This enables SQL-based transformations on streaming data.
# MAGIC
# MAGIC * **4. Parsing and Displaying Orders Data**
# MAGIC   * A SQL query (in Python) parses the JSON order records from `bronze_tmp`, extracting fields such as `order_id`, `order_timestamp`, `customer_id`, `quantity`, `total`, and an array of books. The result is displayed with a specified checkpoint location for streaming.
# MAGIC
# MAGIC * **5. Creating a Silver Orders Temporary View**
# MAGIC   * A SQL cell creates or replaces a temporary view (`orders_silver_tmp`) containing the parsed order data, ready for further processing or writing to a table.
# MAGIC
# MAGIC * **6. Writing Parsed Orders to a Silver Table**
# MAGIC   * Python code writes the streaming data from `orders_silver_tmp` to a managed table (`orders_silver`) using Structured Streaming. It sets a checkpoint location and uses the `availableNow` trigger for batch-style streaming.
# MAGIC
# MAGIC * **7. Alternative Streaming Pipeline (Direct Python)**
# MAGIC   * Another Python cell demonstrates a direct approach: reading from `bronze`, filtering for `orders`, parsing JSON, and writing the result to `orders_silver`—all in Python, without intermediate SQL views.
# MAGIC
# MAGIC * **8. Querying the Silver Table**
# MAGIC   * A final SQL cell selects all records from the `orders_silver` table, allowing inspection of the fully processed and stored order data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Summary:**
# MAGIC This notebook provides two approaches (SQL and Python) for building a streaming ETL pipeline: reading raw data, parsing JSON, transforming, and writing to a Delta table. It demonstrates best practices for checkpointing, temporary views, and both SQL/Python interoperability in Databricks.