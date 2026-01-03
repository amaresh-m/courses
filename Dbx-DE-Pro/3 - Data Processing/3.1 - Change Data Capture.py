# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

customers_df = (spark.table("bronze")
                 .filter("topic = 'customers'")
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                 .select("v.*")
                 .filter(F.col("row_status").isin(["insert", "update"])))

display(customers_df)

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())

ranked_df = (customers_df.withColumn("rank", F.rank().over(window))
                          .filter("rank == 1")
                          .drop("rank"))
display(ranked_df)

# COMMAND ----------

# This will throw an exception because non-time-based window operations are not supported on streaming DataFrames.
ranked_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'customers'")
                   .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                   .select("v.*")
                   .filter(F.col("row_status").isin(["insert", "update"]))
                   .withColumn("rank", F.rank().over(window))
                   .filter("rank == 1")
                   .drop("rank")
             )

(ranked_df.writeStream
            .option("checkpointLocation", f"{bookstore.checkpoint_path}/ranked")
            .trigger(availableNow=True)
            .format("console")
            .start()
)


# Cell 5 attempts to apply a window function (ranking) directly on a streaming DataFrame, which is not supported in Spark Structured Streaming. The code reads customer events from the bronze table as a stream, parses the JSON payload, filters for inserts and updates, and tries to rank each customer’s records by timestamp to keep only the latest version.

# However, the line with F.rank().over(window) will cause an exception because Spark does not allow non-time-based window operations (like ranking or partitioning) on streaming DataFrames. The code then tries to write the ranked results to the console using writeStream, but this will fail due to the unsupported window operation. This cell illustrates a common pitfall when working with streaming data: complex window functions must be handled differently, typically using foreachBatch or other batch-oriented logic.


# Spark Structured Streaming does not support non-time-based window operations (such as ranking, partitioning, or row_number) on streaming DataFrames because these operations require a complete view of all data within each partition to compute correct results. In streaming, data arrives continuously and is unbounded, so Spark cannot guarantee it has seen all records for a given partition (e.g., all events for a customer) at any point in time.

# Time-based window operations (like tumbling or sliding windows) work because they group data by event time, allowing Spark to process and finalize results for each window as data arrives. In contrast, ranking or partitioning without a time boundary would require Spark to wait indefinitely for more data, making it impossible to produce consistent, incremental results in a streaming context. This limitation ensures that streaming queries remain scalable and can produce timely, deterministic outputs.



# COMMAND ----------

from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    
    (microBatchDF.filter(F.col("row_status").isin(["insert", "update"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank == 1")
                 .drop("rank")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
        ON c.customer_id=r.customer_id
            WHEN MATCHED AND c.row_time < r.row_time
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver
# MAGIC (customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)

# COMMAND ----------

df_country_lookup = spark.read.json(f"{bookstore.dataset_path}/country_lookup")
display(df_country_lookup)

# COMMAND ----------

query = (spark.readStream
                  .table("bronze")
                  .filter("topic = 'customers'")
                  .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                  .select("v.*")
                  .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code") , "inner")
               .writeStream
                  .foreachBatch(batch_upsert)
                  .option("checkpointLocation", f"{bookstore.checkpoint_path}/customers_silver")
                  .trigger(availableNow=True)
                  .start()
          )

query.awaitTermination()

# COMMAND ----------

count = spark.table("customers_silver").count()
expected_count = spark.table("customers_silver").select("customer_id").distinct().count()

assert count == expected_count, "Unit test failed"
print("Unit test passed")