### Question 1

An upstream system has been configured to pass the date for a given batch of data to the Databricks Jobs API as a parameter. The notebook to be scheduled will use this parameter to load data with the following code: df = spark.read.format("parquet").load(f"/mnt/source/(date)")Which code block should be used to create the date Python variable used in the above code block?

- A.date = spark.conf.get("date")
- B.input_dict = input()date= input_dict["date"]
- C.import sysdate = sys.argv[1]
- D.date = dbutils.notebooks.getParam("date")
- E.dbutils.widgets.text("date", "null")date = dbutils.widgets.get("date")

---

### Question 2

The Databricks workspace administrator has configured interactive clusters for each of the data engineering groups. To control costs, clusters are set to terminate after 30 minutes of inactivity. Each user should be able to execute workloads against their assigned clusters at any time of the day.Assuming users have been added to a workspace but not granted any permissions, which of the following describes the minimal permissions a user would need to start and attach to an already configured cluster.

- A."Can Manage" privileges on the required cluster
- B.Workspace Admin privileges, cluster creation allowed, "Can Attach To" privileges on the required cluster
- C.Cluster creation allowed, "Can Attach To" privileges on the required cluster
- D."Can Restart" privileges on the required cluster
- E.Cluster creation allowed, "Can Restart" privileges on the required cluster

---

### Question 3

When scheduling Structured Streaming jobs for production, which configuration automatically recovers from query failures and keeps costs low?

- A.Cluster: New Job Cluster;Retries: Unlimited;Maximum Concurrent Runs: Unlimited
- B.Cluster: New Job Cluster;Retries: None;Maximum Concurrent Runs: 1
- C.Cluster: Existing All-Purpose Cluster;Retries: Unlimited;Maximum Concurrent Runs: 1
- D.Cluster: New Job Cluster;Retries: Unlimited;Maximum Concurrent Runs: 1
- E.Cluster: Existing All-Purpose Cluster;Retries: None;Maximum Concurrent Runs: 1

---

### Question 4

![Question 4 Image 1](examtopics_images/question_4_img_1.png)

The data engineering team has configured a Databricks SQL query and alert to monitor the values in a Delta Lake table. The recent_sensor_recordings table contains an identifying sensor_id alongside the timestamp and temperature for the most recent 5 minutes of recordings.The below query is used to create the alert:The query is set to refresh each minute and always completes in less than 10 seconds. The alert is set to trigger when mean (temperature) > 120. Notifications are triggered to be sent at most every 1 minute.If this alert raises notifications for 3 consecutive minutes and then stops, which statement must be true?

- A.The total average temperature across all sensors exceeded 120 on three consecutive executions of the query
- B.The recent_sensor_recordings table was unresponsive for three consecutive runs of the query
- C.The source query failed to update properly for three consecutive minutes and then restarted
- D.The maximum temperature recording for at least one sensor exceeded 120 on three consecutive executions of the query
- E.The average temperature recordings for at least one sensor exceeded 120 on three consecutive executions of the query

---

### Question 5

A junior developer complains that the code in their notebook isn't producing the correct results in the development environment. A shared screenshot reveals that while they're using a notebook versioned with Databricks Repos, they're using a personal branch that contains old logic. The desired branch named dev-2.3.9 is not available from the branch selection dropdown.Which approach will allow this developer to review the current logic for this notebook?

- A.Use Repos to make a pull request use the Databricks REST API to update the current branch to dev-2.3.9
- B.Use Repos to pull changes from the remote Git repository and select the dev-2.3.9 branch.
- C.Use Repos to checkout the dev-2.3.9 branch and auto-resolve conflicts with the current branch
- D.Merge all changes back to the main branch in the remote Git repository and clone the repo again
- E.Use Repos to merge the current branch and the dev-2.3.9 branch, then make a pull request to sync with the remote repository

---

### Question 6

![Question 6 Image 1](examtopics_images/question_6_img_1.png)

The security team is exploring whether or not the Databricks secrets module can be leveraged for connecting to an external database.After testing the code with all Python variables being defined with strings, they upload the password to the secrets module and configure the correct permissions for the currently active user. They then modify their code to the following (leaving all other variables unchanged).Which statement describes what will happen when the above code is executed?

- A.The connection to the external table will fail; the string "REDACTED" will be printed.
- B.An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the encoded password will be saved to DBFS.
- C.An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the password will be printed in plain text.
- D.The connection to the external table will succeed; the string value of password will be printed in plain text.
- E.The connection to the external table will succeed; the string "REDACTED" will be printed.

---

### Question 7

![Question 7 Image 1](examtopics_images/question_7_img_1.png)

The data science team has created and logged a production model using MLflow. The following code correctly imports and applies the production model to output the predictions as a new DataFrame named preds with the schema "customer_id LONG, predictions DOUBLE, date DATE".The data science team would like predictions saved to a Delta Lake table with the ability to compare all predictions across time. Churn predictions will be made at most once per day.Which code block accomplishes this task while minimizing potential compute costs?

- A.preds.write.mode("append").saveAsTable("churn_preds")
- B.preds.write.format("delta").save("/preds/churn_preds")
- C.
- D.
- E.
- ![alt text](examtopics_images/question_7_img_2.png)

---

### Question 8

![Question 8 Image 1](examtopics_images/question_8_img_1.png)

An upstream source writes Parquet data as hourly batches to directories named with the current date. A nightly batch job runs the following code to ingest all data from the previous day as indicated by the date variable:Assume that the fields customer_id and order_id serve as a composite key to uniquely identify each order.If the upstream system is known to occasionally produce duplicate entries for a single order hours apart, which statement is correct?

- A.Each write to the orders table will only contain unique records, and only those records without duplicates in the target table will be written.
- B.Each write to the orders table will only contain unique records, but newly written records may have duplicates already present in the target table.
- C.Each write to the orders table will only contain unique records; if existing records with the same key are present in the target table, these records will be overwritten.
- D.Each write to the orders table will only contain unique records; if existing records with the same key are present in the target table, the operation will fail.
- E.Each write to the orders table will run deduplication over the union of new and existing records, ensuring no duplicate records are present.

---

### Question 9

![Question 9 Image 1](examtopics_images/question_9_img_1.png)

A junior member of the data engineering team is exploring the language interoperability of Databricks notebooks. The intended outcome of the below code is to register a view of all sales that occurred in countries on the continent of Africa that appear in the geo_lookup table.Before executing the code, running SHOW TABLES on the current database indicates the database contains only two tables: geo_lookup and sales.Which statement correctly describes the outcome of executing these command cells in order in an interactive notebook?

- A.Both commands will succeed. Executing show tables will show that countries_af and sales_af have been registered as views.
- B.Cmd 1 will succeed. Cmd 2 will search all accessible databases for a table or view named countries_af: if this entity exists, Cmd 2 will succeed.
- C.Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable representing a PySpark DataFrame.
- D.Both commands will fail. No new variables, tables, or views will be created.
- E.Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable containing a list of strings.

---

### Question 10

A Delta table of weather records is partitioned by date and has the below schema: date DATE, device_id INT, temp FLOAT, latitude FLOAT, longitude FLOATTo find all the records from within the Arctic Circle, you execute a query with the below filter: latitude > 66.3Which statement describes how the Delta engine identifies which files to load?

- A.All records are cached to an operational database and then the filter is applied
- B.The Parquet file footers are scanned for min and max statistics for the latitude column
- C.All records are cached to attached storage and then the filter is applied
- D.The Delta log is scanned for min and max statistics for the latitude column
- E.The Hive metastore is scanned for min and max statistics for the latitude column

---

### Question 11

The data engineering team has configured a job to process customer requests to be forgotten (have their data deleted). All user data that needs to be deleted is stored in Delta Lake tables using default table settings.The team has decided to process all deletions from the previous week as a batch job at 1am each Sunday. The total duration of this job is less than one hour. Every Monday at 3am, a batch job executes a series of VACUUM commands on all Delta Lake tables throughout the organization.The compliance officer has recently learned about Delta Lake's time travel functionality. They are concerned that this might allow continued access to deleted data.Assuming all delete logic is correctly implemented, which statement correctly addresses this concern?

- A.Because the VACUUM command permanently deletes all files containing deleted records, deleted records may be accessible with time travel for around 24 hours.
- B.Because the default data retention threshold is 24 hours, data files containing deleted records will be retained until the VACUUM job is run the following day.
- C.Because Delta Lake time travel provides full access to the entire history of a table, deleted records can always be recreated by users with full admin privileges.
- D.Because Delta Lake's delete statements have ACID guarantees, deleted records will be permanently purged from all storage systems as soon as a delete job completes.
- E.Because the default data retention threshold is 7 days, data files containing deleted records will be retained until the VACUUM job is run 8 days later.

---

### Question 12

![Question 12 Image 1](examtopics_images/question_12_img_1.png)

A junior data engineer has configured a workload that posts the following JSON to the Databricks REST API endpoint 2.0/jobs/create.Assuming that all configurations and referenced resources are available, which statement describes the result of executing this workload three times?

- A.Three new jobs named "Ingest new data" will be defined in the workspace, and they will each run once daily.
- B.The logic defined in the referenced notebook will be executed three times on new clusters with the configurations of the provided cluster ID.
- C.Three new jobs named "Ingest new data" will be defined in the workspace, but no jobs will be executed.
- D.One new job named "Ingest new data" will be defined in the workspace, but it will not be executed.
- E.The logic defined in the referenced notebook will be executed three times on the referenced existing all purpose cluster.

---

### Question 13

An upstream system is emitting change data capture (CDC) logs that are being written to a cloud object storage directory. Each record in the log indicates the change type (insert, update, or delete) and the values for each field after the change. The source table has a primary key identified by the field pk_id.For auditing purposes, the data governance team wishes to maintain a full record of all values that have ever been valid in the source system. For analytical purposes, only the most recent value for each record needs to be recorded. The Databricks job to ingest these records occurs once per hour, but each individual record may have changed multiple times over the course of an hour.Which solution meets these requirements?

- A.Create a separate history table for each pk_id resolve the current state of the table by running a union all filtering the history tables for the most recent state.
- B.Use MERGE INTO to insert, update, or delete the most recent entry for each pk_id into a bronze table, then propagate all changes throughout the system.
- C.Iterate through an ordered set of changes to the table, applying each in turn; rely on Delta Lake's versioning ability to create an audit log.
- D.Use Delta Lake's change data feed to automatically process CDC data from an external system, propagating all changes to all dependent tables in the Lakehouse.
- E.Ingest all log information into a bronze table; use MERGE INTO to insert, update, or delete the most recent entry for each pk_id into a silver table to recreate the current table state.

The correct solution is  **E: Ingest all log information into a bronze table; use MERGE INTO to insert, update, or delete the most recent entry for each pk_id into a silver table to recreate the current table state.**

Explanation for Correct Answer

The requirement is twofold: maintain a  **full history**  of all changes (for auditing) and provide the  **most recent state**  of each record (for analytics).

-   **Bronze Table (Full History):**  By ingesting all raw CDC log information (inserts, updates, and deletes) into a Bronze table, you create a permanent, append-only record of every change that has ever occurred in the source system. This satisfies the data governance team's audit requirement.
-   **Silver Table (Current State):**  By using  `MERGE INTO`  with the  `pk_id`  to upsert only the  **most recent**  version of each record into a Silver table, you recreate the current state of the source table. This satisfies the analytical requirement for current data.
-   **Handling Multiple Changes per Hour:**  Since a record may change multiple times within an hour, the ingest process must identify the latest state for each  `pk_id`  within that batch before merging it into the Silver table to ensure only the final state is preserved.

Why Other Options Are Incorrect

-   **A: History table per  `pk_id`:**  Creating a separate table for every individual primary key is architecturally impossible and would result in millions of tables, leading to a "small files" problem and total system failure.
-   **B: MERGE INTO a bronze table:**  The Bronze layer should be a raw landing zone for all data. Applying  `MERGE`  at this stage (which overwrites or deletes data) would destroy the historical record needed for auditing.
-   **C: Relying on Delta versioning:**  While Delta Lake keeps versioned history, it is designed for point-in-time recovery and time travel, not as a primary auditing solution for high-frequency CDC changes. Relying solely on this would make long-term auditing difficult as versions may be purged by  `VACUUM`.
-   **D: Use Change Data Feed (CDF) for external data:**  Change Data Feed is a feature that exposes changes  _within_  a Delta table to downstream consumers. It does not "automatically" process logs from an external cloud object storage; you must still write logic to ingest that external data into the Lakehouse first.

---

### Question 14

An hourly batch job is configured to ingest data files from a cloud object storage container where each batch represent all records produced by the source system in a given hour. The batch job to process these records into the Lakehouse is sufficiently delayed to ensure no late-arriving data is missed. The user_id field represents a unique key for the data, which has the following schema: user_id BIGINT, username STRING, user_utc STRING, user_region STRING, last_login BIGINT, auto_pay BOOLEAN, last_updated BIGINTNew records are all ingested into a table named account_history which maintains a full record of all data in the same schema as the source. The next table in the system is named account_current and is implemented as a Type 1 table representing the most recent value for each unique user_id.Assuming there are millions of user accounts and tens of thousands of records processed hourly, which implementation can be used to efficiently update the described account_current table as part of each hourly batch job?

- A.Use Auto Loader to subscribe to new files in the account_history directory; configure a Structured Streaming trigger once job to batch update newly detected files into the account_current table.
- B.Overwrite the account_current table with each batch using the results of a query against the account_history table grouping by user_id and filtering for the max value of last_updated.
- C.Filter records in account_history using the last_updated field and the most recent hour processed, as well as the max last_iogin by user_id write a merge statement to update or insert the most recent value for each user_id.
- D.Use Delta Lake version history to get the difference between the latest version of account_history and one version prior, then write these records to account_current.
- E.Filter records in account_history using the last_updated field and the most recent hour processed, making sure to deduplicate on username; write a merge statement to update or insert the most recent value for each username.

The correct solution is

**C: Filter records in account\_history using the last\_updated field and the most recent hour processed, as well as the max last\_updated by user\_id; write a merge statement to update or insert the most recent value for each user\_id.** 

Explanation for Correct Answer 

To efficiently update a Type 1 table (`account_current`) from a history table (`account_history`) in a batch context, you must minimize the volume of data scanned and processed: 

- **Filtering by `last_updated`:** By filtering for records updated within the most recent hour (the current batch), you avoid scanning millions of historical records in `account_history`.
- **Intra-batch Deduplication:** Since a `user_id` might have multiple changes within a single hour, you must find the `max(last_updated)` for each `user_id` within that specific batch to ensure only the latest state is merged.
- **`MERGE INTO`:** This is the standard Delta Lake operation for Type 1 tables. It allows you to update existing users with their latest information or insert new users if the `user_id` does not yet exist. 

Why Other Options Are Incorrect 

- **A: Auto Loader to batch update:** While Auto Loader is efficient for ingestion, using it to trigger a update from the _directory_ of a table that is already managed (`account_history`) is redundant and less efficient than querying the table directly using Delta's metadata.
- **B: Overwrite the entire table:** "Millions of user accounts" makes a full overwrite every hour extremely inefficient. As the dataset grows, the time and cost of rewriting the entire `account_current` table will become unsustainable.
- **D: Delta Version History:** Delta versioning tracks file-level changes. Comparing two versions of the history table would provide the changes, but it doesn't account for the logic needed to handle records that may have changed multiple times across several versions, nor is it a standard pattern for propagating CDC data.
- **E: Deduplicate on `username`:** The prompt identifies `user_id` as the unique key. Deduplicating on `username` is incorrect because usernames can change (e.g., a marriage or preference change), whereas the primary key (`user_id`) remains constant. Using the wrong key would lead to data corruption.

---

### Question 15

A table in the Lakehouse named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.The churn prediction model used by the ML team is fairly stable in production. The team is only interested in making predictions on records that have changed in the past 24 hours.Which approach would simplify the identification of these changed records?

- A.Apply the churn model to all rows in the customer_churn_params table, but implement logic to perform an upsert into the predictions table that ignores rows where predictions have not changed.
- B.Convert the batch job to a Structured Streaming job using the complete output mode; configure a Structured Streaming job to read from the customer_churn_params table and incrementally predict against the churn model.
- C.Calculate the difference between the previous model predictions and the current customer_churn_params on a key identifying unique customers before making new predictions; only make predictions on those customers not in the previous predictions.
- D.Modify the overwrite logic to include a field populated by calling spark.sql.functions.current_timestamp() as data are being written; use this field to identify records written on a particular date.
- E.Replace the current overwrite logic with a merge statement to modify only those records that have changed; write logic to make predictions on the changed records identified by the change data feed.

The correct solution is

**E: Replace the current overwrite logic with a merge statement to modify only those records that have changed; write logic to make predictions on the changed records identified by the change data feed.** 

Explanation for Correct Answer 

To identify only the records that have changed in the past 24 hours, the system needs a way to distinguish between "newly updated" and "unchanged" data at the row level. 

-   **Merge Statement:** By switching from a full table overwrite to a `MERGE` statement, the data engineering team ensures that only records with actual differences are updated or inserted. This preserves the state of unchanged rows.
-   **Change Data Feed (CDF):** Once CDF is enabled on the Delta table, it automatically captures row-level changes (inserts and updates) with associated metadata, such as the commit timestamp or version.
-   **Simplified Identification:** The ML team can then query the change feed directly (e.g., using the `table_changes` function) for the specific time range of the last 24 hours. This allows them to run their churn prediction model only on the precise set of records that were modified, rather than re-processing the entire table. 

Why Other Options Are Incorrect 

-   **A: Upsert into predictions table:** This approach still requires the ML model to run on **all** rows in the large `customer_churn_params` table just to determine if the result has changed, which does not solve the requirement of only making predictions on changed records.
-   **B: Structured Streaming with complete mode:** Complete mode re-writes the entire result table every time, which is the opposite of incremental processing. Converting a stable nightly batch job to a streaming job adds unnecessary complexity without providing the row-level filtering needed.
-   **C: Calculate differences manually:** Manually calculating the difference between previous predictions and current data is computationally expensive and complex at scale. It also relies on the prediction outcome rather than the underlying data changes.
-   **D: Overwrite with a timestamp field:** Because the current process **overwrites** the entire table, calling `current_timestamp()` would give every single row the exact same timestamp (the time of the overwrite). This would make it impossible to identify which records actually changed in the source system compared to the previous day. 

[](https://labs.google.com/search/experiment/22)

Creating a public link...

---

### Question 16

![Question 16 Image 1](examtopics_images/question_16_img_1.png)

A table is registered with the following code:Both users and orders are Delta Lake tables. Which statement describes the results of querying recent_orders?

- A.All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query finishes.
- B.All logic will execute when the table is defined and store the result of joining tables to the DBFS; this stored data will be returned when the table is queried.
- C.Results will be computed and cached when the table is defined; these cached results will incrementally update as new records are inserted into source tables.
- D.All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query began.
- E.The versions of each source table will be stored in the table transaction log; query results will be saved to DBFS with each query.

Answer:

**B** 

Explanation for Correct Answer 

The `CREATE TABLE ... AS SELECT` (CTAS) statement is a Data Definition Language (DDL) operation that physically creates a new table and populates it with the results of the query **at the time the statement is executed**. It is a snapshot of the data. 

-   **B. All logic will execute when the table is defined and store the result of joining tables to the DBFS, this stored data will be returned when the table is queried.** This statement accurately describes the behavior. The data is computed once during creation and stored as physical files (typically Parquet files with Delta metadata). Subsequent queries of `recent_orders` simply read these static stored files, without re-running the join logic or recalculating the `current_date()-7` filter. 

Why Other Options Are Incorrect 

-   **A/D. All logic will execute at query time...** These statements describe the behavior of a SQL **VIEW**, which is a virtual table definition that re-runs the underlying query every time it is accessed. A physical `CREATE TABLE AS` does not work this way.
-   **C. Results will be computed and cached when the table is defined; these cached results will incrementally update...** Caching may happen for performance, but the results do not incrementally update automatically as new data arrives in the source tables (`users` and `orders`). The table is static until explicitly refreshed or recreated.
-   **E. The versions of each source table will be stored in the table transaction log; query results will be saved to DBFS with each query.** The transaction log stores changes to the *current* table, not all source table versions indefinitely. Query results are saved *once* during definition, not with each subsequent query. 

---

### Question 17

A production workload incrementally applies updates from an external Change Data Capture feed to a Delta Lake table as an always-on Structured Stream job. When data was initially migrated for this table, OPTIMIZE was executed and most data files were resized to 1 GB. Auto Optimize and Auto Compaction were both turned on for the streaming production job. Recent review of data files shows that most data files are under 64 MB, although each partition in the table contains at least 1 GB of data and the total table size is over 10 TB.Which of the following likely explains these smaller file sizes?

- A.Databricks has autotuned to a smaller target file size to reduce duration of MERGE operations
- B.Z-order indices calculated on the table are preventing file compaction
- C.Bloom filter indices calculated on the table are preventing file compaction
- D.Databricks has autotuned to a smaller target file size based on the overall size of data in the table
- E.Databricks has autotuned to a smaller target file size based on the amount of data in each partition

The likely explanation for the smaller file sizes is that

**A: Databricks has autotuned to a smaller target file size to reduce the duration of `MERGE` operations**. 

Explanation for Correct Answer 

The workload involves an always-on Structured Stream job that incrementally applies updates using `MERGE` operations. `MERGE` operations typically rewrite existing data files that contain matching records. 

-   **Workload Autotuning:** Databricks automatically detects merge-heavy workloads and dynamically tunes the target file size to a smaller value (often in the 16MB to 64MB range).
-   **Performance Benefit:** Smaller files mean that `MERGE` operations have to read and rewrite less data per change, thereby reducing the duration and overhead of each transaction, which is critical for the low-latency requirements of a continuous streaming job.
-   **Initial `OPTIMIZE` vs. Streaming Behavior:** The initial `OPTIMIZE` to 1GB files was a one-time operation for analytical read performance. The ongoing streaming process with auto-tuning prioritizes write (merge) performance for the continuous updates. 

Why Other Options Are Incorrect 

-   **B/C. Z-order/Bloom filter indices:** These indices are for optimizing read performance and data skipping; they do not prevent file compaction or influence the target file size used by Auto Optimize.
-   **D. Autotuned based on overall table size:** While Databricks does autotune file size based on total table size for non-merge-intensive tables (aiming for 1GB files for tables over 10TB), the workload-specific tuning for frequent merges overrides this general rule and targets smaller files.
-   **E. Autotuned based on the amount of data in each partition:** Auto compaction does tune file sizes based on the data volume written in micro-batches within a partition, which naturally results in smaller files for smaller batches. However, the core reason the *target* size has shifted from 1GB down to below 64MB, despite partitions having plenty of data, is the specific optimization for the high-frequency `MERGE` workload type. 


---

### Question 18

Which statement regarding stream-static joins and static Delta tables is correct?

- A.Each microbatch of a stream-static join will use the most recent version of the static Delta table as of each microbatch.
- B.Each microbatch of a stream-static join will use the most recent version of the static Delta table as of the job's initialization.
- C.The checkpoint directory will be used to track state information for the unique keys present in the join.
- D.Stream-static joins cannot use static Delta tables because of consistency issues.
- E.The checkpoint directory will be used to track updates to the static Delta table.

The correct statement is

**A:** ==**Each microbatch of a stream-static join will use the most recent version of the static Delta table as of each microbatch**==**.** 

Explanation for Correct Answer 

A stream-static join in Databricks Structured Streaming is a stateless operation that joins a continuous stream of data with a static (slowly changing) Delta table. 

-   **A. Each microbatch of a stream-static join will use the most recent version of the static Delta table as of each microbatch.** The [Databricks documentation](https://community.databricks.com/t5/data-engineering/stream-static-join/td-p/125477) confirms that when processing a micro-batch of data, the latest valid version of the static Delta table is used for the join. This allows the streaming results to stay up-to-date with the reference data in near real-time, without requiring complex state management or watermarking. 

Why Other Options Are Incorrect 

-   **B. Each microbatch of a stream-static join will use the most recent version of the static Delta table as of the job's initialization.** This is a common misconception. While this might be the behavior for general static data sources in Spark (like a static file source that doesn't track appends), Delta tables are specifically designed to provide the latest snapshot for each microbatch in a stream-static join context.
-   **C. The checkpoint directory will be used to track state information for the unique keys present in the join.** Stream-static joins are inherently stateless and do not maintain join state in the checkpoint directory. Only the streaming side's progress is tracked.
-   **D. Stream-static joins cannot use static Delta tables because of consistency issues.** Stream-static joins are a recommended and supported pattern for joining streams with static Delta tables in [Databricks](https://www.databricks.com/).
-   **E. The checkpoint directory will be used to track updates to the static Delta table.** The checkpoint directory tracks the *progress* of the streaming query, not changes or updates to the static table itself. The static table's versioning is handled internally by Delta Lake for each microbatch read


---

### Question 19

![Question 19 Image 1](examtopics_images/question_19_img_1.png)

A junior data engineer has been asked to develop a streaming data pipeline with a grouped aggregation using DataFrame df. The pipeline needs to calculate the average humidity and average temperature for each non-overlapping five-minute interval. Events are recorded once per minute per device.Streaming DataFrame df has the following schema:"device_id INT, event_time TIMESTAMP, temp FLOAT, humidity FLOAT"Code block:Choose the response that correctly fills in the blank within the code block to complete this task.

- A.to_interval("event_time", "5 minutes").alias("time")
- B.window("event_time", "5 minutes").alias("time")
- C."event_time"
- D.window("event_time", "10 minutes").alias("time")
- E.lag("event_time", "10 minutes").alias("time")

The correct answer to fill in the blank is

==**B: window("event_time", "5 minutes") alias("time")**.==

Explanation for Correct Answer 

The user needs to calculate the average humidity and temperature for "each non-overlapping five-minute interval". This specific type of time-based aggregation in Structured Streaming requires the use of the `window()` function in conjunction with `groupBy()`. 

- **`window()` function:** This function is used to bucket records into defined time windows based on an event-time column (`event_time`).
- **"5 minutes" duration:** Specifying "5 minutes" creates _tumbling_ windows by default, which are non-overlapping intervals of exactly that duration.
- **`alias("time")`:** This renames the resulting structural column representing the start and end of the window to "time", matching the code snippet's structure. 

The complete code block would look like this: 

```
df.withWatermark("event_time", "10 minutes")
  .groupBy(
    window("event_time", "5 minutes").alias("time"),
    "deviceId"
  )
  .agg(
    avg("temp").alias("avg_temp"),
    avg("humidity").alias("avg_humidity")
  )
  .writeStream
  .format("delta")
  .saveAsTable("sensor_avg")
```

Why Other Options Are Incorrect 

- **A: `to_interval("event_time", "5 minutes")`:** The `to_interval` function is used for casting a column to an interval data type, not for performing event-time windowed aggregations in Structured Streaming.
- **C: `"event_time"`:** Grouping just by the raw `"event_time"` column without a window function would perform an aggregation for every single unique timestamp in the stream, which is not what is requested (non-overlapping 5-minute _intervals_).
- **D: `window("event_time", "10 minutes")`:** This would create 10-minute windows, which violates the requirement for 5-minute intervals.
- **E: `lag("event_time", "10 minutes")`:** The `lag()` window function is used to access a row from an earlier point in the stream or table, typically used for sessionization or time-difference calculations, not for defining fixed-interval tumbling windows for aggregation.


---

### Question 20

![Question 20 Image 1](examtopics_images/question_20_img_1.png)

A data architect has designed a system in which two Structured Streaming jobs will concurrently write to a single bronze Delta table. Each job is subscribing to a different topic from an Apache Kafka source, but they will write data with the same schema. To keep the directory structure simple, a data engineer has decided to nest a checkpoint directory to be shared by both streams.The proposed directory structure is displayed below:Which statement describes whether this checkpoint directory structure is valid for the given scenario and why?

- A.No; Delta Lake manages streaming checkpoints in the transaction log.
- B.Yes; both of the streams can share a single checkpoint directory.
- C.No; only one stream can write to a Delta Lake table.
- D.Yes; Delta Lake supports infinite concurrent writers.
- E.No; each of the streams needs to have its own checkpoint directory.

The correct answer is ==**E**==. 

Explanation for Correct Answer 

The checkpoint directory in Structured Streaming is crucial for maintaining fault tolerance, tracking the processed data (offsets), and preserving the state of a single streaming query. It serves as the unique identity and progress marker for that specific stream. 

-   **E. No; each of the streams needs to have its own checkpoint directory.** This is the correct statement. If two separate streaming queries share the same checkpoint directory, they will corrupt each other's state information, leading to data inconsistency, processing errors, and job failures. Even though they are writing to the same Delta table (which supports multiple concurrent writers), the stream *metadata* tracking must remain independent. To run these two Kafka topics concurrently and write to one Delta table, the streams must be configured with separate, unique checkpoint locations. 

Why Other Options Are Incorrect 

-   **A. No; Delta Lake manages streaming checkpoints in the transaction log.** Structured Streaming checkpoints are managed in a separate directory (`_checkpoint`), not directly within the Delta table's `_delta_log` transaction log.
-   **B. Yes; both of the streams can share a single checkpoint directory.** This is incorrect, as sharing a checkpoint directory between two distinct streams causes state conflicts and data corruption.
-   **C. No; only one stream can write to a Delta Lake table.** This is incorrect. Delta Lake is designed to support multiple concurrent writers to the same table by using optimistic concurrency control.
-   **D. Yes; Delta Lake supports infinite concurrent writers.** While Delta Lake supports robust concurrency, this option doesn't address the critical requirement that each concurrent *stream* must have a unique checkpoint directory for proper state management

---

### Question 21

A Structured Streaming job deployed to production has been experiencing delays during peak hours of the day. At present, during normal execution, each microbatch of data is processed in less than 3 seconds. During peak hours of the day, execution time for each microbatch becomes very inconsistent, sometimes exceeding 30 seconds. The streaming write is currently configured with a trigger interval of 10 seconds.Holding all other variables constant and assuming records need to be processed in less than 10 seconds, which adjustment will meet the requirement?

- A.Decrease the trigger interval to 5 seconds; triggering batches more frequently allows idle executors to begin processing the next batch while longer running tasks from previous batches finish.
- B.Increase the trigger interval to 30 seconds; setting the trigger interval near the maximum execution time observed for each batch is always best practice to ensure no records are dropped.
- C.The trigger interval cannot be modified without modifying the checkpoint directory; to maintain the current stream state, increase the number of shuffle partitions to maximize parallelism.
- D.Use the trigger once option and configure a Databricks job to execute the query every 10 seconds; this ensures all backlogged records are processed with each batch.
- E.Decrease the trigger interval to 5 seconds; triggering batches more frequently may prevent records from backing up and large batches from causing spill.

The correct answer is ==**E**==. 

Explanation for Correct Answer  

**E. Decrease the trigger interval to 5 seconds; triggering batches more frequently may prevent records from backing up and large batches from causing spill.**  

- In Spark Structured Streaming, each query processes **one microbatch at a time**; the next trigger only starts once the previous batch has finished. The trigger interval controls **how often** Spark *attempts* to start a new batch and therefore indirectly controls the **amount of data per batch**.
- With a 10-second trigger during peak hours, significantly more data can accumulate between triggers, causing large, uneven microbatches. These larger batches can lead to longer processing times (for example, >30 seconds), spill, and skewed performance.
- Reducing the trigger interval from 10 seconds to 5 seconds cuts the amount of data per batch roughly in half (assuming steady input rate), which typically results in **smaller, more predictable batch sizes** and helps keep per-batch processing time under the 10-second SLA.
- Databricks documentation notes that you can tune **trigger intervals** and **batch size** (for example, with max offsets/files/bytes per batch) to control latency and backlog in Structured Streaming workloads (see Databricks docs: *Structured Streaming concepts* and *Configure Structured Streaming trigger intervals / batch size*).

Why Other Options Are Incorrect  

- **A. Decrease the trigger interval to 5 seconds; triggering batches more frequently allows idle executors to begin processing the next batch while longer running tasks from previous batches finish.**  
  - The configuration change (shorter trigger interval) is reasonable, but the **reasoning is wrong**. Structured Streaming does **not** run multiple microbatches of the same query concurrently; it processes one batch at a time. Executors do not start the “next batch” while a previous batch is still running. The benefit comes from **smaller batches and reduced backlog**, not overlapping batch execution.  
- **B. Increase the trigger interval to 30 seconds; setting the trigger interval near the maximum execution time observed for each batch is always best practice to ensure no records are dropped.**  
  - Increasing the trigger interval **increases** the amount of data per batch, usually **making batch durations longer and more variable**, which worsens the observed problem.  
  - Records are not “dropped” when processing time exceeds the trigger interval; subsequent triggers are simply delayed. Best practice is to **reduce per-batch work or scale compute**, not to enlarge batches to the worst-case duration.  
- **C. The trigger interval cannot be modified without modifying the checkpoint directory; to maintain the current stream state, increase the number of shuffle partitions to maximize parallelism.**  
  - Databricks and Spark allow you to **change the trigger interval without changing the checkpoint location**; the checkpoint tracks offsets and state, not timing configuration.  
  - Increasing shuffle partitions may sometimes help parallelism, but it does not directly address the core problem of **large, bursty microbatches** causing 30+ second processing times.  
- **D. Use the trigger once option and configure a Databricks job to execute the query every 10 seconds; this ensures all backlogged records are processed with each batch.**  
  - `trigger(once=True)` is intended for **batch-like runs** that process all available data and then stop. Running this every 10 seconds via a job would repeatedly spin up new executions that read all backlog, adding overhead and **not** providing continuous low-latency streaming.  
  - This pattern complicates fault tolerance and state handling, and is contrary to the normal always-on streaming design described in Databricks Structured Streaming docs.

Key Takeaways (Interview-Ready)  

- Structured Streaming microbatches for a given query run **sequentially**, not concurrently; only one batch executes at a time.
- The **trigger interval** primarily controls **how much data accumulates per microbatch**; shorter intervals generally mean smaller batches and more stable latencies, assuming adequate cluster resources.
- To meet a latency SLA (for example, <10 seconds), you typically **reduce trigger interval and/or tune batch size** and scale compute to avoid large backlogs and spill.  
- You can **safely adjust trigger intervals** without recreating checkpoints; checkpoints store offsets/state, not scheduling configuration.  
- Databricks docs emphasize tuning **trigger interval** and **batch size** together to balance throughput and latency in production streaming jobs.


---

### Question 22

Which statement describes Delta Lake Auto Compaction?

- A.An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.
- B.Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.
- C.Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.
- D.Data is queued in a messaging bus instead of committing data directly to memory; all data is committed from the messaging bus in one batch once the job is complete.
- E.An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 128 MB.

The correct answer is ==**E**==. 

Explanation for Correct Answer 

Delta Lake **Auto Compaction** automatically combines small files within Delta table partitions to solve the "small file problem" and improve read performance. 

-   **E. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 128 MB.** This statement accurately describes the main characteristics of auto compaction: it runs after a write operation, in the background (asynchronously or as a post-commit hook on the same cluster), and aims for a default target file size of **128 MB**. This smaller size (compared to a manual `OPTIMIZE` command's 1 GB target) makes the background operation quick and less intrusive to concurrent workloads. 

Why Other Options Are Incorrect 

-   **A. An asynchronous job runs after the write completes... toward a default of 1 GB.** This is incorrect because the default target file size for auto compaction is 128 MB, not 1 GB. The 1 GB target is typically for manual `OPTIMIZE` commands.
-   **B. Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.** This describes a different, less common or legacy feature where optimization might be tied to cluster termination, not the general mechanism of Auto Compaction which triggers immediately after a successful write.
-   **C. Optimized writes use logical partitions instead of directory partitions...** This describes the separate feature called **Optimized Writes**, which optimizes file sizes *during* the write operation itself, often using an intelligent shuffle, not *after* the write is complete.
-   **D. Data is queued in a messaging bus...** This describes a data ingestion pattern using a messaging system like Kafka or event hubs, which is unrelated to how Delta Lake performs file maintenance or compaction.

---

### Question 23

Which statement characterizes the general programming model used by Spark Structured Streaming?

- A.Structured Streaming leverages the parallel processing of GPUs to achieve highly parallel data throughput.
- B.Structured Streaming is implemented as a messaging bus and is derived from Apache Kafka.
- C.Structured Streaming uses specialized hardware and I/O streams to achieve sub-second latency for data transfer.
- D.Structured Streaming models new data arriving in a data stream as new rows appended to an unbounded table.
- E.Structured Streaming relies on a distributed network of nodes that hold incremental state values for cached stages.

The correct answer is ==**D**==. 

Explanation for Correct Answer  

**D. Structured Streaming models new data arriving in a data stream as new rows appended to an unbounded table.**  

- The core programming model for Spark Structured Streaming is to treat a stream as an **unbounded table** to which new rows are continuously appended. Queries are expressed using the same Dataset/DataFrame (Spark SQL) APIs as batch jobs, and the engine **incrementally** updates the result as new data arrives.
- The official Spark Structured Streaming overview describes this explicitly: you “express your streaming computation the same way you would express a batch computation on static data,” and the engine runs it “incrementally and continuously” as data keeps arriving, conceptually viewing the input as a growing table of rows (unbounded table model).
- This abstraction lets you reuse SQL/DataFrame skills for streaming, and it underpins features like event-time windows, aggregations, joins, and exactly-once semantics, as documented in the Spark Structured Streaming Programming Guide and Databricks Structured Streaming concepts pages.

Why Other Options Are Incorrect  

- **A. Structured Streaming leverages the parallel processing of GPUs to achieve highly parallel data throughput.**  
  - Spark Structured Streaming is built on the **Spark SQL engine** and runs on standard CPU-based Spark clusters. While Spark can integrate with GPUs in some contexts, GPUs are **not** the defining programming model for Structured Streaming, and nothing in the core docs ties the model to GPU-specific execution.  
- **B. Structured Streaming is implemented as a messaging bus and is derived from Apache Kafka.**  
  - Structured Streaming is a **processing engine**, not a messaging system. It commonly **reads from** and **writes to** Kafka and other message buses, but it is implemented within Apache Spark, not “derived from Kafka” or acting as a bus itself.  
- **C. Structured Streaming uses specialized hardware and I/O streams to achieve sub-second latency for data transfer.**  
  - The docs emphasize a **software model**: micro-batch or continuous processing on the Spark SQL engine. Latency improvements come from engine design (micro-batches, continuous processing) and cluster sizing, not from any required “specialized hardware” or proprietary I/O devices.  
- **E. Structured Streaming relies on a distributed network of nodes that hold incremental state values for cached stages.**  
  - While streaming queries can maintain state (for example, for aggregations or joins) across distributed executors, the **programming model** is not described as “cached stages with incremental state values.” That phrasing confuses internal execution details with the external programming abstraction, which is the **unbounded table** view of a stream.

Key Takeaways (Interview-Ready)  

- Spark Structured Streaming’s fundamental abstraction is an **unbounded table**: new data is modeled as rows continuously appended to a logical table.
- Developers write streaming logic using the **same DataFrame/Dataset and SQL APIs** as batch jobs; the engine handles incremental, continuous execution.
- Structured Streaming is an **engine on top of Spark SQL**, not a messaging system and not inherently GPU- or hardware-specific.
- Concepts like **event-time windows, aggregations, joins, and exactly-once guarantees** are built on this unbounded-table model.
- Databricks and Apache Spark documentation both emphasize that the main benefit is reusing batch-style code while the engine transparently handles streaming semantics and fault tolerance.

---

### Question 24

Which configuration parameter directly affects the size of a spark-partition upon ingestion of data into Spark?

- A.spark.sql.files.maxPartitionBytes
- B.spark.sql.autoBroadcastJoinThreshold
- C.spark.sql.files.openCostInBytes
- D.spark.sql.adaptive.coalescePartitions.minPartitionNum
- E.spark.sql.adaptive.advisoryPartitionSizeInBytes

The correct answer is  **A.  `spark.sql.files.maxPartitionBytes`**.

**Why A is correct**

-   `spark.sql.files.maxPartitionBytes`  controls the  **target size (in bytes) of each input partition**  when Spark reads files. Larger value → fewer, bigger partitions; smaller value → more, smaller partitions. This directly affects the size of a Spark partition at ingestion.

**Why the others are not**

-   **B.  `spark.sql.autoBroadcastJoinThreshold`**  – controls when small datasets are broadcast in joins; it does  **not**  affect input partition size.
-   **C.  `spark.sql.files.openCostInBytes`**  – a cost heuristic to decide whether to split files; it influences planning but not the actual max bytes per partition.
-   **D.  `spark.sql.adaptive.coalescePartitions.minPartitionNum`**  – used by Adaptive Query Execution to coalesce  **shuffle**  partitions at runtime, not file-ingestion partitions.
-   **E.  `spark.sql.adaptive.advisoryPartitionSizeInBytes`**  – an advisory size for  **shuffle**  partitions under AQE, again not the initial file-read partition size.

**Key takeaways (interview-ready)**

-   Input file partition size on read is mainly controlled by  **`spark.sql.files.maxPartitionBytes`**.
-   AQE settings (`spark.sql.adaptive.*`) tune  **post-shuffle**  partitions, not the first-stage partitions created during file ingestion.
-   Join and broadcast thresholds (`autoBroadcastJoinThreshold`) are orthogonal to partition sizing.

---

### Question 25

A Spark job is taking longer than expected. Using the Spark UI, a data engineer notes that the Min, Median, and Max Durations for tasks in a particular stage show the minimum and median time to complete a task as roughly the same, but the max duration for a task to be roughly 100 times as long as the minimum.Which situation is causing increased duration of the overall job?

- A.Task queueing resulting from improper thread pool assignment.
- B.Spill resulting from attached volume storage being too small.
- C.Network latency due to some cluster nodes being in different regions from the source data
- D.Skew caused by more data being assigned to a subset of spark-partitions.
- E.Credential validation errors while pulling data from an external system.

The correct answer is ==**D**==.

**Explanation for Correct Answer**

**D. Skew caused by more data being assigned to a subset of spark-partitions.**

-   When  **Min**  and  **Median**  task durations are roughly the same, but the  **Max**  duration is ~100x larger, it indicates that  **most tasks finish quickly**, while  **a few tasks take a very long time**.
-   This is the classic symptom of  **data skew**: some partitions contain  _far more data_  (or “hot” keys) than others, so the tasks processing those partitions run much longer and delay completion of the entire stage.
-   In the Spark UI (Stages tab), skew appears exactly like this: a tight cluster of short-running tasks plus a tiny number of very long tasks. The long tasks become the bottleneck and are what cause the overall job to run slowly.

**Why Other Options Are Incorrect**

-   **A. Task queueing resulting from improper thread pool assignment.**
    -   Thread-pool or executor queueing issues typically impact  **many**  tasks and cause more uniform slowdowns, not just a few extreme outliers with 100x longer duration compared to min/median.
-   **B. Spill resulting from attached volume storage being too small.**
    -   Severe spill (due to insufficient memory or disk) often affects a broad set of tasks that hit the same memory limits, leading to widespread longer durations. You would usually see many tasks slowed, not just a tiny subset with extreme durations.
-   **C. Network latency due to some cluster nodes being in different regions from the source data.**
    -   Cross-region or network issues generally show up as  **systematic**  slowness for all tasks on affected executors, not as a sharp disparity where only a handful of tasks are 100x slower than the rest while min and median remain similar.
-   **E. Credential validation errors while pulling data from an external system.**
    -   Authentication or credential issues usually cause  **task failures/retries**  or clear error messages, rather than just a single or small set of tasks taking vastly longer while others complete normally.

**Key Takeaways (Interview-Ready)**

-   A  **huge gap**  between max task duration and min/median (with min≈median) is a strong indicator of  **data skew**  in Spark.
-   Data skew occurs when  **some partitions or keys contain much more data**  than others, creating straggler tasks that dominate stage runtime.
-   Mitigation strategies include  **repartitioning**,  **salting hot keys**, using  **skew hints/optimizations on joins**, or redesigning keys/partitioning schemes.

---

### Question 26

Each configuration below is identical to the extent that each cluster has 400 GB total of RAM, 160 total cores and only one Executor per VM.Given a job with at least one wide transformation, which of the following cluster configurations will result in maximum performance?

- A.• Total VMs; 1• 400 GB per Executor• 160 Cores / Executor
- B.• Total VMs: 8• 50 GB per Executor• 20 Cores / Executor
- C.• Total VMs: 16• 25 GB per Executor• 10 Cores/Executor
- D.• Total VMs: 4• 100 GB per Executor• 40 Cores/Executor
- E.• Total VMs:2• 200 GB per Executor• 80 Cores / Executor

The correct answer is  **==C==**.

**Explanation for Correct Answer**

**C. • Total VMs: 16 • 25 GB per Executor • 10 Cores / Executor**

For a Spark job with at least one  **wide transformation**  (shuffle), performance is heavily influenced by how well you can  **parallelize**  work and avoid bottlenecks inside a single executor:

-   With a fixed total of  **160 cores**  and  **400 GB RAM**, option C gives:
    -   **16 executors**  (one per VM)
    -   **10 cores per executor**  (16 × 10 = 160)
    -   **25 GB per executor**  (16 × 25 GB = 400 GB)
-   Compared with fewer, very large executors, having  **more executors with fewer cores each**  generally:
    -   Increases the number of tasks that can run in parallel across the cluster.
    -   Reduces GC pressure and contention within each JVM (less “oversized” executor).
    -   Avoids the “monolithic executor” anti-pattern (like 160 cores in a single JVM).
-   Spark best practices often recommend  **not**  using very high core counts per executor; moderate sizes (e.g., 5–10 cores/executor) strike a better balance between parallelism and GC/serialization overhead. Option C matches this pattern best given the choices.

**Why Other Options Are Incorrect**

-   **A. 1 VM, 400 GB, 160 Cores / Executor**
    -   Single executor with  **160 cores**  is a classic anti-pattern:
        -   Very high GC overhead in one JVM.
        -   Limited failure isolation (if that node/executor fails, everything is impacted).
        -   Poor task scheduling flexibility; you can’t spread work across machines.
-   **B. 8 VMs, 50 GB, 20 Cores / Executor**
    -   Better than A (more executors), but  **20 cores per executor**  is still relatively large:
        -   More GC pressure and thread contention per executor than in C.
        -   Fewer executors (8) → less granular parallelism than 16 in C.
-   **D. 4 VMs, 100 GB, 40 Cores / Executor**
    -   Very large executors (40 cores each):
        -   Similar issues to A and B but worse than B: heavy GC, fewer failure domains, coarse-grained parallelism.
-   **E. 2 VMs, 200 GB, 80 Cores / Executor**
    -   Only two executors with  **80 cores**  each:
        -   Strong risk of GC storms, long pauses, and poor utilization of cluster resources.
        -   Very limited parallelism across nodes; two big executors are far less flexible than 16 smaller ones.

**Key Takeaways (Interview-Ready)**

-   For Spark,  **more executors with fewer cores each**  is usually better than a few massive executors, given the same total cores/RAM.
-   Very large executors (dozens of cores) often suffer from  **GC issues, thread contention, and poor fault isolation**.
-   Jobs with  **wide transformations**  benefit from fine-grained parallelism and spreading shuffle work across more executors.
-   Typical guidance: keep executor core counts  **moderate**  (e.g., around 5–10 cores per executor), then scale out by adding more executors/VMs.

---

### Question 27

![Question 27 Image 1](examtopics_images/question_27_img_1.png)

A junior data engineer on your team has implemented the following code block.The view new_events contains a batch of records with the same schema as the events Delta table. The event_id field serves as a unique key for this table.When this query is executed, what will happen with new records that have the same event_id as an existing record?

- A.They are merged.
- B.They are ignored.
- C.They are updated.
- D.They are inserted.
- E.They are deleted.


**Correct answer: ✅ B. They are ignored.**

**Explanation**

The SQL shown is:

`MERGE  INTO events USING new_events ON events.event_id = new_events.event_id WHEN  NOT MATCHED INSERT  *` 

Key points:

-   `event_id` is the **unique key**
    
-   The `MERGE` statement has **only one clause**:
    
    -   `WHEN NOT MATCHED THEN INSERT`
        
-   There is **no `WHEN MATCHED` clause**
    

**What happens during execution?**

-   **If a record in `new_events` has an `event_id` that does NOT exist in `events`**  
    → it is **inserted**
    
-   **If a record in `new_events` has an `event_id` that already exists in `events`**  
    → it **matches**, but since **no `WHEN MATCHED` action is defined**, **nothing happens**
    

So those records are **silently ignored**.

**Why other options are incorrect**

-   **A. They are merged** ❌  
    A merge requires a `WHEN MATCHED` clause to define behavior.
    
-   **C. They are updated** ❌  
    No `UPDATE` clause exists.
    
-   **D. They are inserted** ❌  
    Inserts only occur for **NOT MATCHED** rows.
    
-   **E. They are deleted** ❌  
    No delete clause exists.
    

----------

**Interview-ready one-liner**

> In a Delta Lake `MERGE`, matched rows are ignored if no `WHEN MATCHED` clause is specified.

If you want, I can also explain:

-   How to implement **UPSERT**
    
-   How Delta handles **multiple matches**
    
-   Common **MERGE pitfalls** in Databricks certification questions

---

### Question 28

![Question 28 Image 1](examtopics_images/question_28_img_1.png)

A junior data engineer seeks to leverage Delta Lake's Change Data Feed functionality to create a Type 1 table representing all of the values that have ever been valid for all rows in a bronze table created with the property delta.enableChangeDataFeed = true. They plan to execute the following code as a daily job:Which statement describes the execution and results of running the above query multiple times?

- A.Each time the job is executed, newly updated records will be merged into the target table, overwriting previous values with the same primary keys.
- B.Each time the job is executed, the entire available history of inserted or updated records will be appended to the target table, resulting in many duplicate entries.
- C.Each time the job is executed, the target table will be overwritten using the entire history of inserted or updated records, giving the desired result.
- D.Each time the job is executed, the differences between the original and current versions are calculated; this may result in duplicate entries for some records.
- E.Each time the job is executed, only those records that have been inserted or updated since the last execution will be appended to the target table, giving the desired result.


The correct answer is **B**.

**Explanation for Correct Answer**

**B. Each time the job is executed, the entire available history of inserted or updated records will be appended to the target table, resulting in many duplicate entries.**

-   The code reads from the `bronze` Delta table using **Change Data Feed (CDF)** with  
    `option("startingVersion", 0)`.
    
-   This means **every execution re-reads the full change history from version 0 onward**, not just new changes.
    
-   The filter keeps only `_change_type` values `insert` and `update_postimage`, i.e., all inserts and the final images of updates.
    
-   The write operation uses **`.mode("append")`**, so Spark **always appends** rows to the target table `bronze_history_type1`.
    
-   There is **no checkpointing, no tracking of the last processed version, and no MERGE logic**.
    
-   As a result, **the same historical insert and update records are appended again on every run**, producing duplicates.
    

**Why Other Options Are Incorrect**

**A. Each time the job is executed, newly updated records will be merged into the target table, overwriting previous values with the same primary keys.**

-   Incorrect because there is **no `MERGE` operation**. The code performs a simple append.
    

**C. Each time the job is executed, the target table will be overwritten using the entire history of inserted or updated records.**

-   Incorrect because the write mode is **append**, not overwrite.
    

**D. Each time the job is executed, the differences between the original and current versions are calculated; this may result in duplicate entries for some records.**

-   Incorrect because CDF directly emits change rows; **no diff calculation** is being performed by this query.
    

**E. Each time the job is executed, only those records that have been inserted or updated since the last execution will be appended to the target table, giving the desired result.**

-   Incorrect because incremental behavior would require **checkpointing or dynamically tracking the last processed version**, which is not present.
    

**Key Takeaway (Interview-Ready)**

-   Using Delta Change Data Feed in **batch mode with a fixed `startingVersion` and append writes will reprocess history every run**.
    
-   To achieve true incremental Type 1 behavior, you must either:
    
    -   Track and update the last processed version manually, or
        
    -   Use **Structured Streaming with checkpoints**, or
        
    -   Apply a **MERGE-based upsert** instead of append.

---

### Question 29

A new data engineer notices that a critical field was omitted from an application that writes its Kafka source to Delta Lake. This happened even though the critical field was in the Kafka source. That field was further missing from data written to dependent, long-term storage. The retention threshold on the Kafka service is seven days. The pipeline has been in production for three months.Which describes how Delta Lake can help to avoid data loss of this nature in the future?

- A.The Delta log and Structured Streaming checkpoints record the full history of the Kafka producer.
- B.Delta Lake schema evolution can retroactively calculate the correct value for newly added fields, as long as the data was in the original source.
- C.Delta Lake automatically checks that all fields present in the source data are included in the ingestion layer.
- D.Data can never be permanently dropped or deleted from Delta Lake, so data loss is not possible under any circumstance.
- E.Ingesting all raw data and metadata from Kafka to a bronze Delta table creates a permanent, replayable history of the data state.

The correct answer is **E**.

**Explanation for Correct Answer**

**E. Ingesting all raw data and metadata from Kafka to a bronze Delta table creates a permanent, replayable history of the data state.**

-   Kafka has a **finite retention period** (7 days in this case). Once data ages out, it is **irretrievably lost** from Kafka.
    
-   Because the pipeline omitted a critical field at ingestion time, that field never made it into Delta Lake or downstream systems.
    
-   **Delta Lake itself cannot recover fields that were never written**, but it *can* prevent this type of loss in the future.
    
-   By ingesting **all raw Kafka data (entire value + metadata)** into a **bronze Delta table**, Delta Lake becomes the **system of record**.
    
-   Delta tables provide:
    
    -   Durable storage
        
    -   Time travel
        
    -   Full replayability for reprocessing with corrected logic
        
-   If a field is missed later, engineers can **reprocess historical Delta data**, even long after Kafka retention has expired.
    

**Why Other Options Are Incorrect**

**A. The Delta log and Structured Streaming checkpoints record the full history of the Kafka producer.**

-   Incorrect. Checkpoints track **streaming progress**, not the full Kafka producer history or dropped fields.
    

**B. Delta Lake schema evolution can retroactively calculate the correct value for newly added fields.**

-   Incorrect. Schema evolution only applies **at write time**. It cannot recover data that was never ingested.
    

**C. Delta Lake automatically checks that all fields present in the source data are included in the ingestion layer.**

-   Incorrect. Delta Lake does **not validate semantic completeness** of source schemas.
    

**D. Data can never be permanently dropped or deleted from Delta Lake.**

-   Incorrect. Data can be deleted, overwritten, vacuumed, or never written in the first place.
    

**Key Takeaway (Interview-Ready)**

-   Kafka is a **temporary buffer**, not a system of record.
    
-   Delta Lake bronze tables should capture **raw, immutable data**.
    
-   This design ensures **replayability and protection against upstream schema or logic bugs**, even months later.

---

### Question 30

![Question 30 Image 1](examtopics_images/question_30_img_1.png)

A nightly job ingests data into a Delta Lake table using the following code:The next step in the pipeline requires a function that returns an object that can be used to manipulate new records that have not yet been processed to the next table in the pipeline.Which code snippet completes this function definition?def new_records():

- A.return spark.readStream.table("bronze")
- B.return spark.readStream.load("bronze")
- C.
- D.return spark.read.option("readChangeFeed", "true").table ("bronze")
- E.
- ![alt text](examtopics_images/question_30_img_2.png)

The correct answer is **A**.

**Explanation for Correct Answer**

**A. `return spark.readStream.table("bronze")`**

-   The requirement is to return **an object that can be used to manipulate new records that have not yet been processed to the next table**.
    
-   In Delta Lake, **Structured Streaming** is the mechanism that tracks which records are new versus already processed, using **checkpointing**.
    
-   `spark.readStream.table("bronze")`:
    
    -   Treats the Delta table as a **streaming source**
        
    -   Automatically reads **only new appended data**
        
    -   Maintains state via checkpoints when written downstream
        
-   This is the **canonical and correct pattern** for incrementally processing newly arrived records from a Delta table.
    

**Why Other Options Are Incorrect**

**B. `return spark.readStream.load("bronze")`**

-   Incorrect because `load()` expects a **path**, not a table name.
    

**C. Filtering by `ingest_time == current_timestamp()`**

-   Incorrect because:
    
    -   `current_timestamp()` is evaluated at query runtime
        
    -   Timestamps will never exactly match
        
    -   This does not guarantee correctness or replayability
        

**D. Using `readChangeFeed`**

-   Incorrect in this context because:
    
    -   Change Data Feed is used for **change tracking**, not basic incremental consumption
        
    -   It requires additional configuration and is unnecessary for simple append-only ingestion
        

**E. Filtering by `source_file` path**

-   Incorrect because:
    
    -   File paths do not define processing state
        
    -   This approach is brittle and not scalable
        

**Key Takeaway (Interview-Ready)**

-   To process **only new records from a Delta table**, use **Structured Streaming**:
    
    `spark.readStream.table("table_name")` 
    
-   This ensures **exactly-once processing**, scalability, and correct state management without manual filtering.

---

### Question 31

A junior data engineer is working to implement logic for a Lakehouse table named silver_device_recordings. The source data contains 100 unique fields in a highly nested JSON structure.The silver_device_recordings table will be used downstream to power several production monitoring dashboards and a production model. At present, 45 of the 100 fields are being used in at least one of these applications.The data engineer is trying to determine the best approach for dealing with schema declaration given the highly-nested structure of the data and the numerous fields.Which of the following accurately presents information about Delta Lake and Databricks that may impact their decision-making process?

- A.The Tungsten encoding used by Databricks is optimized for storing string data; newly-added native support for querying JSON strings means that string types are always most efficient.
- B.Because Delta Lake uses Parquet for data storage, data types can be easily evolved by just modifying file footer information in place.
- C.Human labor in writing code is the largest cost associated with data engineering workloads; as such, automating table declaration logic should be a priority in all migration workloads.
- D.Because Databricks will infer schema using types that allow all observed data to be processed, setting types manually provides greater assurance of data quality enforcement.
- E.Schema inference and evolution on Databricks ensure that inferred types will always accurately match the data types used by downstream systems.

The correct answer is **D**.

**Explanation for Correct Answer**

**D. Because Databricks will infer schema using types that allow all observed data to be processed, setting types manually provides greater assurance of data quality enforcement.**

-   When Databricks infers schema from semi-structured or nested JSON data, it chooses **the most permissive data types** that can accommodate all observed values (for example, inferring `string` instead of numeric types when mixed values appear).
    
-   This behavior prioritizes **ingestion robustness**, not strict correctness.
    
-   For a **silver table** that powers:
    
    -   Production dashboards
        
    -   A production ML model  
        schema correctness and stability are critical.
        
-   Manually declaring schema:
    
    -   Enforces **data quality and contracts**
        
    -   Prevents silent type drift
        
    -   Produces more predictable downstream behavior
        
-   This is especially important when only **45 of 100 fields** are actually used—those fields should have **explicit, well-defined types**.
    

----------

**Why Other Options Are Incorrect**

**A. Tungsten encoding makes string types always most efficient.**

-   Incorrect. Columnar formats like Parquet + Tungsten are generally **more efficient with strongly typed, primitive columns** than with opaque JSON strings.
    

**B. Data types can be evolved by modifying Parquet footers in place.**

-   Incorrect. Parquet files are **immutable**; schema evolution requires writing new files.
    

**C. Human labor is always the largest cost, so automation should always be prioritized.**

-   Incorrect. This is a **business opinion**, not a technical truth, and ignores operational risk and data correctness requirements.
    

**E. Schema inference and evolution always match downstream data type needs.**

-   Incorrect. Inference optimizes for ingestion success, **not downstream semantic correctness**.
    

----------

**Key Takeaway (Interview-Ready)**

-   **Schema inference favors permissiveness, not correctness.**
    
-   For **production-grade silver tables**, explicitly defining schemas for critical fields is a best practice to ensure **data quality, stability, and downstream trust**.

---

### Question 32

![Question 32 Image 1](examtopics_images/question_32_img_1.png)

The data engineering team maintains the following code:Assuming that this code produces logically correct results and the data in the source tables has been de-duplicated and validated, which statement describes what will occur when this code is executed?

- A.A batch job will update the enriched_itemized_orders_by_account table, replacing only those rows that have different values than the current version of the table, using accountID as the primary key.
- B.The enriched_itemized_orders_by_account table will be overwritten using the current valid version of data in each of the three tables referenced in the join logic.
- C.An incremental job will leverage information in the state store to identify unjoined rows in the source tables and write these rows to the enriched_iteinized_orders_by_account table.
- D.An incremental job will detect if new rows have been written to any of the source tables; if new rows are detected, all results will be recalculated and used to overwrite the enriched_itemized_orders_by_account table.
- E.No computation will occur until enriched_itemized_orders_by_account is queried; upon query materialization, results will be calculated using the current valid version of data in each of the three tables referenced in the join logic.

The correct answer is **B**.

**Explanation for Correct Answer**

**B. The `enriched_itemized_orders_by_account` table will be overwritten using the current valid version of data in each of the three tables referenced in the join logic.**

-   The code uses **standard Spark batch DataFrame operations** (`spark.table`, `join`, `select`).
    
-   There is **no streaming**, **no checkpointing**, and **no state store** involved.
    
-   The final write uses:
    
    `finalDF.write
      .mode("overwrite")
      .table("enriched_itemized_orders_by_account")` 
    
-   `mode("overwrite")` means:
    
    -   The target table is **fully replaced**
        
    -   All results are **recomputed from scratch**
        
    -   The computation uses the **current snapshot** of:
        
        -   `accounts`
            
        -   `orders`
            
        -   `items`
            
-   Delta Lake does **not infer primary keys** and does not perform row-level updates unless explicitly coded (e.g., via `MERGE`).
    

This is a **full batch recomputation and overwrite pattern**.

**Why Other Options Are Incorrect**

**A. A batch job will update only changed rows using accountID as the primary key.**

-   Incorrect. Delta Lake does not infer primary keys, and no `MERGE` logic exists.
    

**C. An incremental job will use state to identify unjoined rows.**

-   Incorrect. State stores are only used in **Structured Streaming**, not batch jobs.
    

**D. An incremental job will detect new rows and recompute everything.**

-   Incorrect. There is no incremental detection logic here—this is a plain batch job.
    

**E. No computation occurs until the table is queried.**

-   Incorrect. This is not a view or lazy materialization; the `.write()` action **triggers immediate computation**.
    

**Key Takeaway (Interview-Ready)**

-   Spark batch jobs with `.mode("overwrite")` always perform a **full recomputation** using the **current snapshot of source tables**.
    
-   Incremental behavior requires **Structured Streaming, Change Data Feed, or MERGE-based logic**—none of which are present here.

---

### Question 33

The data engineering team is migrating an enterprise system with thousands of tables and views into the Lakehouse. They plan to implement the target architecture using a series of bronze, silver, and gold tables. Bronze tables will almost exclusively be used by production data engineering workloads, while silver tables will be used to support both data engineering and machine learning workloads. Gold tables will largely serve business intelligence and reporting purposes. While personal identifying information (PII) exists in all tiers of data, pseudonymization and anonymization rules are in place for all data at the silver and gold levels.The organization is interested in reducing security concerns while maximizing the ability to collaborate across diverse teams.Which statement exemplifies best practices for implementing this system?

- A.Isolating tables in separate databases based on data quality tiers allows for easy permissions management through database ACLs and allows physical separation of default storage locations for managed tables.
- B.Because databases on Databricks are merely a logical construct, choices around database organization do not impact security or discoverability in the Lakehouse.
- C.Storing all production tables in a single database provides a unified view of all data assets available throughout the Lakehouse, simplifying discoverability by granting all users view privileges on this database.
- D.Working in the default Databricks database provides the greatest security when working with managed tables, as these will be created in the DBFS root.
- E.Because all tables must live in the same storage containers used for the database they're created in, organizations should be prepared to create between dozens and thousands of databases depending on their data isolation requirements.

The correct answer is **A**.

**Explanation for Correct Answer**

**A. Isolating tables in separate databases based on data quality tiers allows for easy permissions management through database ACLs and allows physical separation of default storage locations for managed tables.**

-   Organizing tables by **bronze, silver, and gold databases** aligns directly with Lakehouse best practices.
    
-   Databases in Databricks provide:
    
    -   **Logical isolation** for governance and discoverability
        
    -   **Permission boundaries** via database- and table-level ACLs
        
    -   **Physical separation** of managed table storage locations
        
-   This design supports the stated goals:
    
    -   **Reduced security risk** (restrict access to bronze with raw PII)
        
    -   **Maximum collaboration** (broader access to silver and gold with pseudonymized/anonymized data)
        
-   Silver tables serving both data engineering and ML workloads benefit from controlled, shared access without exposing raw sensitive data.
    

This approach scales well for **thousands of tables** and diverse consumer groups.

**Why Other Options Are Incorrect**

**B. Databases are merely logical and do not impact security or discoverability.**

-   Incorrect. Databases are a **core governance construct** in Databricks and directly affect access control and discoverability.
    

**C. Storing all production tables in a single database simplifies discoverability.**

-   Incorrect. This increases **security risk**, violates least-privilege principles, and makes governance harder at scale.
    

**D. Working in the default Databricks database provides the greatest security.**

-   Incorrect. The default database is **not intended for production isolation** and offers no special security advantages.
    

**E. All tables must live in the same storage containers as their database.**

-   Incorrect. External tables and custom storage locations allow flexible physical layouts without creating thousands of databases.
    

**Key Takeaway (Interview-Ready)**

-   **Tier-based database isolation (bronze/silver/gold)** is a best practice in the Lakehouse.
    
-   It balances **security, governance, scalability, and collaboration**, especially in large enterprise migrations.

---

### Question 34

The data architect has mandated that all tables in the Lakehouse should be configured as external Delta Lake tables.Which approach will ensure that this requirement is met?

- A.Whenever a database is being created, make sure that the LOCATION keyword is used
- B.When configuring an external data warehouse for all table storage, leverage Databricks for all ELT.
- C.Whenever a table is being created, make sure that the LOCATION keyword is used.
- D.When tables are created, make sure that the EXTERNAL keyword is used in the CREATE TABLE statement.
- E.When the workspace is being configured, make sure that external cloud object storage has been mounted.

The correct answer is **C**.

**Explanation for Correct Answer**

**C. Whenever a table is being created, make sure that the `LOCATION` keyword is used.**

-   In Delta Lake, a table is considered **external** when its data is stored at a **user-managed storage location**.
    
-   Specifying the `LOCATION` clause in the `CREATE TABLE` statement explicitly tells Databricks **where the data lives**, outside the managed database directory.
    
-   This guarantees that the table is created as an **external Delta table**, regardless of the database configuration.
    

----------

**Why Other Options Are Incorrect**

**A. Using `LOCATION` when creating a database**

-   Incorrect. This only sets the _default_ location for managed tables in that database; tables can still be created without specifying a location and will remain managed.
    

**B. Leveraging an external data warehouse for all table storage**

-   Incorrect. This is an architectural choice, not a Databricks or Delta Lake enforcement mechanism.
    

**D. Using the `EXTERNAL` keyword in `CREATE TABLE`**

-   Incorrect. Delta Lake does **not require or rely on an `EXTERNAL` keyword**; external vs managed is determined by whether `LOCATION` is specified.
    

**E. Mounting external cloud object storage**

-   Incorrect. Mounting storage makes it accessible, but **does not enforce** that tables are created as external.
    

----------

**Key Takeaway (Interview-Ready)**

-   In Delta Lake, **external tables are defined by specifying a `LOCATION` at table creation time**.
    
-   To enforce an “all external tables” policy, require `CREATE TABLE … LOCATION …` for every table.

---

### Question 35

To reduce storage and compute costs, the data engineering team has been tasked with curating a series of aggregate tables leveraged by business intelligence dashboards, customer-facing applications, production machine learning models, and ad hoc analytical queries.The data engineering team has been made aware of new requirements from a customer-facing application, which is the only downstream workload they manage entirely. As a result, an aggregate table used by numerous teams across the organization will need to have a number of fields renamed, and additional fields will also be added.Which of the solutions addresses the situation while minimally interrupting other teams in the organization without increasing the number of tables that need to be managed?

- A.Send all users notice that the schema for the table will be changing; include in the communication the logic necessary to revert the new table schema to match historic queries.
- B.Configure a new table with all the requisite fields and new names and use this as the source for the customer-facing application; create a view that maintains the original data schema and table name by aliasing select fields from the new table.
- C.Create a new table with the required schema and new fields and use Delta Lake's deep clone functionality to sync up changes committed to one table to the corresponding table.
- D.Replace the current table definition with a logical view defined with the query logic currently writing the aggregate table; create a new table to power the customer-facing application.
- E.Add a table comment warning all users that the table schema and field names will be changing on a given date; overwrite the table in place to the specifications of the customer-facing application.

The correct answer is **B**.

**Explanation for Correct Answer**

**B. Configure a new table with all the requisite fields and new names and use this as the source for the customer-facing application; create a view that maintains the original data schema and table name by aliasing select fields from the new table.**

-   The requirement is to:
    
    -   Support **new schema changes** (renamed + additional fields)
        
    -   **Minimally interrupt** many downstream teams
        
    -   **Avoid increasing the number of tables** that must be managed
        
-   Creating **one new physical table** with the updated schema:
    
    -   Satisfies the customer-facing application’s needs
        
    -   Becomes the new canonical storage
        
-   Creating a **view with the original table name and schema**:
    
    -   Preserves backward compatibility
        
    -   Requires **zero changes** for existing consumers
        
    -   Allows a gradual migration to the new schema
        

This is a **schema evolution via views** pattern, widely used in Lakehouse and warehouse systems.

----------

**Why Other Options Are Incorrect**

**A. Sending notices and asking users to revert logic**

-   Incorrect. This shifts operational burden to consumers and breaks the goal of minimal interruption.
    

**C. Using deep clone to sync schema changes**

-   Incorrect. Deep clones create **another managed table**, increasing operational overhead and cost.
    

**D. Replacing the table with a view and creating a new table**

-   Incorrect. This reverses the dependency model and increases risk by changing the behavior of an existing table.
    

**E. Overwriting the table in place with warnings**

-   Incorrect. This introduces **breaking changes** for many teams and violates backward compatibility principles.
    

----------

**Key Takeaway (Interview-Ready)**

-   When schema changes impact many consumers, the best practice is:
    
    -   **Change the physical table**
        
    -   **Preserve the original contract with a compatibility view**
        
-   Views provide **zero-copy schema versioning** without increasing table sprawl.
---

### Question 36

A Delta Lake table representing metadata about content posts from users has the following schema: user_id LONG, post_text STRING, post_id STRING, longitude FLOAT, latitude FLOAT, post_time TIMESTAMP, date DATEThis table is partitioned by the date column. A query is run with the following filter: longitude < 20 & longitude > -20Which statement describes how data will be filtered?

- A.Statistics in the Delta Log will be used to identify partitions that might Include files in the filtered range.
- B.No file skipping will occur because the optimizer does not know the relationship between the partition column and the longitude.
- C.The Delta Engine will use row-level statistics in the transaction log to identify the flies that meet the filter criteria.
- D.Statistics in the Delta Log will be used to identify data files that might include records in the filtered range.
- E.The Delta Engine will scan the parquet file footers to identify each row that meets the filter criteria.


The correct answer is **D**.

**Explanation for Correct Answer**

**D. Statistics in the Delta Log will be used to identify data files that might include records in the filtered range.**

-   The table is **partitioned by `date`**, but the filter predicate is on **`longitude`**, which is **not a partition column**.
    
-   Delta Lake maintains **file-level statistics** (min/max, null counts, etc.) for columns in the **Delta transaction log**.
    
-   For a filter like `longitude < 20 AND longitude > -20`, Delta Lake can:
    
    -   Use **file-level min/max statistics for `longitude`**
        
    -   **Skip entire data files** whose statistics prove they cannot contain matching rows
        
-   This process is known as **data skipping** and happens **without scanning Parquet footers**.
    

----------

**Why Other Options Are Incorrect**

**A. Statistics will be used to identify partitions.**

-   Incorrect. Partition pruning only works when the filter references the **partition column (`date`)**, which it does not.
    

**B. No file skipping will occur.**

-   Incorrect. Delta Lake supports **data skipping on non-partition columns** using file-level statistics.
    

**C. Row-level statistics will be used.**

-   Incorrect. Delta Lake stores **file-level**, not row-level, statistics.
    

**E. Parquet file footers will be scanned row by row.**

-   Incorrect. Delta Lake relies on the **transaction log statistics**, avoiding unnecessary Parquet scans.
    

----------

**Key Takeaway (Interview-Ready)**

-   **Partition pruning** works only on partition columns.
    
-   **Delta data skipping** uses **file-level statistics in the Delta log** and works for **any column with stats**, including non-partition columns like `longitude`.

---

### Question 37

A small company based in the United States has recently contracted a consulting firm in India to implement several new data engineering pipelines to power artificial intelligence applications. All the company's data is stored in regional cloud storage in the United States.The workspace administrator at the company is uncertain about where the Databricks workspace used by the contractors should be deployed.Assuming that all data governance considerations are accounted for, which statement accurately informs this decision?

- A.Databricks runs HDFS on cloud volume storage; as such, cloud virtual machines must be deployed in the region where the data is stored.
- B.Databricks workspaces do not rely on any regional infrastructure; as such, the decision should be made based upon what is most convenient for the workspace administrator.
- C.Cross-region reads and writes can incur significant costs and latency; whenever possible, compute should be deployed in the same region the data is stored.
- D.Databricks leverages user workstations as the driver during interactive development; as such, users should always use a workspace deployed in a region they are physically near.
- E.Databricks notebooks send all executable code from the user’s browser to virtual machines over the open internet; whenever possible, choosing a workspace region near the end users is the most secure.

The correct answer is **C**.

**Explanation for Correct Answer**

**C. Cross-region reads and writes can incur significant costs and latency; whenever possible, compute should be deployed in the same region the data is stored.**

-   Databricks **compute executes close to the data**, not close to the user.
    
-   When a workspace is deployed in a **different cloud region** than the data:
    
    -   Cross-region network traffic incurs **additional egress costs**
        
    -   Latency increases, negatively impacting:
        
        -   ETL pipelines
            
        -   ML training
            
        -   Interactive analytics
            
-   Even if contractors are physically located in India, the **Databricks control plane and UI access are global**, while **data access is regional**.
    
-   Best practice is to deploy the **Databricks workspace in the same cloud region as the data** (United States in this case).
    

----------

**Why Other Options Are Incorrect**

**A. Databricks runs HDFS on cloud volume storage.**

-   Incorrect. Databricks uses **cloud object storage** (S3, ADLS, GCS), not HDFS-backed volumes.
    

**B. Databricks workspaces do not rely on regional infrastructure.**

-   Incorrect. Databricks workspaces are **region-specific**, and compute is provisioned in that region.
    

**D. Databricks leverages user workstations as the driver.**

-   Incorrect. Drivers and executors run on **cloud VMs**, not user machines.
    

**E. Code is sent over the open internet, so proximity improves security.**

-   Incorrect. Databricks uses **secure control planes and encrypted channels**; region choice is driven by **data locality**, not user proximity.
    

----------

**Key Takeaway (Interview-Ready)**

-   **Deploy Databricks compute in the same region as the data** to minimize latency and cloud networking costs.
    
-   **User location is irrelevant**; data locality is what matters.

---

### Question 38

![Question 38 Image 1](examtopics_images/question_38_img_1.png)

The downstream consumers of a Delta Lake table have been complaining about data quality issues impacting performance in their applications. Specifically, they have complained that invalid latitude and longitude values in the activity_details table have been breaking their ability to use other geolocation processes.A junior engineer has written the following code to add CHECK constraints to the Delta Lake table:A senior engineer has confirmed the above logic is correct and the valid ranges for latitude and longitude are provided, but the code fails when executed.Which statement explains the cause of this failure?

- A.Because another team uses this table to support a frequently running application, two-phase locking is preventing the operation from committing.
- B.The activity_details table already exists; CHECK constraints can only be added during initial table creation.
- C.The activity_details table already contains records that violate the constraints; all existing data must pass CHECK constraints in order to add them to an existing table.
- D.The activity_details table already contains records; CHECK constraints can only be added prior to inserting values into a table.
- E.The current table schema does not contain the field valid_coordinates; schema evolution will need to be enabled before altering the table to add a constraint.


The correct answer is **C**.

**Explanation for Correct Answer**

**C. The `activity_details` table already contains records that violate the constraints; all existing data must pass CHECK constraints in order to add them to an existing table.**

-   In Delta Lake, **CHECK constraints are enforced immediately** when they are added to a table.
    
-   When you run `ALTER TABLE … ADD CONSTRAINT … CHECK (…)`:
    
    -   Delta Lake **validates the constraint against all existing rows**
        
    -   If **any row violates the constraint**, the operation **fails**
        
-   Since downstream users are already complaining about **invalid latitude and longitude values**, it strongly indicates that:
    
    -   Some existing records fall outside the valid ranges
        
    -   This causes the constraint addition to fail, even though the logic itself is correct
        

----------

**Why Other Options Are Incorrect**

**A. Two-phase locking is preventing the operation from committing.**

-   Incorrect. Delta Lake uses **optimistic concurrency control**, not two-phase locking.
    

**B. CHECK constraints can only be added during initial table creation.**

-   Incorrect. Delta Lake supports adding CHECK constraints **after table creation**, provided existing data is valid.
    

**D. CHECK constraints can only be added prior to inserting values.**

-   Incorrect. Constraints can be added later, but **existing data must comply**.
    

**E. The schema does not contain the field `valid_coordinates`.**

-   Incorrect. `valid_coordinates` is the **constraint name**, not a column, and does not require schema evolution.
    

----------

**Key Takeaway (Interview-Ready)**

-   Delta Lake **CHECK constraints are not retroactive fixes**.
    
-   You must **clean or correct existing invalid data first** before adding constraints to enforce data quality going forward.

---

### Question 39

Which of the following is true of Delta Lake and the Lakehouse?

- A.Because Parquet compresses data row by row. strings will only be compressed when a character is repeated multiple times.
- B.Delta Lake automatically collects statistics on the first 32 columns of each table which are leveraged in data skipping based on query filters.
- C.Views in the Lakehouse maintain a valid cache of the most recent versions of source tables at all times.
- D.Primary and foreign key constraints can be leveraged to ensure duplicate values are never entered into a dimension table.
- E.Z-order can only be applied to numeric values stored in Delta Lake tables.

The correct answer is **B**.

**Explanation for Correct Answer**

**B. Delta Lake automatically collects statistics on the first 32 columns of each table which are leveraged in data skipping based on query filters.**

-   Delta Lake stores **file-level statistics** (such as min, max, and null counts) in the **transaction log**.
    
-   By default, these statistics are collected for the **first 32 columns** of a table.
    
-   The query optimizer uses these statistics to perform **data skipping**, avoiding reading files that cannot satisfy a query’s filter predicates.
    
-   This improves query performance without requiring partitioning on every filtered column.
    

----------

**Why Other Options Are Incorrect**

**A. Parquet compresses data row by row.**

-   Incorrect. Parquet is a **columnar** format and compresses data **column-wise**, which is especially efficient for strings and repeated values.
    

**C. Views maintain a valid cache of the most recent versions of source tables.**

-   Incorrect. Views are **logical definitions**, not cached data. They are evaluated at query time unless explicitly materialized.
    

**D. Primary and foreign key constraints ensure uniqueness.**

-   Incorrect. Delta Lake supports declaring PK/FK constraints, but they are **not enforced**; they are informational only.
    

**E. Z-order can only be applied to numeric values.**

-   Incorrect. Z-ordering can be applied to **any orderable column type**, including strings and timestamps.
    

----------

**Key Takeaway (Interview-Ready)**

-   Delta Lake improves performance using **file-level statistics and data skipping**, with stats collected by default on the **first 32 columns** of each table.

---

### Question 40

![Question 40 Image 1](examtopics_images/question_40_img_1.png)

The view updates represents an incremental batch of all newly ingested data to be inserted or updated in the customers table.The following logic is used to process these records.Which statement describes this implementation?

- A.The customers table is implemented as a Type 3 table; old values are maintained as a new column alongside the current value.
- B.The customers table is implemented as a Type 2 table; old values are maintained but marked as no longer current and new values are inserted.
- C.The customers table is implemented as a Type 0 table; all writes are append only with no changes to existing values.
- D.The customers table is implemented as a Type 1 table; old values are overwritten by new values and no history is maintained.
- E.The customers table is implemented as a Type 2 table; old values are overwritten and new customers are appended.

The correct answer is **B**.

**Explanation for Correct Answer**

**B. The customers table is implemented as a Type 2 table; old values are maintained but marked as no longer current and new values are inserted.**

-   The logic explicitly checks for **changes in address** for customers marked as `current = true`.
    
-   When a change is detected:
    
    -   The existing record is **updated**:
        
        -   `current = false`
            
        -   `end_date` is populated
            
    -   A **new record is inserted** with:
        
        -   `current = true`
            
        -   A new `effective_date`
            
        -   `end_date = NULL`
            
-   This preserves **full historical versions** of customer records while clearly identifying the active one.
    

This pattern is the textbook implementation of a **Slowly Changing Dimension (SCD) Type 2**.

----------

**Why Other Options Are Incorrect**

**A. Type 3**

-   Incorrect. Type 3 stores limited history in **additional columns**, not additional rows.
    

**C. Type 0**

-   Incorrect. Type 0 allows **no changes** to existing attributes.
    

**D. Type 1**

-   Incorrect. Type 1 **overwrites** old values and does not maintain history.
    

**E. Type 2 with overwrite**

-   Incorrect. Type 2 never overwrites historical records; it **expires** them and inserts new rows.
    

----------

**Key Takeaway (Interview-Ready)**

-   **SCD Type 2** keeps history by:
    
    -   Expiring old records (`current = false`, `end_date`)
        
    -   Inserting new records for each change
        
-   Delta Lake `MERGE` makes Type 2 implementations **atomic and scalable**.
---

### Question 41

The DevOps team has configured a production workload as a collection of notebooks scheduled to run daily using the Jobs UI. A new data engineering hire is onboarding to the team and has requested access to one of these notebooks to review the production logic.What are the maximum notebook permissions that can be granted to the user without allowing accidental changes to production code or data?

- A.Can Manage
- B.Can Edit
- C.No permissions
- D.Can Read
- E.Can Run


The correct answer is **D**.

**Explanation for Correct Answer**

**D. Can Read**

-   **Can Read** permission allows the user to:
    
    -   View the notebook contents
        
    -   Understand the production logic
        
-   It **does not allow**:
    
    -   Editing the notebook
        
    -   Running the notebook
        
    -   Modifying production data or code
        

This satisfies the requirement to **review production logic** while **preventing accidental changes**.

----------

**Why Other Options Are Incorrect**

**A. Can Manage**

-   Allows full control, including permissions, edits, and job configuration changes.
    

**B. Can Edit**

-   Allows modifying notebook code, which risks accidental changes.
    

**C. No permissions**

-   Prevents the user from viewing the notebook at all.
    

**E. Can Run**

-   Allows execution of production code, which may modify data.
    

----------

**Key Takeaway (Interview-Ready)**

-   Use **Can Read** when you want users to **inspect production notebooks safely** without execution or modification rights.
---

### Question 42

![Question 42 Image 1](examtopics_images/question_42_img_1.png)

A table named user_ltv is being used to create a view that will be used by data analysts on various teams. Users in the workspace are configured into groups, which are used for setting up data access using ACLs.The user_ltv table has the following schema:email STRING, age INT, ltv INTThe following view definition is executed:An analyst who is not a member of the marketing group executes the following query:SELECT * FROM email_ltv -Which statement describes the results returned by this query?

- A.Three columns will be returned, but one column will be named "REDACTED" and contain only null values.
- B.Only the email and ltv columns will be returned; the email column will contain all null values.
- C.The email and ltv columns will be returned with the values in user_ltv.
- D.The email.age, and ltv columns will be returned with the values in user_ltv.
- E.Only the email and ltv columns will be returned; the email column will contain the string "REDACTED" in each row.

The correct answer is **E**.

**Explanation for Correct Answer**

**E. Only the email and ltv columns will be returned; the email column will contain the string `"REDACTED"` in each row.**

-   The view definition explicitly selects **two columns**:
    
    -   A derived column aliased as `email`
        
    -   The `ltv` column
        
-   The logic uses:
    
    `CASE  WHEN is_member('marketing') THEN email ELSE  'REDACTED'  END  AS email` 
    
-   The analyst is **not a member of the `marketing` group**, so:
    
    -   `is_member('marketing')` evaluates to **false**
        
    -   The `ELSE 'REDACTED'` branch is used for **every row**
        
-   Therefore:
    
    -   The `email` column contains the literal string `"REDACTED"` for all rows
        
    -   The `ltv` column contains the original values from `user_ltv`
        
-   The `age` column is **not selected** in the view and will not appear in the results.
    

----------

**Why Other Options Are Incorrect**

**A. Three columns returned with one named "REDACTED".**

-   Incorrect. Only **two columns** are selected, and `"REDACTED"` is a value, not a column name.
    

**B. Email and ltv returned, but email contains nulls.**

-   Incorrect. The logic returns the **string `'REDACTED'`**, not `NULL`.
    

**C. Email and ltv returned with original values.**

-   Incorrect. That would only happen if the user **were a member of the marketing group**.
    

**D. Email, age, and ltv returned.**

-   Incorrect. The `age` column is not selected in the view.
    

----------

**Key Takeaway (Interview-Ready)**

-   Databricks views can enforce **dynamic, group-based column masking** using `is_member()`.
    
-   Users without the required group membership still see the schema, but **sensitive values are replaced**, not removed.

---

### Question 43

![Question 43 Image 1](examtopics_images/question_43_img_1.png)

The data governance team has instituted a requirement that all tables containing Personal Identifiable Information (PH) must be clearly annotated. This includes adding column comments, table comments, and setting the custom table property "contains_pii" = true.The following SQL DDL statement is executed to create a new table:Which command allows manual confirmation that these three requirements have been met?

- A.DESCRIBE EXTENDED dev.pii_test
- B.DESCRIBE DETAIL dev.pii_test
- C.SHOW TBLPROPERTIES dev.pii_test
- D.DESCRIBE HISTORY dev.pii_test
- E.SHOW TABLES dev

The correct answer is **A**.

**Explanation for Correct Answer**

**A. `DESCRIBE EXTENDED dev.pii_test`**

-   `DESCRIBE EXTENDED` returns **all relevant metadata in one place**, including:
    
    -   **Column-level comments** (to confirm PII annotation on columns)
        
    -   **Table comment** (`COMMENT "Contains PII"`)
        
    -   **Table properties**, including the custom property
        
        `contains_pii = true` 
        
-   This makes it the **only single command** that allows **manual confirmation of all three governance requirements at once**.
    

----------

**Why Other Options Are Incorrect**

**B. `DESCRIBE DETAIL dev.pii_test`**

-   Shows storage, size, format, and properties, but **does not reliably show column comments**.
    

**C. `SHOW TBLPROPERTIES dev.pii_test`**

-   Confirms the table property only; does **not show column or table comments**.
    

**D. `DESCRIBE HISTORY dev.pii_test`**

-   Shows commit history, not metadata annotations.
    

**E. `SHOW TABLES dev`**

-   Only lists tables; no metadata details.
    

----------

**Key Takeaway (Interview-Ready)**

-   Use **`DESCRIBE EXTENDED`** when you need a **single, comprehensive view of schema, comments, and table properties** for governance and compliance validation.

---

### Question 44

![Question 44 Image 1](examtopics_images/question_44_img_1.png)

The data governance team is reviewing code used for deleting records for compliance with GDPR. They note the following logic is used to delete records from the Delta Lake table named users.Assuming that user_id is a unique identifying key and that delete_requests contains all users that have requested deletion, which statement describes whether successfully executing the above logic guarantees that the records to be deleted are no longer accessible and why?

- A.Yes; Delta Lake ACID guarantees provide assurance that the DELETE command succeeded fully and permanently purged these records.
- B.No; the Delta cache may return records from previous versions of the table until the cluster is restarted.
- C.Yes; the Delta cache immediately updates to reflect the latest data files recorded to disk.
- D.No; the Delta Lake DELETE command only provides ACID guarantees when combined with the MERGE INTO command.
- E.No; files containing deleted records may still be accessible with time travel until a VACUUM command is used to remove invalidated data files.

The correct answer is **E**.

**Explanation for Correct Answer**

**E. No; files containing deleted records may still be accessible with time travel until a `VACUUM` command is used to remove invalidated data files.**

-   In Delta Lake, a `DELETE` operation:
    
    -   Logically removes records by creating a **new table version**
        
    -   Does **not immediately delete underlying data files**
        
-   Old data files remain accessible via **time travel** until they are physically removed.
    
-   To permanently remove deleted data (required for GDPR compliance):
    
    -   A `VACUUM` operation must be executed
        
    -   The retention period must be configured appropriately (with care)
        
-   Therefore, **executing `DELETE` alone does not guarantee permanent removal** of data.
    

---

**Why Other Options Are Incorrect**

**A. ACID guarantees permanent purge**

-   Incorrect. ACID guarantees consistency and isolation, not **physical deletion**.
    

**B. Delta cache returns old records**

-   Incorrect. Delta cache respects table versions and does not expose deleted rows incorrectly.
    

**C. Delta cache immediately updates disk files**

-   Incorrect. Deletes are logical, not immediate physical removals.
    

**D. DELETE only works with MERGE**

-   Incorrect. `DELETE` is a fully supported standalone operation.
    

---

**Key Takeaway (Interview-Ready)**

-   Delta Lake `DELETE` ensures **logical deletion**.
    
-   **Physical data removal** requires `VACUUM`.
    
-   For GDPR and “right to be forgotten” use cases:
    
    -   Use `DELETE`
        
    -   Follow with `VACUUM` (with retention policy considerations).

---

### Question 45

![Question 45 Image 1](examtopics_images/question_45_img_1.png)

![Question 45 Image 2](examtopics_images/question_45_img_2.png)

An external object storage container has been mounted to the location /mnt/finance_eda_bucket.The following logic was executed to create a database for the finance team:After the database was successfully created and permissions configured, a member of the finance team runs the following code:If all users on the finance team are members of the finance group, which statement describes how the tx_sales table will be created?

- A.A logical table will persist the query plan to the Hive Metastore in the Databricks control plane.
- B.An external table will be created in the storage container mounted to /mnt/finance_eda_bucket.
- C.A logical table will persist the physical plan to the Hive Metastore in the Databricks control plane.
- D.An managed table will be created in the storage container mounted to /mnt/finance_eda_bucket.
- E.A managed table will be created in the DBFS root storage container.

The correct answer is **D**.

**Explanation for Correct Answer**

**D. A managed table will be created in the storage container mounted to `/mnt/finance_eda_bucket`.**

-   The database `finance_eda_db` is created with an explicit **LOCATION**:
    
    sql
    
    Copy code
    
    `CREATE DATABASE finance_eda_db LOCATION '/mnt/finance_eda_bucket';`
    
-   This sets the **default storage location for managed tables** created in this database.
    
-   The table is created using:
    
    sql
    
    Copy code
    
    `CREATE TABLE finance_eda_db.tx_sales AS SELECT * FROM sales WHERE state = "TX";`
    
-   Because:
    
    -   No `LOCATION` is specified at table creation time, and
        
    -   The database itself has a defined location  
        👉 the table is created as a **managed table**, stored under the database’s location in the mounted external storage.
        

So:

-   **Managed table** ✔
    
-   **Stored in `/mnt/finance_eda_bucket`** ✔
    

---

**Why Other Options Are Incorrect**

**A. A logical table will persist the query plan to the Hive Metastore.**

-   Incorrect. This describes a **view**, not a table created with `CREATE TABLE AS SELECT`.
    

**B. An external table will be created in the mounted container.**

-   Incorrect. External tables require a **`LOCATION` clause at table creation time**.
    

**C. A logical table will persist the physical plan to the Hive Metastore.**

-   Incorrect. Tables persist **metadata**, not query plans.
    

**E. A managed table will be created in the DBFS root.**

-   Incorrect. The database’s explicit `LOCATION` overrides the DBFS root default.
    

---

**Key Takeaway (Interview-Ready)**

-   **Database LOCATION controls where managed tables are stored**.
    
-   A table is **external only if `LOCATION` is specified at table creation**.
    
-   `CREATE TABLE AS SELECT` without `LOCATION` → **managed table** in the database’s storage path.

---

### Question 46

Although the Databricks Utilities Secrets module provides tools to store sensitive credentials and avoid accidentally displaying them in plain text users should still be careful with which credentials are stored here and which users have access to using these secrets.Which statement describes a limitation of Databricks Secrets?

- A.Because the SHA256 hash is used to obfuscate stored secrets, reversing this hash will display the value in plain text.
- B.Account administrators can see all secrets in plain text by logging on to the Databricks Accounts console.
- C.Secrets are stored in an administrators-only table within the Hive Metastore; database administrators have permission to query this table by default.
- D.Iterating through a stored secret and printing each character will display secret contents in plain text.
- E.The Databricks REST API can be used to list secrets in plain text if the personal access token has proper credentials.

The correct answer is **D**.

**Explanation for Correct Answer**

**D. Iterating through a stored secret and printing each character will display secret contents in plain text.**

-   Databricks Secrets are **not encrypted at runtime** when accessed by a notebook or job.
    
-   Secrets are only **masked in notebook output** when printed directly.
    
-   If a user has permission to access a secret, they can:
    
    -   Assign it to a variable
        
    -   Iterate over it
        
    -   Manipulate it programmatically
        
-   This means a user could deliberately (or accidentally) expose the secret’s value in plain text.
    

This is a **designed limitation**: Databricks Secrets protect against *accidental exposure*, not *malicious misuse by authorized users*.

---

**Why Other Options Are Incorrect**

**A. SHA256 hashes can be reversed.**

-   Incorrect. SHA256 is a one-way hash and cannot be reversed; Databricks does not rely on reversible hashing for secrets.
    

**B. Account administrators can see secrets in plain text.**

-   Incorrect. Even administrators **cannot view secret values** once stored.
    

**C. Secrets are stored in the Hive Metastore.**

-   Incorrect. Secrets are stored in a **secure, internal secrets service**, not queryable tables.
    

**E. REST API can list secrets in plain text.**

-   Incorrect. The REST API can list **secret metadata**, not secret values.
    

---

**Key Takeaway (Interview-Ready)**

-   Databricks Secrets prevent **accidental disclosure**, not **intentional misuse**.
    
-   Any user with access to a secret can programmatically expose it.
    
-   Always follow **least-privilege access** when granting secret permissions.

---

### Question 47

What statement is true regarding the retention of job run history?

- A.It is retained until you export or delete job run logs
- B.It is retained for 30 days, during which time you can deliver job run logs to DBFS or S3
- C.It is retained for 60 days, during which you can export notebook run results to HTML
- D.It is retained for 60 days, after which logs are archived
- E.It is retained for 90 days or until the run-id is re-used through custom run configuration

The correct answer is **C**.

**Explanation for Correct Answer**

**C. It is retained for 60 days, during which you can export notebook run results to HTML.**

-   Databricks **retains job run history for 60 days** by default.
    
-   During this retention window, users can:
    
    -   View job and task run details in the Jobs UI
        
    -   Inspect logs and outputs
        
    -   **Export notebook run results to HTML** for auditing, debugging, or sharing
        
-   After the 60-day period, run history and associated artifacts are **no longer accessible** through the Databricks UI.
    

---

**Why Other Options Are Incorrect**

**A. Retained until you export or delete job run logs**

-   Incorrect. Retention is **time-based**, not user-controlled.
    

**B. Retained for 30 days**

-   Incorrect. The standard retention period is **60 days**, not 30.
    

**D. Retained for 60 days, after which logs are archived**

-   Incorrect. Logs are **not archived for later access**; they are removed.
    

**E. Retained for 90 days or until run-id reuse**

-   Incorrect. There is no 90-day default retention, and run IDs are not reused in this manner.
    

---

**Key Takeaway (Interview-Ready)**

-   **Databricks job run history is retained for 60 days**.
    
-   If long-term auditability is required, **export logs or notebook results within that window** to external storage.


---

### Question 48

A data engineer, User A, has promoted a new pipeline to production by using the REST API to programmatically create several jobs. A DevOps engineer, User B, has configured an external orchestration tool to trigger job runs through the REST API. Both users authorized the REST API calls using their personal access tokens.Which statement describes the contents of the workspace audit logs concerning these events?

- A.Because the REST API was used for job creation and triggering runs, a Service Principal will be automatically used to identify these events.
- B.Because User B last configured the jobs, their identity will be associated with both the job creation events and the job run events.
- C.Because these events are managed separately, User A will have their identity associated with the job creation events and User B will have their identity associated with the job run events.
- D.Because the REST API was used for job creation and triggering runs, user identity will not be captured in the audit logs.
- E.Because User A created the jobs, their identity will be associated with both the job creation events and the job run events.

The correct answer is **C**.

**Explanation for Correct Answer**

**C. Because these events are managed separately, User A will have their identity associated with the job creation events and User B will have their identity associated with the job run events.**

-   Databricks **workspace audit logs record the identity tied to the personal access token (PAT)** used for each API call.
    
-   Job **creation** and job **execution** are **separate events** in the audit log.
    
-   Since:
    
    -   **User A** used their PAT to create the jobs via the REST API
        
    -   **User B** used their PAT to trigger job runs via the REST API
        
-   The audit logs will correctly reflect:
    
    -   **User A** as the actor for **job creation**
        
    -   **User B** as the actor for **job run triggers**
        

This is exactly how Databricks supports traceability and accountability.

---

**Why Other Options Are Incorrect**

**A. A Service Principal is automatically used.**

-   Incorrect. A service principal is only used if **explicitly configured**; PATs represent users.
    

**B. User B is associated with both events.**

-   Incorrect. Job creation and job runs are logged independently based on the PAT used.
    

**D. User identity is not captured.**

-   Incorrect. Audit logs always capture the authenticated identity.
    

**E. User A is associated with both events.**

-   Incorrect. Job runs are attributed to **who triggered them**, not who created the job.
    

---

**Key Takeaway (Interview-Ready)**

-   **Audit logs reflect the identity associated with the authentication token used for each action**.
    
-   Using **PATs ties actions to individual users**, even when actions are performed programmatically via the REST API.
    
-   For shared automation, **service principals** are recommended to avoid user-bound attribution.

---

### Question 49

A user new to Databricks is trying to troubleshoot long execution times for some pipeline logic they are working on. Presently, the user is executing code cell-by-cell, using display() calls to confirm code is producing the logically correct results as new transformations are added to an operation. To get a measure of average time to execute, the user is running each cell multiple times interactively.Which of the following adjustments will get a more accurate measure of how code is likely to perform in production?

- A.Scala is the only language that can be accurately tested using interactive notebooks; because the best performance is achieved by using Scala code compiled to JARs, all PySpark and Spark SQL logic should be refactored.
- B.The only way to meaningfully troubleshoot code execution times in development notebooks Is to use production-sized data and production-sized clusters with Run All execution.
- C.Production code development should only be done using an IDE; executing code against a local build of open source Spark and Delta Lake will provide the most accurate benchmarks for how code will perform in production.
- D.Calling display() forces a job to trigger, while many transformations will only add to the logical query plan; because of caching, repeated execution of the same logic does not provide meaningful results.
- E.The Jobs UI should be leveraged to occasionally run the notebook as a job and track execution time during incremental code development because Photon can only be enabled on clusters launched for scheduled jobs.

The correct answer is **D**.

**Explanation for Correct Answer**

**D. Calling `display()` forces a job to trigger, while many transformations will only add to the logical query plan; because of caching, repeated execution of the same logic does not provide meaningful results.**

-   In Spark, **most transformations are lazy**—they only build a logical execution plan.
    
-   An **action** (such as `display()`, `show()`, `count()`, or `write`) is what actually **triggers execution**.
    
-   When the user repeatedly runs cells interactively:
    
    -   Spark may reuse cached data
        
    -   The execution plan may already be optimized
        
    -   Subsequent runs are **not representative of first-run or production performance**
        
-   As a result, timing interactive, repeated `display()` calls gives a **misleadingly optimistic view** of execution time.
    

Understanding Spark’s lazy evaluation model is essential for meaningful performance measurement.

---

**Why Other Options Are Incorrect**

**A. Scala-only testing is required for accuracy**

-   Incorrect. PySpark and Spark SQL compile to the same execution engine; performance differences are negligible for most workloads.
    

**B. Production-sized data and clusters are the only way to test**

-   Incorrect. While scale matters, the key issue here is **execution semantics**, not cluster size alone.
    

**C. Local Spark builds provide the most accurate benchmarks**

-   Incorrect. Local Spark lacks distributed execution characteristics and does not reflect Databricks runtime optimizations.
    

**E. Photon can only be enabled for scheduled jobs**

-   Incorrect. Photon can be enabled on **interactive clusters** as well.
    

---

**Key Takeaway (Interview-Ready)**

-   Spark is **lazy by design**.
    
-   Re-running cells with `display()` often measures **cache effects**, not real execution cost.
    
-   For accurate performance insights, understand **when jobs are triggered** and avoid timing repeated interactive executions.

---

### Question 50

A production cluster has 3 executor nodes and uses the same virtual machine type for the driver and executor.When evaluating the Ganglia Metrics for this cluster, which indicator would signal a bottleneck caused by code executing on the driver?

- A.The five Minute Load Average remains consistent/flat
- B.Bytes Received never exceeds 80 million bytes per second
- C.Total Disk Space remains constant
- D.Network I/O never spikes
- E.Overall cluster CPU utilization is around 25%

The correct answer is **E**.

**Explanation for Correct Answer**

**E. Overall cluster CPU utilization is around 25%**

-   The cluster has **3 executor nodes plus 1 driver**, all using the same VM type.
    
-   If the workload were evenly distributed, you would expect **high CPU utilization across executors** during heavy processing.
    
-   Seeing **overall cluster CPU utilization stuck around ~25%** strongly suggests:
    
    -   Only **one node (the driver)** is doing most of the work
        
    -   The executors are largely idle
        
-   This is a classic sign of a **driver-side bottleneck**, often caused by:
    
    -   Non-distributed logic (e.g., `collect()`, `toPandas()`)
        
    -   Driver-only operations
        
    -   Single-threaded processing on the driver
        

Since the driver represents roughly **1 out of 4 equally sized nodes**, ~25% utilization aligns with driver-bound execution.

---

**Why Other Options Are Incorrect**

**A. Five-minute load average remains flat**

-   Not specific to driver bottlenecks; could indicate many normal scenarios.
    

**B. Bytes received never exceeds 80 MB/s**

-   Network throughput alone does not indicate driver-side execution.
    

**C. Total disk space remains constant**

-   Disk usage is unrelated to driver vs executor CPU bottlenecks.
    

**D. Network I/O never spikes**

-   Lack of network I/O does not directly imply driver-only execution.
    

---

**Key Takeaway (Interview-Ready)**

-   In a symmetric cluster, **low overall CPU utilization (~1 / number of nodes)** is a strong indicator that work is being executed primarily on the **driver** instead of being distributed to executors.
    
-   Ganglia metrics are especially useful for spotting **driver-bound Spark jobs** early.

---



### Question 51

Where in the Spark UI can one diagnose a performance problem induced by not leveraging predicate push-down?

- A.In the Executor’s log file, by grepping for "predicate push-down"
- B.In the Stage’s Detail screen, in the Completed Stages table, by noting the size of data read from the Input column
- C.In the Storage Detail screen, by noting which RDDs are not stored on disk
- D.In the Delta Lake transaction log. by noting the column statistics
- E.In the Query Detail screen, by interpreting the Physical Plan

The correct answer is **E**.

**Explanation for Correct Answer**

**E. In the Query Detail screen, by interpreting the Physical Plan**

-   **Predicate push-down** is a query-planning optimization where filters are pushed as close to the data source as possible (e.g., into Parquet/Delta scans).
    
-   Whether predicate push-down is applied is visible in the **Physical Plan** shown in the **Query Detail** page of the Spark UI.
    
-   In the Physical Plan, you can:
    
    -   Inspect scan operators (e.g., `FileScan parquet`, `DeltaScan`)
        
    -   Verify whether **filters appear at the scan level**
        
    -   See pushed filters listed as `PushedFilters`
        
-   If filters are missing from the scan node, Spark is reading more data than necessary—indicating a lack of predicate push-down and a potential performance issue.
    

---

**Why Other Options Are Incorrect**

**A. Executor logs**

-   Not reliable or standard for diagnosing predicate push-down.
    

**B. Stage Detail – Input size**

-   Shows *symptoms* (large reads) but not the *cause* (whether filters were pushed down).
    

**C. Storage Detail (RDDs)**

-   Related to caching, not query optimization.
    

**D. Delta transaction log**

-   Contains file-level statistics, not runtime query execution details.
    

---

**Key Takeaway (Interview-Ready)**

-   To diagnose missing predicate push-down, always check the **Physical Plan in the Spark UI Query Detail screen**.
    
-   Look for filters being applied **at the scan**, not after data is loaded.

---

### Question 52

![Question 52 Image 1](examtopics_images/question_52_img_1.png)

Review the following error traceback:Which statement describes the error being raised?

- A.The code executed was PySpark but was executed in a Scala notebook.
- B.There is no column in the table named heartrateheartrateheartrate
- C.There is a type error because a column object cannot be multiplied.
- D.There is a type error because a DataFrame object cannot be multiplied.
- E.There is a syntax error because the heartrate column is not correctly identified as a column.

The correct answer is **B**.

**Explanation for Correct Answer**

**B. There is no column in the table named `heartrateheartrateheartrate`.**

-   The traceback shows the failing line:
    
    python
    
    Copy code
    
    `display(df.select(3*"heartrate"))`
    
-   In Python, multiplying a **string** by an integer results in **string repetition**:
    
    python
    
    Copy code
    
    `3 * "heartrate"  →  "heartrateheartrateheartrate"`
    
-   Spark then tries to resolve a column literally named:
    
    text
    
    Copy code
    
    `heartrateheartrateheartrate`
    
-   The error message confirms this:
    
    vbnet
    
    Copy code
    
    `AnalysisException: cannot resolve 'heartrateheartrateheartrate' given input columns: ...`
    
-   Since the table only contains a column named `heartrate`, Spark raises an **unresolved column** error.
    

---

**Why Other Options Are Incorrect**

**A. PySpark code executed in a Scala notebook**

-   Incorrect. This is a valid PySpark traceback running in Python.
    

**C. A column object cannot be multiplied**

-   Incorrect. No `Column` object is involved here—this is string multiplication.
    

**D. A DataFrame object cannot be multiplied**

-   Incorrect. The DataFrame is not being multiplied.
    

**E. A syntax error because the column is not correctly identified**

-   Incorrect. This is not a syntax error; it is a **runtime analysis (resolution) error**.
    

---

**Key Takeaway (Interview-Ready)**

-   In PySpark, column names must be passed as:
    
    python
    
    Copy code
    
    `col("heartrate")`
    
    or
    
    python
    
    Copy code
    
    `df["heartrate"]`
    
-   Accidentally performing string operations can silently create **invalid column names**, leading to `AnalysisException` errors during query planning.

---

### Question 53

Which distribution does Databricks support for installing custom Python code packages?

- A.sbt
- B.CRANC. npm
- D.Wheels
- E.jars

The correct answer is **D**.

**Explanation**

**D. Wheels**

-   Databricks supports installing custom Python packages using **Python wheel files (`.whl`)**.
    
-   Wheels are the **standard Python binary distribution format** and are supported for:
    
    -   Cluster libraries
        
    -   Job libraries
        
    -   Workspace-installed libraries
        
-   This allows teams to package, version, and deploy Python code reliably across clusters.
    

---

**Why Other Options Are Incorrect**

**A. sbt**

-   Used for building **Scala** projects, not Python packages.
    

**B. CRAN**

-   CRAN is for **R** packages, not Python.
    

**C. npm**

-   npm is for **JavaScript/Node.js**, not Python.
    

**E. jars**

-   JARs are for **Java/Scala**, not Python.
    

---

**Key Takeaway (Interview-Ready)**

-   For Python on Databricks, **wheel (`.whl`) files are the supported format** for distributing and installing custom packages.

---

### Question 54

Which Python variable contains a list of directories to be searched when trying to locate required modules?

- A.importlib.resource_path
- B.sys.path
- C.os.path
- D.pypi.path
- E.pylib.source

The correct answer is **B**.

**Explanation**

**B. `sys.path`**

-   `sys.path` is a **Python list of directory paths**.
    
-   When you run `import module_name`, Python searches for the module **in order through the directories listed in `sys.path`**.
    
-   It typically includes:
    
    -   The current script’s directory
        
    -   Paths from the `PYTHONPATH` environment variable
        
    -   Standard library directories
        
    -   Site-packages (installed libraries)
        

---

**Why Other Options Are Incorrect**

**A. `importlib.resource_path`**

-   Not a standard variable for module search paths.
    

**C. `os.path`**

-   A module for filesystem path operations, not a list of import locations.
    

**D. `pypi.path`**

-   Does not exist in standard Python.
    

**E. `pylib.source`**

-   Not a valid Python variable.
    

---

**Key Takeaway (Interview-Ready)**

-   Python determines where to look for modules using **`sys.path`**.
    
-   Debug import issues by inspecting:
    
    python
    
    Copy code
    
    `import sys print(sys.path)`

---

### Question 55

Incorporating unit tests into a PySpark application requires upfront attention to the design of your jobs, or a potentially significant refactoring of existing code.Which statement describes a main benefit that offset this additional effort?

- A.Improves the quality of your data
- B.Validates a complete use case of your application
- C.Troubleshooting is easier since all steps are isolated and tested individually
- D.Yields faster deployment and execution times
- E.Ensures that all steps interact correctly to achieve the desired end result

The correct answer is **C**.

**Explanation**

**C. Troubleshooting is easier since all steps are isolated and tested individually**

-   Unit tests focus on **small, isolated pieces of logic** (functions, transformations, business rules).
    
-   Designing PySpark jobs to be testable usually means:
    
    -   Modularizing code
        
    -   Separating transformation logic from I/O
        
-   The main payoff of this extra design effort is that:
    
    -   Failures are **localized**
        
    -   Bugs are easier to reproduce and fix
        
    -   Regressions are caught early
        

This is the core, widely accepted benefit of unit testing.

---

**Why Other Options Are Incorrect**

**A. Improves the quality of your data**

-   Indirect at best; unit tests improve **code correctness**, not data quality guarantees.
    

**B. Validates a complete use case**

-   That describes **integration or end-to-end testing**, not unit testing.
    

**D. Yields faster deployment and execution times**

-   Unit tests do not directly affect runtime performance.
    

**E. Ensures all steps interact correctly**

-   Also describes **integration testing**, not unit testing.
    

---

**Key Takeaway (Interview-Ready)**

-   The primary benefit of unit testing PySpark code is **easier debugging and maintenance through isolation of logic**.
    
-   The upfront design cost pays off in **faster troubleshooting and safer refactoring**.

---

### Question 56

Which statement describes integration testing?

- A.Validates interactions between subsystems of your application
- B.Requires an automated testing framework
- C.Requires manual intervention
- D.Validates an application use case
- E.Validates behavior of individual elements of your application

The correct answer is **A**.

**Explanation**

**A. Validates interactions between subsystems of your application**

-   **Integration testing** focuses on verifying that **multiple components or subsystems work together correctly**.
    
-   It sits between:
    
    -   **Unit testing** (individual components in isolation)
        
    -   **End-to-end testing** (full application workflows)
        
-   In data engineering and PySpark contexts, this often means testing:
    
    -   Interactions between transformations
        
    -   Reads/writes across tables
        
    -   Integration with external systems (storage, APIs, message queues)
        

---

**Why Other Options Are Incorrect**

**B. Requires an automated testing framework**

-   Incorrect. Integration tests *can* be automated, but automation is not a defining requirement.
    

**C. Requires manual intervention**

-   Incorrect. Manual intervention is not required; integration tests are commonly automated.
    

**D. Validates an application use case**

-   Incorrect. This describes **end-to-end (system) testing**.
    

**E. Validates behavior of individual elements**

-   Incorrect. This describes **unit testing**.
    

---

**Key Takeaway (Interview-Ready)**

-   **Unit testing** → individual components
    
-   **Integration testing** → interactions between components
    
-   **End-to-end testing** → full business use cases

---

### Question 57

Which REST API call can be used to review the notebooks configured to run as tasks in a multi-task job?

- A./jobs/runs/list
- B./jobs/runs/get-output
- C./jobs/runs/get
- D./jobs/get
- E./jobs/list

The correct answer is **D**.

**Explanation**

**D. `/jobs/get`**

-   The **`/jobs/get`** REST API returns the **full job definition**, including:
    
    -   Job name
        
    -   Job settings
        
    -   **All tasks in a multi-task job**
        
    -   For each task, the configured **notebook path**, cluster configuration, and dependencies
        
-   This is the only endpoint that lets you **review which notebooks are configured to run as tasks** in a multi-task job.
    

---

**Why Other Options Are Incorrect**

**A. `/jobs/runs/list`**

-   Lists **job runs**, not the job configuration or task definitions.
    

**B. `/jobs/runs/get-output`**

-   Returns logs and outputs for a **specific run**, not task configuration.
    

**C. `/jobs/runs/get`**

-   Shows metadata about a **single job run**, not the notebooks configured in the job.
    

**E. `/jobs/list`**

-   Lists jobs at a high level but **does not include detailed task definitions**.
    

---

**Key Takeaway (Interview-Ready)**

-   To inspect **task-level configuration (including notebooks)** in a Databricks multi-task job, always use:
    
    swift
    
    Copy code
    
    `GET /api/2.1/jobs/get`

---

### Question 58

A Databricks job has been configured with 3 tasks, each of which is a Databricks notebook. Task A does not depend on other tasks. Tasks B and C run in parallel, with each having a serial dependency on task A.If tasks A and B complete successfully but task C fails during a scheduled run, which statement describes the resulting state?

- A.All logic expressed in the notebook associated with tasks A and B will have been successfully completed; some operations in task C may have completed successfully.
- B.All logic expressed in the notebook associated with tasks A and B will have been successfully completed; any changes made in task C will be rolled back due to task failure.
- C.All logic expressed in the notebook associated with task A will have been successfully completed; tasks B and C will not commit any changes because of stage failure.
- D.Because all tasks are managed as a dependency graph, no changes will be committed to the Lakehouse until ail tasks have successfully been completed.
- E.Unless all tasks complete successfully, no changes will be committed to the Lakehouse; because task C failed, all commits will be rolled back automatically.

The correct answer is **A**.

**Explanation**

**A. All logic expressed in the notebook associated with tasks A and B will have been successfully completed; some operations in task C may have completed successfully.**

-   In a Databricks **multi-task job**, each task:
    
    -   Runs **independently**
        
    -   Commits its results **as it executes**
        
-   There is **no automatic transactional rollback across tasks**.
    
-   Given the dependency graph:
    
    -   **Task A** runs first and completes successfully
        
    -   **Tasks B and C** then run in parallel
        
    -   **Task B** completes successfully → its changes are committed
        
    -   **Task C** fails → any work it completed **before the failure remains committed**, unless explicitly handled in the code
        

Databricks jobs **do not provide atomicity across tasks**. Each task is responsible for its own correctness and rollback logic.

---

**Why Other Options Are Incorrect**

**B. Task C changes are rolled back automatically**

-   ❌ Incorrect. There is **no automatic rollback** for failed tasks.
    

**C. Tasks B and C do not commit because of stage failure**

-   ❌ Incorrect. Task B completed successfully and commits independently.
    

**D. No changes are committed until all tasks succeed**

-   ❌ Incorrect. Databricks jobs are **not globally transactional**.
    

**E. All commits are rolled back if any task fails**

-   ❌ Incorrect. This behavior does not exist in Databricks Jobs.
    

---

**Key Takeaway (Interview-Ready)**

-   Databricks multi-task jobs provide **orchestration, not distributed transactions**.
    
-   Each task commits its results independently.
    
-   If cross-task atomicity is required, you must:
    
    -   Implement explicit rollback logic, or
        
    -   Use idempotent writes and validation steps, or
        
    -   Consolidate logic into a single transactional unit (e.g., one notebook with controlled commits).

---

### Question 59

![Question 59 Image 1](examtopics_images/question_59_img_1.png)

A Delta Lake table was created with the below query:Realizing that the original query had a typographical error, the below code was executed:ALTER TABLE prod.sales_by_stor RENAME TO prod.sales_by_storeWhich result will occur after running the second command?

- A.The table reference in the metastore is updated and no data is changed.
- B.The table name change is recorded in the Delta transaction log.
- C.All related files and metadata are dropped and recreated in a single ACID transaction.
- D.The table reference in the metastore is updated and all data files are moved.
- E.A new Delta transaction log Is created for the renamed table.

The correct answer is **A**.

**Explanation**

**A. The table reference in the metastore is updated and no data is changed.**

-   The table was created as an **external Delta table** using a fixed `LOCATION`:
    
    sql
    
    Copy code
    
    `LOCATION "/mnt/prod/sales_by_store"`
    
-   Running:
    
    sql
    
    Copy code
    
    `ALTER TABLE prod.sales_by_stor RENAME TO prod.sales_by_store`
    
    only:
    
    -   Updates the **table name metadata in the metastore**
        
    -   Does **not** modify:
        
        -   Delta data files
            
        -   The Delta transaction log
            
        -   The storage location
            
-   The underlying Delta table (data + `_delta_log`) remains exactly the same.
    

This is a **metadata-only operation**.

---

**Why Other Options Are Incorrect**

**B. The table name change is recorded in the Delta transaction log.**

-   ❌ Incorrect. The Delta transaction log tracks **data changes**, not metastore renames.
    

**C. All related files and metadata are dropped and recreated.**

-   ❌ Incorrect. No data rewrite or recreation occurs.
    

**D. All data files are moved.**

-   ❌ Incorrect. The `LOCATION` does not change, so files are not moved.
    

**E. A new Delta transaction log is created.**

-   ❌ Incorrect. The existing `_delta_log` continues to be used.
    

---

**Key Takeaway (Interview-Ready)**

-   `ALTER TABLE … RENAME TO` in Delta Lake:
    
    -   Is a **metastore-only operation**
        
    -   Does **not touch data files or the Delta transaction log**
        
    -   Is fast and non-disruptive, even for very large tables

---

### Question 60

![Question 60 Image 1](examtopics_images/question_60_img_1.png)

The data engineering team maintains a table of aggregate statistics through batch nightly updates. This includes total sales for the previous day alongside totals and averages for a variety of time periods including the 7 previous days, year-to-date, and quarter-to-date. This table is named store_saies_summary and the schema is as follows:The table daily_store_sales contains all the information needed to update store_sales_summary. The schema for this table is: store_id INT, sales_date DATE, total_sales FLOATIf daily_store_sales is implemented as a Type 1 table and the total_sales column might be adjusted after manual data auditing, which approach is the safest to generate accurate reports in the store_sales_summary table?

- A.Implement the appropriate aggregate logic as a batch read against the daily_store_sales table and overwrite the store_sales_summary table with each Update.
- B.Implement the appropriate aggregate logic as a batch read against the daily_store_sales table and append new rows nightly to the store_sales_summary table.
- C.Implement the appropriate aggregate logic as a batch read against the daily_store_sales table and use upsert logic to update results in the store_sales_summary table.
- D.Implement the appropriate aggregate logic as a Structured Streaming read against the daily_store_sales table and use upsert logic to update results in the store_sales_summary table.
- E.Use Structured Streaming to subscribe to the change data feed for daily_store_sales and apply changes to the aggregates in the store_sales_summary table with each update.

The correct answer is **A**.

**Explanation for Correct Answer**

**A. Implement the appropriate aggregate logic as a batch read against the `daily_store_sales` table and overwrite the `store_sales_summary` table with each update.**

-   The key detail is that **`daily_store_sales` is a Type 1 table**:
    
    -   Historical `total_sales` values **can be corrected after the fact** due to manual auditing.
        
-   Because source data can change retroactively:
    
    -   Incremental approaches (append, upsert, streaming, or CDF-based logic) risk producing **incorrect aggregates**
        
    -   Previously computed results may no longer be valid
        
-   The **safest and most accurate approach** is to:
    
    -   Recompute all aggregates from the **current, authoritative snapshot** of `daily_store_sales`
        
    -   **Overwrite** the `store_sales_summary` table on each batch run
        

This guarantees that:

-   All downstream reports reflect the **latest corrected data**
    
-   No stale or partially updated aggregates remain
    

---

**Why Other Options Are Incorrect**

**B. Append nightly rows**

-   ❌ Incorrect. Appending aggregates would accumulate outdated values when historical data changes.
    

**C. Batch upsert logic**

-   ❌ Incorrect. Upserts assume stable historical inputs; Type 1 corrections break this assumption.
    

**D. Structured Streaming with upserts**

-   ❌ Incorrect. Streaming is unnecessary and unsafe when historical source data can change arbitrarily.
    

**E. Change Data Feed–based streaming updates**

-   ❌ Incorrect. CDF propagates changes, but recomputing complex rolling aggregates safely from changes alone is error-prone and unnecessary here.
    

---

**Key Takeaway (Interview-Ready)**

-   When source data is **mutable (Type 1)** and aggregates must always be correct:
    
    -   **Recompute aggregates from scratch**
        
    -   **Overwrite summary tables**
        
-   Incremental aggregation is only safe when source data is **append-only or immutable**.

---

### Question 61

![Question 61 Image 1](examtopics_images/question_61_img_1.png)

A member of the data engineering team has submitted a short notebook that they wish to schedule as part of a larger data pipeline. Assume that the commands provided below produce the logically correct results when run as presented.Which command should be removed from the notebook before scheduling it as a job?

- A.Cmd 2
- B.Cmd 3
- C.Cmd 4
- D.Cmd 5
- E.Cmd 6

The correct answer is **E. Cmd 6**.

**Explanation**

**Cmd 6**

python

Copy code

`display(finalDF)`

-   `display()` is an **interactive, notebook-only action** intended for:
    
    -   Visual inspection
        
    -   Debugging during development
        
-   When a notebook is scheduled as a **Databricks Job**:
    
    -   `display()` provides **no operational value**
        
    -   It **forces an extra Spark action**, increasing runtime and cost
        
    -   Job runs should rely on **writes, logs, or assertions**, not visual output
        

Removing `display()` is a best practice before production scheduling.

---

**Why Other Commands Should Remain**

**Cmd 2 – `printSchema()`**

-   Harmless; useful for logging and debugging.
    
-   Does not trigger a Spark job.
    

**Cmd 3 – `select("*", "values.*")`**

-   Core transformation logic; required.
    

**Cmd 4 – `drop("values")`**

-   Core transformation logic; required.
    

**Cmd 5 – `explain()`**

-   Prints the query plan only; does not execute the job.
    
-   Acceptable (though optional) in production.
    

---

**Key Takeaway (Interview-Ready)**

-   **Never use `display()` in scheduled Databricks jobs**.
    
-   It is meant for **interactive analysis**, not production pipelines.
    
-   Production notebooks should focus on **data transformations and writes only**.

---

### Question 62

The business reporting team requires that data for their dashboards be updated every hour. The total processing time for the pipeline that extracts transforms, and loads the data for their pipeline runs in 10 minutes.Assuming normal operating conditions, which configuration will meet their service-level agreement requirements with the lowest cost?

- A.Manually trigger a job anytime the business reporting team refreshes their dashboards
- B.Schedule a job to execute the pipeline once an hour on a new job cluster
- C.Schedule a Structured Streaming job with a trigger interval of 60 minutes
- D.Schedule a job to execute the pipeline once an hour on a dedicated interactive cluster
- E.Configure a job that executes every time new data lands in a given directory

The correct answer is **B**.

**Explanation**

**B. Schedule a job to execute the pipeline once an hour on a new job cluster**

-   The SLA requires **hourly updates**, and the pipeline completes in **10 minutes**, so a simple **hourly batch schedule** fully satisfies the requirement.
    
-   Using a **job cluster** is the **lowest-cost option** because:
    
    -   The cluster is **created only when the job runs**
        
    -   It is **terminated immediately after completion**
        
    -   There are **no idle compute costs** during the remaining ~50 minutes of each hour
        
-   This is the recommended Databricks pattern for **predictable, periodic batch workloads**.
    

---

**Why Other Options Are Incorrect**

**A. Manually triggering jobs**

-   ❌ Operationally risky and does not guarantee hourly freshness.
    

**C. Structured Streaming with a 60-minute trigger**

-   ❌ Higher cost and complexity; streaming clusters run continuously and are unnecessary for simple hourly batch needs.
    

**D. Dedicated interactive cluster**

-   ❌ Most expensive option due to **always-on compute**, even when idle.
    

**E. Event-driven execution on new data arrival**

-   ❌ May trigger jobs more frequently than required, increasing cost and potentially violating the “lowest cost” constraint.
    

---

**Key Takeaway (Interview-Ready)**

-   For **hourly SLAs with short, predictable runtimes**, the most cost-effective solution is:
    
    -   **Scheduled batch jobs on ephemeral job clusters**
        
-   Streaming and always-on clusters should be reserved for **low-latency or continuously processing workloads**.

---

### Question 63

A Databricks SQL dashboard has been configured to monitor the total number of records present in a collection of Delta Lake tables using the following query pattern:SELECT COUNT (*) FROM table -Which of the following describes how results are generated each time the dashboard is updated?

- A.The total count of rows is calculated by scanning all data files
- B.The total count of rows will be returned from cached results unless REFRESH is run
- C.The total count of records is calculated from the Delta transaction logs
- D.The total count of records is calculated from the parquet file metadata
- E.The total count of records is calculated from the Hive metastore

The correct answer is **C**.

**Explanation**

**C. The total count of records is calculated from the Delta transaction logs**

-   Delta Lake maintains **transaction logs (`_delta_log`)** that include metadata such as:
    
    -   Number of rows added or removed in each commit
        
-   For queries like:
    
    sql
    
    Copy code
    
    `SELECT COUNT(*) FROM table`
    
    Databricks SQL can:
    
    -   **Aggregate row counts directly from the Delta transaction log**
        
    -   Avoid scanning Parquet data files entirely
        
-   This makes `COUNT(*)` on Delta tables **much faster and more efficient**, especially for large tables, and ideal for dashboards.
    

---

**Why Other Options Are Incorrect**

**A. Scanning all data files**

-   ❌ Incorrect. Delta avoids full scans for `COUNT(*)` when transaction log stats are available.
    

**B. Returned from cached results**

-   ❌ Incorrect. Dashboards do not rely on cached query results unless explicitly configured.
    

**D. Calculated from Parquet file metadata**

-   ❌ Incorrect. Parquet footers do not reliably store row counts in a way Databricks SQL uses for `COUNT(*)`.
    

**E. Calculated from the Hive metastore**

-   ❌ Incorrect. The Hive metastore stores schema and table metadata, not row counts.
    

---

**Key Takeaway (Interview-Ready)**

-   Delta Lake enables **metadata-only queries**.
    
-   `COUNT(*)` in Databricks SQL is optimized by using **row-count statistics stored in the Delta transaction log**, making dashboards fast and cost-efficient.

---

### Question 64

![Question 64 Image 1](examtopics_images/question_64_img_1.png)

A Delta Lake table was created with the below query:Consider the following query:DROP TABLE prod.sales_by_store -If this statement is executed by a workspace admin, which result will occur?

- A.Nothing will occur until a COMMIT command is executed.
- B.The table will be removed from the catalog but the data will remain in storage.
- C.The table will be removed from the catalog and the data will be deleted.
- D.An error will occur because Delta Lake prevents the deletion of production data.
- E.Data will be marked as deleted but still recoverable with Time Travel.

The correct answer is **C**.

**Explanation**

**C. The table will be removed from the catalog and the data will be deleted.**

-   The table was created using:
    
    sql
    
    Copy code
    
    `CREATE TABLE prod.sales_by_store AS (SELECT …)`
    
-   No `LOCATION` clause is specified, so this is a **managed Delta table**.
    
-   For **managed tables** in Databricks:
    
    -   `DROP TABLE` removes the table metadata from the metastore
        
    -   **AND deletes the underlying data files** from storage
        
-   A workspace admin has sufficient privileges to perform this operation.
    

This is immediate and does not require any additional commit step.

---

**Why Other Options Are Incorrect**

**A. Nothing will occur until a COMMIT command is executed.**

-   ❌ Incorrect. Databricks SQL auto-commits DDL statements.
    

**B. The table will be removed from the catalog but the data will remain in storage.**

-   ❌ Incorrect. This behavior applies to **external tables**, not managed tables.
    

**D. An error will occur because Delta Lake prevents deletion of production data.**

-   ❌ Incorrect. Delta Lake does not distinguish “production” tables.
    

**E. Data will be marked as deleted but still recoverable with Time Travel.**

-   ❌ Incorrect. Time Travel applies to table versions, not to tables that have been dropped entirely.
    

---

**Key Takeaway (Interview-Ready)**

-   **DROP TABLE on a managed Delta table deletes both metadata and data**.
    
-   To preserve data on drop, tables must be created as **external tables** using an explicit `LOCATION`.

---

### Question 65

Two of the most common data locations on Databricks are the DBFS root storage and external object storage mounted with dbutils.fs.mount().Which of the following statements is correct?

- A.DBFS is a file system protocol that allows users to interact with files stored in object storage using syntax and guarantees similar to Unix file systems.
- B.By default, both the DBFS root and mounted data sources are only accessible to workspace administrators.
- C.The DBFS root is the most secure location to store data, because mounted storage volumes must have full public read and write permissions.
- D.Neither the DBFS root nor mounted storage can be accessed when using %sh in a Databricks notebook.
- E.The DBFS root stores files in ephemeral block volumes attached to the driver, while mounted directories will always persist saved data to external storage between sessions.

The correct answer is **A**.

**Explanation**

**A. DBFS is a file system protocol that allows users to interact with files stored in object storage using syntax and guarantees similar to Unix file systems.**

-   **DBFS (Databricks File System)** provides a **filesystem-like abstraction** over cloud object storage (S3, ADLS, GCS).
    
-   It allows users to:
    
    -   Use familiar paths (e.g., `/dbfs/...`)
        
    -   Interact with files using Unix-like commands (`ls`, `cp`, `rm`) and APIs
        
-   While object storage is not a true POSIX filesystem, DBFS **bridges that gap** for usability within Databricks.
    

---

**Why Other Options Are Incorrect**

**B. Both locations are only accessible to workspace administrators.**

-   ❌ Incorrect. Access depends on **workspace permissions, cluster access, and cloud IAM**, not admin-only by default.
    

**C. DBFS root is the most secure because mounts must be public.**

-   ❌ Incorrect. Mounted storage uses **cloud IAM credentials** and does *not* require public access.
    

**D. Neither location can be accessed using `%sh`.**

-   ❌ Incorrect. Both are accessible via `%sh` using the `/dbfs` path:
    
    bash
    
    Copy code
    
    `ls /dbfs/mnt/...`
    

**E. DBFS root is ephemeral block storage.**

-   ❌ Incorrect. The DBFS root is backed by **persistent object storage**, not ephemeral driver disks.
    

---

**Key Takeaway (Interview-Ready)**

-   **DBFS is an abstraction layer** that lets Databricks users work with cloud object storage using familiar filesystem semantics.
    
-   Both **DBFS root and mounted storage are persistent** and accessible from notebooks, jobs, and shell commands.

---

### Question 66

![Question 66 Image 1](examtopics_images/question_66_img_1.png)

The following code has been migrated to a Databricks notebook from a legacy workload:The code executes successfully and provides the logically correct results, however, it takes over 20 minutes to extract and load around 1 GB of data.Which statement is a possible explanation for this behavior?

- A.%sh triggers a cluster restart to collect and install Git. Most of the latency is related to cluster startup time.
- B.Instead of cloning, the code should use %sh pip install so that the Python code can get executed in parallel across all nodes in a cluster.
- C.%sh does not distribute file moving operations; the final line of code should be updated to use %fs instead.
- D.Python will always execute slower than Scala on Databricks. The run.py script should be refactored to Scala.
- E.%sh executes shell code on the driver node. The code does not take advantage of the worker nodes or Databricks optimized Spark.

The correct answer is **E**.

**Explanation**

**E. `%sh` executes shell code on the driver node. The code does not take advantage of the worker nodes or Databricks-optimized Spark.**

-   The notebook uses:
    
    bash
    
    Copy code
    
    `%sh git clone … python ./data_loader/run.py mv ./output /dbfs/mnt/new_data`
    
-   `%sh` runs **only on the driver node**, not across executors.
    
-   The Python script (`run.py`) is executed as a **single-node process**, so:
    
    -   No Spark parallelism is used
        
    -   Worker nodes remain idle
        
-   Processing ~1 GB of data serially on the driver can easily take **20+ minutes**, even though the logic is correct.
    

To improve performance, the logic should be refactored to:

-   Use **Spark APIs (PySpark / Spark SQL)** for distributed execution
    
-   Avoid heavy data movement via shell commands
    

---

**Why Other Options Are Incorrect**

**A. `%sh` triggers a cluster restart**

-   ❌ Incorrect. `%sh` runs immediately on the active cluster.
    

**B. Using `%sh pip install` enables parallel execution**

-   ❌ Incorrect. Installing packages does not make Python scripts distributed.
    

**C. `%sh` should be replaced with `%fs`**

-   ❌ Incorrect. `%fs` is for filesystem commands, not distributed compute.
    

**D. Python is always slower than Scala**

-   ❌ Incorrect. PySpark executes on the JVM; performance differences are minimal for Spark workloads.
    

---

**Key Takeaway (Interview-Ready)**

-   `%sh` is **driver-only** execution.
    
-   Heavy ETL logic in `%sh` **bypasses Spark parallelism**, leading to poor performance.
    
-   For scalable workloads on Databricks, always use **Spark APIs**, not shell scripts.

---

### Question 67

The data science team has requested assistance in accelerating queries on free form text from user reviews. The data is currently stored in Parquet with the below schema:item_id INT, user_id INT, review_id INT, rating FLOAT, review STRINGThe review column contains the full text of the review left by the user. Specifically, the data science team is looking to identify if any of 30 key words exist in this field.A junior data engineer suggests converting this data to Delta Lake will improve query performance.Which response to the junior data engineer s suggestion is correct?

- A.Delta Lake statistics are not optimized for free text fields with high cardinality.
- B.Text data cannot be stored with Delta Lake.
- C.ZORDER ON review will need to be run to see performance gains.
- D.The Delta log creates a term matrix for free text fields to support selective filtering.
- E.Delta Lake statistics are only collected on the first 4 columns in a table.

The correct answer is **A**.

**Explanation**

**A. Delta Lake statistics are not optimized for free text fields with high cardinality.**

-   Delta Lake improves performance mainly through:
    
    -   **File-level statistics** (min/max, null counts)
        
    -   **Data skipping**
        
    -   **Partitioning and Z-ordering**
        
-   These optimizations work best for:
    
    -   Numeric columns
        
    -   Low-to-moderate cardinality columns
        
    -   Columns commonly used in range or equality predicates
        
-   A **free-form text column** like `review`:
    
    -   Has **very high cardinality**
        
    -   Does not benefit meaningfully from min/max statistics
        
    -   Cannot be efficiently filtered using Delta’s data-skipping mechanisms for keyword search
        
-   Converting Parquet → Delta alone will **not materially speed up keyword searches** across large text fields.
    

For this use case, better solutions include:

-   Full-text search engines (e.g., Elasticsearch / OpenSearch)
    
-   Databricks **Vector Search** or embeddings + ANN search
    
-   Pre-tokenization or inverted indexes built explicitly for text search
    

---

**Why Other Options Are Incorrect**

**B. Text data cannot be stored with Delta Lake.**

-   ❌ Incorrect. Delta Lake fully supports `STRING` columns.
    

**C. ZORDER ON review will need to be run.**

-   ❌ Incorrect. Z-ordering free text columns provides little to no benefit.
    

**D. The Delta log creates a term matrix for free text fields.**

-   ❌ Incorrect. Delta Lake does not build inverted indexes or term matrices.
    

**E. Delta Lake statistics are only collected on the first 4 columns.**

-   ❌ Incorrect. By default, statistics are collected on the **first 32 columns**.
    

---

**Key Takeaway (Interview-Ready)**

-   Delta Lake is **not a text search engine**.
    
-   For keyword search on large free-text fields, use **search or vector-based solutions**, not Delta statistics or Z-ordering alone.

---

### Question 68

Assuming that the Databricks CLI has been installed and configured correctly, which Databricks CLI command can be used to upload a custom Python Wheel to object storage mounted with the DBFS for use with a production job?

- A.configure
- B.fs
- C.jobs
- D.libraries
- E.workspace

The correct answer is **B**.

**Explanation**

**B. `fs`**

-   The Databricks CLI **`fs`** command is used to interact with **DBFS (Databricks File System)**.
    
-   To upload a custom Python wheel (`.whl`) to object storage mounted in DBFS, you would use a command like:
    
    bash
    
    Copy code
    
    `databricks fs cp my_package-1.0.0-py3-none-any.whl dbfs:/mnt/libs/my_package-1.0.0-py3-none-any.whl`
    
-   Once uploaded to DBFS, the wheel can be attached to:
    
    -   A **job cluster**
        
    -   An **interactive cluster**
        
    -   A **Databricks job** as a library
        

---

**Why Other Options Are Incorrect**

**A. `configure`**

-   Used to set up authentication and profiles, not file transfer.
    

**C. `jobs`**

-   Used to create, update, or run jobs—not to upload files.
    

**D. `libraries`**

-   Used to **install libraries**, not upload them to storage.
    

**E. `workspace`**

-   Used to import/export notebooks and workspace files, not DBFS data.
    

---

**Key Takeaway (Interview-Ready)**

-   To upload files (including Python wheels) to DBFS using the Databricks CLI, always use:
    
    bash
    
    Copy code
    
    `databricks fs cp`

---

### Question 69

![Question 69 Image 1](examtopics_images/question_69_img_1.png)

![Question 69 Image 2](examtopics_images/question_69_img_2.png)

The business intelligence team has a dashboard configured to track various summary metrics for retail stores. This includes total sales for the previous day alongside totals and averages for a variety of time periods. The fields required to populate this dashboard have the following schema:For demand forecasting, the Lakehouse contains a validated table of all itemized sales updated incrementally in near real-time. This table, named products_per_order, includes the following fields:Because reporting on long-term sales trends is less volatile, analysts using the new dashboard only require data to be refreshed once daily. Because the dashboard will be queried interactively by many users throughout a normal business day, it should return results quickly and reduce total compute associated with each materialization.Which solution meets the expectations of the end users while controlling and limiting possible costs?

- A.Populate the dashboard by configuring a nightly batch job to save the required values as a table overwritten with each update.
- B.Use Structured Streaming to configure a live dashboard against the products_per_order table within a Databricks notebook.
- C.Configure a webhook to execute an incremental read against products_per_order each time the dashboard is refreshed.
- D.Use the Delta Cache to persist the products_per_order table in memory to quickly update the dashboard with each query.
- E.Define a view against the products_per_order table and define the dashboard against this view.

The correct answer is **A**.

**Explanation**

**A. Populate the dashboard by configuring a nightly batch job to save the required values as a table overwritten with each update.**

-   The dashboard:
    
    -   Is queried **interactively by many users**
        
    -   Needs to be **fast**
        
    -   Only requires data to be refreshed **once per day**
        
-   The source table (`products_per_order`) is:
    
    -   **Incrementally updated in near real-time**
        
    -   Well-suited as a **raw/validated fact table**, not for repeated heavy aggregations
        
-   The most cost-effective and performant pattern is to:
    
    -   Run a **nightly batch job**
        
    -   **Precompute all required aggregates**
        
    -   Store results in a **materialized summary table**
        
    -   **Overwrite** it each day to ensure correctness
        

This ensures:

-   Minimal compute during business hours
    
-   Very fast dashboard queries
    
-   Predictable and low overall cost
    

---

**Why Other Options Are Incorrect**

**B. Live dashboard using Structured Streaming**

-   ❌ Overkill. Streaming is unnecessary when freshness is daily and increases cost/complexity.
    

**C. Incremental read on every dashboard refresh**

-   ❌ Expensive and slow; recomputes aggregates repeatedly under user load.
    

**D. Delta Cache on the raw table**

-   ❌ Still requires scanning and aggregating large volumes of data per query.
    

**E. View over the raw table**

-   ❌ Views do not materialize results; every query recomputes aggregates.
    

---

**Key Takeaway (Interview-Ready)**

-   For **high-concurrency BI dashboards with relaxed freshness requirements**, the best practice is:
    
    -   **Precompute aggregates on a schedule**
        
    -   **Materialize them into a summary table**
        
-   This delivers the **fastest queries at the lowest cost**.


---

### Question 70

A data ingestion task requires a one-TB JSON dataset to be written out to Parquet with a target part-file size of 512 MB. Because Parquet is being used instead of Delta Lake, built-in file-sizing features such as Auto-Optimize & Auto-Compaction cannot be used.Which strategy will yield the best performance without shuffling data?

- A.Set spark.sql.files.maxPartitionBytes to 512 MB, ingest the data, execute the narrow transformations, and then write to parquet.
- B.Set spark.sql.shuffle.partitions to 2,048 partitions (1TB*1024*1024/512), ingest the data, execute the narrow transformations, optimize the data by sorting it (which automatically repartitions the data), and then write to parquet.
- C.Set spark.sql.adaptive.advisoryPartitionSizeInBytes to 512 MB bytes, ingest the data, execute the narrow transformations, coalesce to 2,048 partitions (1TB*1024*1024/512), and then write to parquet.
- D.Ingest the data, execute the narrow transformations, repartition to 2,048 partitions (1TB* 1024*1024/512), and then write to parquet.
- E.Set spark.sql.shuffle.partitions to 512, ingest the data, execute the narrow transformations, and then write to parquet.

The correct answer is **C**.

**Explanation**

**C. Set `spark.sql.adaptive.advisoryPartitionSizeInBytes` to 512 MB, ingest the data, execute the narrow transformations, coalesce to 2,048 partitions, and then write to Parquet.**

-   The requirement is:
    
    -   **Target Parquet file size ≈ 512 MB**
        
    -   **Best performance**
        
    -   **No shuffle**
        
-   To get ~512 MB files from ~1 TB of data, you need about:
    
    lua
    
    Copy code
    
    `1 TB / 512 MB ≈ 2,048 output files`
    
-   **`coalesce()`**:
    
    -   Reduces the number of partitions
        
    -   Is a **narrow transformation**
        
    -   **Does not trigger a shuffle**
        
-   Setting **`spark.sql.adaptive.advisoryPartitionSizeInBytes`** to 512 MB allows Adaptive Query Execution (AQE) to:
    
    -   Aim for partitions close to the desired size
        
    -   Improve partition sizing without forcing a shuffle
        
-   This combination achieves the target file size efficiently and safely.
    

---

**Why Other Options Are Incorrect**

**A. `spark.sql.files.maxPartitionBytes`**

-   ❌ Controls **input split size**, not output file size.
    

**B. Sorting to optimize data**

-   ❌ Sorting and repartitioning **forces a shuffle**, explicitly disallowed.
    

**D. `repartition()` to 2,048 partitions**

-   ❌ `repartition()` always triggers a **full shuffle**.
    

**E. `spark.sql.shuffle.partitions = 512`**

-   ❌ Affects shuffle stages only and does not guarantee correct output file sizing.
    

---

**Key Takeaway (Interview-Ready)**

-   To control Parquet file sizes **without shuffling**:
    
    -   Use **`coalesce()`**, not `repartition()`
        
    -   Let **AQE advisory partition sizing** guide partition size
        
-   Shuffles are expensive; avoid them unless absolutely necessary.

---

### Question 71

![Question 71 Image 1](examtopics_images/question_71_img_1.png)

A junior data engineer has been asked to develop a streaming data pipeline with a grouped aggregation using DataFrame df. The pipeline needs to calculate the average humidity and average temperature for each non-overlapping five-minute interval. Incremental state information should be maintained for 10 minutes for late-arriving data.Streaming DataFrame df has the following schema:"device_id INT, event_time TIMESTAMP, temp FLOAT, humidity FLOAT"Code block:Choose the response that correctly fills in the blank within the code block to complete this task.

- A.withWatermark("event_time", "10 minutes")
- B.awaitArrival("event_time", "10 minutes")
- C.await("event_time + ‘10 minutes'")
- D.slidingWindow("event_time", "10 minutes")
- E.delayWrite("event_time", "10 minutes")

The correct answer is **A**.

**A. `withWatermark("event_time", "10 minutes")`**

**Explanation**

-   The pipeline requires:
    
    -   **Event-time windowed aggregation** (`window("event_time", "5 minutes")`)
        
    -   **Handling late-arriving data**
        
    -   **Maintaining incremental state for 10 minutes**
        
-   In **Structured Streaming**, late data handling and state cleanup are controlled using **watermarks**.
    
-   `withWatermark("event_time", "10 minutes")`:
    
    -   Tells Spark to **keep aggregation state for 10 minutes**
        
    -   Allows records arriving up to **10 minutes late** to be included
        
    -   Enables **safe state eviction** after the watermark passes
        

This is the **correct and required API** for managing late-arriving event-time data in windowed aggregations.

---

**Why Other Options Are Incorrect**

**B. `awaitArrival(...)`**

-   ❌ Not a valid Structured Streaming API.
    

**C. `await(...)`**

-   ❌ Not a valid API and incorrect semantics.
    

**D. `slidingWindow(...)`**

-   ❌ Window definition is already handled by `window("event_time", "5 minutes")`; this does not control lateness.
    

**E. `delayWrite(...)`**

-   ❌ Not a valid Structured Streaming API.
    

---

**Final Correct Code Snippet**


```
df \
  .withWatermark("event_time", "10 minutes") \
  .groupBy(
      window("event_time", "5 minutes").alias("time"),
      "device_id"
  ) \
  .agg(
      avg("temp").alias("avg_temp"),
      avg("humidity").alias("avg_humidity")
  ) \
  .writeStream \
  .format("delta") \
  .saveAsTable("sensor_avg")
```

---

**Key Takeaway (Interview-Ready)**

-   **Late data handling in Structured Streaming = `withWatermark()`**
    
-   Window duration controls **aggregation buckets**
    
-   Watermark duration controls **state retention and late-data tolerance**

---

### Question 72

![Question 72 Image 1](examtopics_images/question_72_img_1.png)

![Question 72 Image 2](examtopics_images/question_72_img_2.png)

A data team's Structured Streaming job is configured to calculate running aggregates for item sales to update a downstream marketing dashboard. The marketing team has introduced a new promotion, and they would like to add a new field to track the number of times this promotion code is used for each item. A junior data engineer suggests updating the existing query as follows. Note that proposed changes are in bold.Original query:Proposed query:Proposed query:.start(“/item_agg”)Which step must also be completed to put the proposed query into production?

- A.Specify a new checkpointLocation
- B.Increase the shuffle partitions to account for additional aggregates
- C.Run REFRESH TABLE delta.'/item_agg'
- D.Register the data in the "/item_agg" directory to the Hive metastore
- E.Remove .option(‘mergeSchema’, ‘true’) from the streaming write

The correct answer is **A**.

**A. Specify a new `checkpointLocation`**

**Explanation**

-   This is a **stateful Structured Streaming aggregation** (`groupBy` + `agg` + `outputMode("complete")`).
    
-   The checkpoint stores:
    
    -   Aggregation state
        
    -   Query metadata
        
    -   Output schema information
        
-   The proposed change **adds a new aggregated field** (tracking promotion usage), which **changes the output schema and state definition**.
    
-   Structured Streaming **does not allow schema or state changes to reuse an existing checkpoint**.
    

Therefore, to put the modified query into production, you **must use a new checkpoint location** so Spark can:

-   Initialize new state
    
-   Avoid corrupting or misinterpreting existing state
    

---

**Why Other Options Are Incorrect**

**B. Increase shuffle partitions**

-   ❌ Not required for correctness; this is a performance tuning concern, not a deployment requirement.
    

**C. Run REFRESH TABLE**

-   ❌ Applies to metadata caching, not streaming state.
    

**D. Register the data directory to the Hive metastore**

-   ❌ The table is already being written to `/item_agg`; this does not address streaming state.
    

**E. Remove `mergeSchema`**

-   ❌ `mergeSchema` is unrelated to checkpoint compatibility and is not shown as part of the query change.
    

---

**Key Takeaway (Interview-Ready)**

-   **Any change to a stateful Structured Streaming query (schema, aggregation, grouping)** requires a **new checkpoint location**.
    
-   Reusing an old checkpoint with a modified query will cause the stream to fail or behave incorrectly.

---

### Question 73

A Structured Streaming job deployed to production has been resulting in higher than expected cloud storage costs. At present, during normal execution, each microbatch of data is processed in less than 3s; at least 12 times per minute, a microbatch is processed that contains 0 records. The streaming write was configured using the default trigger settings. The production job is currently scheduled alongside many other Databricks jobs in a workspace with instance pools provisioned to reduce start-up time for jobs with batch execution.Holding all other variables constant and assuming records need to be processed in less than 10 minutes, which adjustment will meet the requirement?

- A.Set the trigger interval to 3 seconds; the default trigger interval is consuming too many records per batch, resulting in spill to disk that can increase volume costs.
- B.Increase the number of shuffle partitions to maximize parallelism, since the trigger interval cannot be modified without modifying the checkpoint directory.
- C.Set the trigger interval to 10 minutes; each batch calls APIs in the source storage account, so decreasing trigger frequency to maximum allowable threshold should minimize this cost.
- D.Set the trigger interval to 500 milliseconds; setting a small but non-zero trigger interval ensures that the source is not queried too frequently.
- E.Use the trigger once option and configure a Databricks job to execute the query every 10 minutes; this approach minimizes costs for both compute and storage.

The correct answer is **C**.

**Explanation**

-   With **default trigger settings**, Structured Streaming effectively runs **as fast as possible**, frequently checking the source for new data.
    
-   The job processes data quickly (<3s per microbatch), but **many microbatches contain 0 records**.
    
-   Even empty microbatches:
    
    -   Poll the source
        
    -   Make metadata and API calls to cloud storage
        
    -   Contribute to **unexpected storage and request costs**
        

The requirement states:

-   Records only need to be processed in **less than 10 minutes**
    

By **setting the trigger interval to 10 minutes**:

-   Spark checks the source **far less frequently**
    
-   Empty microbatches are dramatically reduced
    
-   Cloud storage API calls (and their cost) are minimized
    
-   The latency requirement (<10 minutes) is still satisfied
    

This is the **lowest-risk, lowest-cost adjustment** while keeping the streaming job intact.

---

**Why the other options are incorrect**

**A. Set trigger interval to 3 seconds**

-   ❌ Makes the problem worse by **increasing polling frequency**.
    

**B. Increase shuffle partitions**

-   ❌ Shuffle tuning does not address empty microbatches or storage API calls.
    
-   ❌ Trigger intervals *can* be changed without changing checkpoints.
    

**D. Set trigger interval to 500 ms**

-   ❌ Extremely frequent polling → **higher costs**, not lower.
    

**E. Use `trigger once` every 10 minutes**

-   ❌ Converts the workload into a **batch-style pattern**
    
-   ❌ Adds job orchestration overhead
    
-   ❌ Not holding execution variables constant relative to the current streaming setup
    

---

**Key Takeaway (Interview-Ready)**

-   **Empty microbatches still cost money** due to source polling.
    
-   If latency requirements are relaxed, **increase the trigger interval**.
    
-   For low-volume or bursty streams, **longer trigger intervals are often the most cost-effective choice**

---

### Question 74

Which statement describes the correct use of pyspark.sql.functions.broadcast?

- A.It marks a column as having low enough cardinality to properly map distinct values to available partitions, allowing a broadcast join.
- B.It marks a column as small enough to store in memory on all executors, allowing a broadcast join.
- C.It caches a copy of the indicated table on attached storage volumes for all active clusters within a Databricks workspace.
- D.It marks a DataFrame as small enough to store in memory on all executors, allowing a broadcast join.
- E.It caches a copy of the indicated table on all nodes in the cluster for use in all future queries during the cluster lifetime.

The correct answer is **D**.

**Explanation**

**D. It marks a DataFrame as small enough to store in memory on all executors, allowing a broadcast join.**

-   `pyspark.sql.functions.broadcast()` is a **join hint**.
    
-   It tells Spark’s optimizer that the **entire DataFrame is small enough to be broadcast** to all executors.
    
-   Spark then:
    
    -   Sends a copy of this DataFrame to every executor
        
    -   Avoids a shuffle on the larger DataFrame
        
    -   Performs a **broadcast hash join**, which is usually much faster
        

Example:

python

Copy code

`from pyspark.sql.functions import broadcast  large_df.join(broadcast(small_df), "key")`

---

**Why Other Options Are Incorrect**

**A. Marks a column as having low cardinality**

-   ❌ Incorrect. Broadcast applies to a **DataFrame**, not a column, and has nothing to do with cardinality mapping.
    

**B. Marks a column as small enough to store in memory**

-   ❌ Incorrect. Spark broadcasts **tables/DataFrames**, not individual columns.
    

**C. Caches a copy on attached storage volumes for all clusters**

-   ❌ Incorrect. Broadcast is **in-memory**, executor-scoped, and query-specific.
    

**E. Caches a copy on all nodes for all future queries**

-   ❌ Incorrect. Broadcast does **not persist data across queries** or act like a cache.
    

---

**Key Takeaway (Interview-Ready)**

-   `broadcast()` is a **hint**, not a cache.
    
-   It applies to a **DataFrame**, not a column.
    
-   Use it when one side of a join is **small enough to fit in memory on every executor**, to avoid costly shuffles.

---

### Question 75

A data engineer is configuring a pipeline that will potentially see late-arriving, duplicate records.In addition to de-duplicating records within the batch, which of the following approaches allows the data engineer to deduplicate data against previously processed records as it is inserted into a Delta table?

- A.Set the configuration delta.deduplicate = true.
- B.VACUUM the Delta table after each batch completes.
- C.Perform an insert-only merge with a matching condition on a unique key.
- D.Perform a full outer join on a unique key and overwrite existing data.
- E.Rely on Delta Lake schema enforcement to prevent duplicate records.

The correct answer is **C**.

**C. Perform an insert-only merge with a matching condition on a unique key.**

**Explanation**

-   To deduplicate **against previously processed records**, the pipeline must compare incoming data with **existing data already stored in the Delta table**.
    
-   Delta Lake provides this capability via **`MERGE INTO`**.
    
-   An **insert-only merge** pattern looks like:
    

sql

Copy code

`MERGE INTO target t USING source s ON t.unique_id = s.unique_id WHEN NOT MATCHED THEN   INSERT *`

-   This ensures:
    
    -   New records are inserted
        
    -   Records with a matching unique key already present in the table are **ignored**
        
-   This approach works for:
    
    -   Late-arriving data
        
    -   Replayed batches
        
    -   Exactly-once–like semantics at the table level
        

---

**Why the other options are incorrect**

**A. `delta.deduplicate = true`**  
❌ No such Delta configuration exists.

**B. VACUUM after each batch**  
❌ VACUUM removes old files, not duplicate rows.

**D. Full outer join and overwrite**  
❌ Expensive, risky, and unnecessary; overwrites can introduce data loss.

**E. Schema enforcement**  
❌ Schema enforcement validates **structure and types**, not row-level uniqueness.

---

**Key Takeaway (Interview-Ready)**

-   **Delta Lake does not enforce primary keys automatically**.
    
-   To deduplicate across batches or over time, use an **insert-only MERGE on a unique key**.
    
-   This is the standard, production-safe pattern for handling **late and duplicate data** in Delta Lake.

---

### Question 76

A data pipeline uses Structured Streaming to ingest data from Apache Kafka to Delta Lake. Data is being stored in a bronze table, and includes the Kafka-generated timestamp, key, and value. Three months after the pipeline is deployed, the data engineering team has noticed some latency issues during certain times of the day.A senior data engineer updates the Delta Table's schema and ingestion logic to include the current timestamp (as recorded by Apache Spark) as well as the Kafka topic and partition. The team plans to use these additional metadata fields to diagnose the transient processing delays.Which limitation will the team face while diagnosing this problem?

- A.New fields will not be computed for historic records.
- B.Spark cannot capture the topic and partition fields from a Kafka source.
- C.New fields cannot be added to a production Delta table.
- D.Updating the table schema will invalidate the Delta transaction log metadata.
- E.Updating the table schema requires a default value provided for each field added.

The correct answer is **A**.

**A. New fields will not be computed for historic records.**

**Explanation**

-   The pipeline has been running for **three months**, and the new metadata fields (Spark processing timestamp, Kafka topic, Kafka partition) are being added **now**.
    
-   In Delta Lake:
    
    -   **Schema evolution allows adding new columns**
        
    -   But **existing records are not retroactively recomputed**
        
-   As a result:
    
    -   Historic records written before the schema change will have **NULL values** for the newly added columns
        
    -   Only records ingested **after** the change will contain these new metadata fields
        

This limits diagnosis because:

-   You cannot analyze historical latency patterns using the new fields
    
-   Comparisons across the full three-month window will be incomplete
    

---

**Why Other Options Are Incorrect**

**B. Spark cannot capture topic and partition from Kafka**  
❌ Incorrect. Spark Kafka sources expose `topic`, `partition`, `offset`, and `timestamp`.

**C. New fields cannot be added to a production Delta table**  
❌ Incorrect. Delta Lake fully supports schema evolution in production.

**D. Updating schema invalidates the Delta transaction log**  
❌ Incorrect. Schema changes are tracked safely in the transaction log.

**E. Default values are required for new fields**  
❌ Incorrect. Delta allows adding nullable columns without defaults.

---

**Key Takeaway (Interview-Ready)**

-   Delta Lake **schema evolution is forward-only**.
    
-   New columns apply **only to newly written data**.
    
-   If historical analysis is required, you must:
    
    -   Backfill data, or
        
    -   Reprocess from the source, or
        
    -   Derive insights using existing historical fields

---

### Question 77

![Question 77 Image 1](examtopics_images/question_77_img_1.png)

In order to facilitate near real-time workloads, a data engineer is creating a helper function to leverage the schema detection and evolution functionality of Databricks Auto Loader. The desired function will automatically detect the schema of the source directly, incrementally process JSON files as they arrive in a source directory, and automatically evolve the schema of the table when new fields are detected.The function is displayed below with a blank:Which response correctly fills in the blank to meet the specified requirements?

- A.
- B.
- C.
- D.
- E.
- ![alt text](examtopics_images/question_77_img_2.png)

The correct answer is **E**.

**Explanation**

The requirements are:

-   Incrementally process JSON files as they arrive (near real-time)
    
-   Automatically detect and evolve schema (Auto Loader schema evolution)
    
-   Use **Structured Streaming**, not batch
    
-   Maintain state using a **checkpoint**
    

To meet these requirements, the solution must:

1.  Use **`writeStream`** (not `write`)
    
2.  Enable schema evolution with **`option("mergeSchema", True)`**
    
3.  Provide a **`checkpointLocation`** so Auto Loader can track progress and state
    
4.  Continuously write to the target table/path
    

Option **E** is the only choice that:

-   Uses **Structured Streaming**
    
-   Enables **schema evolution**
    
-   Includes a **checkpoint location**
    
-   Starts a continuously running stream

**Why the other options are incorrect**
-   **A** – Uses `writeStream` but **does not specify a checkpoint**, which is required for streaming Auto Loader.

-   **B** – `trigger(once=True)` turns this into a batch-style job, not near real-time.
    
-   **C** – Uses `.write` (batch), not streaming.
    
-   **D** – Uses batch write semantics (`mode("append")`), not streaming.
    

---

**Final Answer:** ✅ **E**

**Interview-ready takeaway**

> Auto Loader with schema evolution requires **Structured Streaming**, `mergeSchema = true`, and a **checkpointLocation**. Any solution missing one of these will not meet near real-time ingestion requirements.

---

### Question 78

![Question 78 Image 1](examtopics_images/question_78_img_1.png)

The data engineering team maintains the following code:Assuming that this code produces logically correct results and the data in the source table has been de-duplicated and validated, which statement describes what will occur when this code is executed?

- A.The silver_customer_sales table will be overwritten by aggregated values calculated from all records in the gold_customer_lifetime_sales_summary table as a batch job.
- B.A batch job will update the gold_customer_lifetime_sales_summary table, replacing only those rows that have different values than the current version of the table, using customer_id as the primary key.
- C.The gold_customer_lifetime_sales_summary table will be overwritten by aggregated values calculated from all records in the silver_customer_sales table as a batch job.
- D.An incremental job will leverage running information in the state store to update aggregate values in the gold_customer_lifetime_sales_summary table.
- E.An incremental job will detect if new rows have been written to the silver_customer_sales table; if new rows are detected, all aggregates will be recalculated and used to overwrite the gold_customer_lifetime_sales_summary table.

The correct answer is **C**.

**C. The `gold_customer_lifetime_sales_summary` table will be overwritten by aggregated values calculated from all records in the `silver_customer_sales` table as a batch job.**

**Explanation**

-   The code uses:
    
    ```
    spark.table("silver_customer_sales") \
    .groupBy("customer_id") \
    .agg(...) \
    .write \
    .mode("overwrite") \
    .table("gold_customer_lifetime_sales_summary")
    ```
    
-   This is a **batch job**, not streaming:
    
    -   No `readStream` / `writeStream`
        
    -   No checkpointing or state store
        
-   The aggregation:
    
    -   Recomputes **all aggregates from the full `silver_customer_sales` table**
        
    -   Groups by `customer_id`
        
-   The write mode is **`overwrite`**, which means:
    
    -   The **entire target table** (`gold_customer_lifetime_sales_summary`) is replaced
        
    -   There is **no incremental update**, merge, or row-level comparison
        

---

**Why the other options are incorrect**

-   **A** ❌ Reverses source and target tables.
    
-   **B** ❌ Describes an upsert/merge behavior, which is not present.
    
-   **D** ❌ Describes streaming with a state store; this is not a streaming job.
    
-   **E** ❌ Implies change detection and conditional recomputation; not supported here.
    

---

**Key takeaway (interview-ready)**

-   `.write.mode("overwrite")` + batch aggregation = **full recomputation and full table replacement**
    
-   Incremental updates require **Structured Streaming**, **MERGE**, or **CDC/CDF-based logic**
    
-   Gold-layer summary tables are commonly built using this **batch overwrite pattern** when correctness is prioritized over incremental efficiency

---

### Question 79

The data architect has mandated that all tables in the Lakehouse should be configured as external (also known as "unmanaged") Delta Lake tables.Which approach will ensure that this requirement is met?

- A.When a database is being created, make sure that the LOCATION keyword is used.
- B.When configuring an external data warehouse for all table storage, leverage Databricks for all ELT.
- C.When data is saved to a table, make sure that a full file path is specified alongside the Delta format.
- D.When tables are created, make sure that the EXTERNAL keyword is used in the CREATE TABLE statement.
- E.When the workspace is being configured, make sure that external cloud object storage has been mounted.

The correct answer is **C. When data is saved to a table, make sure that a full file path is specified alongside the Delta format.**

**Explanation**

In Delta Lake, the distinction between managed and external (unmanaged) tables is determined by whether you specify a `LOCATION` when creating the table:

-   **Managed tables**: Delta Lake manages both the metadata and the underlying data files. When you drop the table, both metadata and data are deleted.
-   **External tables**: You specify where the data is stored using a `LOCATION` clause. Delta Lake only manages the metadata. When you drop the table, only the metadata is deleted; the underlying data files remain.

**Why C is correct:** When you specify a full file path (location) alongside the Delta format during table creation, you're creating an external table. For example:

sql

```sql
CREATE TABLE table_name
LOCATION 'path/to/data'
USING DELTA;
```

**Why the other options are incorrect:**

-   **A**: While using `LOCATION` at the database level sets a default location for tables in that database, it doesn't guarantee all tables will be external. Tables can still be created as managed tables within that database location.
-   **B**: Configuring an external data warehouse doesn't directly control whether individual tables are managed or unmanaged.
-   **D**: There is no `EXTERNAL` keyword in Delta Lake/Databricks SQL syntax for creating tables. The external nature is determined by the presence of the `LOCATION` clause.
-   **E**: Mounting external storage is about access configuration, not about whether tables are managed or unmanaged.

The key requirement is specifying a `LOCATION` (full file path) when creating each table to ensure it's external/unmanaged.

---

### Question 80

The marketing team is looking to share data in an aggregate table with the sales organization, but the field names used by the teams do not match, and a number of marketing-specific fields have not been approved for the sales org.Which of the following solutions addresses the situation while emphasizing simplicity?

- A.Create a view on the marketing table selecting only those fields approved for the sales team; alias the names of any fields that should be standardized to the sales naming conventions.
- B.Create a new table with the required schema and use Delta Lake's DEEP CLONE functionality to sync up changes committed to one table to the corresponding table.
- C.Use a CTAS statement to create a derivative table from the marketing table; configure a production job to propagate changes.
- D.Add a parallel table write to the current production pipeline, updating a new sales table that varies as required from the marketing table.
- E.Instruct the marketing team to download results as a CSV and email them to the sales organization.

The correct answer is **A. Create a view on the marketing table selecting only those fields approved for the sales team; alias the names of any fields that should be standardized to the sales naming conventions.**

**Explanation**

**Why A is correct:** Views provide the simplest solution that addresses all requirements:

-   **Field filtering**: SELECT only the approved fields
-   **Name aliasing**: Use AS to rename fields to match sales conventions
-   **Simplicity**: No data duplication, no additional ETL jobs needed
-   **Real-time**: Automatically reflects current data from the marketing table
-   **Maintenance**: Single source of truth with minimal overhead

Example:

sql

```sql
CREATE VIEW sales_view AS
SELECT 
    campaign_id AS opportunity_id,
    customer_name AS account_name,
    revenue_amount AS deal_value
FROM marketing_table;
```

**Why the other options are incorrect:**

-   **B (DEEP CLONE)**: Creates a complete copy of the data, requiring ongoing synchronization logic. This adds complexity and storage overhead. DEEP CLONE is useful for creating snapshots but not ideal for ongoing sharing scenarios.
-   **C (CTAS with production job)**: Creates a separate table requiring a production job to maintain. This adds operational complexity, introduces latency, and duplicates data unnecessarily.
-   **D (Parallel table write)**: Requires modifying the existing production pipeline to write to two tables simultaneously. This adds complexity to the pipeline and duplicates data storage.
-   **E (CSV email)**: Manual, non-scalable, not real-time, and introduces data governance issues. This is the opposite of a modern data platform approach.

**Key advantages of the view approach:**

-   No data duplication
-   No additional jobs or pipelines
-   Automatic updates when source data changes
-   Simple to create and maintain
-   Can apply row-level security if needed in the future

Views are the standard solution for presenting different perspectives of the same data to different users or teams while maintaining simplicity.

---

### Question 81

![Question 81 Image 1](examtopics_images/question_81_img_1.png)

A CHECK constraint has been successfully added to the Delta table named activity_details using the following logic:A batch job is attempting to insert new records to the table, including a record where latitude = 45.50 and longitude = 212.67.Which statement describes the outcome of this batch insert?

- A.The write will fail when the violating record is reached; any records previously processed will be recorded to the target table.
- B.The write will fail completely because of the constraint violation and no records will be inserted into the target table.
- C.The write will insert all records except those that violate the table constraints; the violating records will be recorded to a quarantine table.
- D.The write will include all records in the target table; any violations will be indicated in the boolean column named valid_coordinates.
- E.The write will insert all records except those that violate the table constraints; the violating records will be reported in a warning log.

The correct answer is **B. The write will fail completely because of the constraint violation and no records will be inserted into the target table.**

**Explanation**

When a CHECK constraint is added to a Delta Lake table, it enforces data quality rules at write time. The constraint `valid_coordinates` requires:
- latitude >= -90 AND latitude <= 90 AND
- longitude >= -180 AND longitude <= 180

The batch insert includes a record with:
- latitude = 45.50 (valid: within -90 to 90)
- longitude = 212.67 (invalid: exceeds 180)

**When a CHECK constraint violation occurs in Delta Lake:**
- The entire batch operation fails atomically
- **No records** from the batch are written (all-or-nothing behavior)
- Delta Lake maintains ACID properties - partial writes are not allowed
- An error is thrown indicating the constraint violation

**Why the other options are incorrect:**

**A. The write will fail when the violating record is reached; any records previously processed will be recorded**
- Incorrect because Delta Lake operations are atomic transactions
- There's no concept of "previously processed" records in a batch - either the entire batch succeeds or fails
- Partial writes would violate ACID guarantees

**C. The write will insert all records except those that violate the table constraints; the violating records will be recorded to a quarantine table**
- Incorrect - Delta Lake does not automatically create quarantine tables
- CHECK constraints don't filter/skip bad records; they reject the entire operation
- You would need to implement quarantine logic explicitly in your ETL code

**D. The write will include all records in the target table; any violations will be indicated in the boolean column named valid_coordinates**
- Incorrect - There's no automatic boolean column created
- The constraint name `valid_coordinates` is just a label, not a column
- CHECK constraints prevent invalid data from being written, not flag it

**E. The write will insert all records except those that violate the table constraints; the violating records will be reported in a warning log**
- Incorrect - CHECK constraints cause hard failures, not warnings
- Invalid records are not silently skipped
- The operation fails with an error, not a warning

**Key Takeaways (Interview-Ready)**

1. **CHECK constraints enforce strict validation**: They reject entire write operations when violations occur, ensuring data quality at the table level.

2. **ACID guarantees in Delta Lake**: All operations are atomic - a batch insert either completely succeeds or completely fails. There are no partial writes.

3. **Constraint behavior**:
   - Applied at write time (INSERT, UPDATE, MERGE)
   - Cause transaction failures when violated
   - Return explicit error messages identifying the constraint violation

4. **Handling constraint violations in production**:
   - Validate data before writing to constrained tables
   - Implement explicit quarantine/error handling in your ETL pipeline
   - Use try-catch blocks to handle constraint exceptions
   - Consider using MERGE with conditional logic for complex scenarios

5. **Best practice**: Add CHECK constraints after understanding your data distribution to avoid blocking legitimate production writes. Test thoroughly before applying to production tables.

---

### Question 82

A junior data engineer has manually configured a series of jobs using the Databricks Jobs UI. Upon reviewing their work, the engineer realizes that they are listed as the "Owner" for each job. They attempt to transfer "Owner" privileges to the "DevOps" group, but cannot successfully accomplish this task.Which statement explains what is preventing this privilege transfer?

- A.Databricks jobs must have exactly one owner; "Owner" privileges cannot be assigned to a group.
- B.The creator of a Databricks job will always have "Owner" privileges; this configuration cannot be changed.
- C.Other than the default "admins" group, only individual users can be granted privileges on jobs.
- D.A user can only transfer job ownership to a group if they are also a member of that group.
- E.Only workspace administrators can grant "Owner" privileges to a group.

The correct answer is **A. Databricks jobs must have exactly one owner; "Owner" privileges cannot be assigned to a group.**

**Explanation**

In Databricks, job ownership follows specific rules that differ from other access control patterns:

- Each Databricks job must have exactly **one owner**, and that owner must be an **individual user account**
- Ownership cannot be assigned to groups (including the "DevOps" group mentioned)
- The owner has full control over the job including the ability to edit, run, delete, and manage permissions
- While other permissions (like CAN_VIEW, CAN_MANAGE_RUN, CAN_MANAGE) can be granted to both users and groups, the "Owner" role specifically requires a single user

The junior engineer cannot transfer ownership to the "DevOps" group because groups are not eligible to be job owners. They would need to transfer ownership to a specific user within the DevOps group instead.

**Why the other options are incorrect:**

**B. The creator of a Databricks job will always have "Owner" privileges; this configuration cannot be changed**
- False. Ownership can be transferred to another user (not a group). The creator doesn't permanently retain ownership - it can be reassigned to a different individual user account.

**C. Other than the default "admins" group, only individual users can be granted privileges on jobs**
- False. Groups can be granted various job permissions like CAN_VIEW, CAN_MANAGE_RUN, and CAN_MANAGE. The restriction on groups only applies specifically to the "Owner" role, not to all privileges.

**D. A user can only transfer job ownership to a group if they are also a member of that group**
- False. This is a red herring. The issue isn't about group membership - it's that ownership cannot be transferred to any group at all, regardless of the user's membership status.

**E. Only workspace administrators can grant "Owner" privileges to a group**
- False. Even workspace administrators cannot assign job ownership to a group. The fundamental constraint is that groups cannot be owners, regardless of who is attempting the assignment.

**Key Takeaways (Interview-Ready)**

**Single User Ownership Model**: Databricks jobs follow a single-owner pattern where exactly one individual user must be designated as the owner. This ensures clear accountability and a single point of contact for job management.

**Group Permissions Available**: While ownership is restricted to individual users, groups can still be granted other meaningful permissions on jobs (CAN_VIEW, CAN_MANAGE_RUN, CAN_MANAGE), enabling team-based access control for most operational needs.

**Ownership Transfer Process**: To address the scenario in the question, the junior engineer should transfer ownership to a specific user within the DevOps team (like a DevOps lead or service account owner), then grant the DevOps group appropriate management permissions for collaborative access.

**Service Accounts Consideration**: In production environments, consider using service accounts as job owners rather than individual employee accounts to avoid issues when employees leave or change roles.

**Best Practice for Team Management**: For team-based job management, assign ownership to a designated technical lead or service account, then use group-based permissions (CAN_MANAGE) to allow the team to collaborate on job configuration and monitoring without requiring ownership transfer.

---

### Question 83

All records from an Apache Kafka producer are being ingested into a single Delta Lake table with the following schema:key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONGThere are 5 unique topics being ingested. Only the "registration" topic contains Personal Identifiable Information (PII). The company wishes to restrict access to PII. The company also wishes to only retain records containing PII in this table for 14 days after initial ingestion. However, for non-PII information, it would like to retain these records indefinitely.Which of the following solutions meets the requirements?

- A.All data should be deleted biweekly; Delta Lake's time travel functionality should be leveraged to maintain a history of non-PII information.
- B.Data should be partitioned by the registration field, allowing ACLs and delete statements to be set for the PII directory.
- C.Because the value field is stored as binary data, this information is not considered PII and no special precautions should be taken.
- D.Separate object storage containers should be specified based on the partition field, allowing isolation at the storage level.
- E.Data should be partitioned by the topic field, allowing ACLs and delete statements to leverage partition boundaries.

The correct answer is **E. Data should be partitioned by the topic field, allowing ACLs and delete statements to leverage partition boundaries.**

**Explanation**

The requirement has two key components:
1. **Access control**: Restrict access to PII (only in the "registration" topic)
2. **Retention policy**: Delete PII records after 14 days, but keep non-PII records indefinitely

Partitioning by the `topic` field provides the optimal solution because:

- **Physical isolation**: Each topic gets its own directory structure (e.g., `topic=registration/`, `topic=orders/`, etc.)
- **Granular ACLs**: Access control lists can be set at the partition level, restricting who can read the `topic=registration` partition while allowing broader access to other partitions
- **Efficient deletion**: DELETE statements can leverage partition pruning for performance:
  ```sql
  DELETE FROM kafka_table 
  WHERE topic = 'registration' 
  AND timestamp < current_timestamp() - INTERVAL 14 DAYS;
  ```
- **Performance**: The DELETE operation only scans the registration partition, not the entire table
- **Indefinite retention**: Other topic partitions remain untouched by the deletion logic

**Why the other options are incorrect:**

**A. All data should be deleted biweekly; Delta Lake's time travel functionality should be leveraged to maintain a history of non-PII information**
- Violates the requirement to retain non-PII data indefinitely. Deleting all data every two weeks and relying on time travel is not a sustainable retention strategy.
- Time travel has a retention limit (default 30 days for VACUUM operations), so this wouldn't provide indefinite retention.
- Time travel is for recovering deleted/modified data temporarily, not for implementing data retention policies.
- Unnecessarily complex and operationally risky.

**B. Data should be partitioned by the registration field, allowing ACLs and delete statements to be set for the PII directory**
- There is no "registration field" in the schema. The schema shows: key, value, topic, partition, offset, timestamp.
- This appears to confuse the topic name "registration" with a column that doesn't exist.
- Cannot partition by a non-existent field.

**C. Because the value field is stored as binary data, this information is not considered PII and no special precautions should be taken**
- Fundamentally wrong understanding of PII. Binary encoding doesn't make data non-PII.
- PII remains PII regardless of how it's stored (binary, encrypted, encoded, etc.).
- Regulatory requirements (GDPR, CCPA, etc.) apply to PII data regardless of storage format.
- This would violate compliance requirements and company policy.

**D. Separate object storage containers should be specified based on the partition field, allowing isolation at the storage level**
- The "partition" field in the schema refers to the Kafka partition number (a technical field for message ordering), not a logical data partition.
- Kafka partitions (0, 1, 2, etc.) contain mixed topics and don't provide logical data separation.
- Separating by Kafka partition number doesn't isolate PII vs non-PII data.
- This would scatter data from the same topic across multiple storage containers, making it harder to manage.

**Key Takeaways (Interview-Ready)**

**Partition Strategy for Multi-Tenant Data**: When ingesting data from multiple sources (like Kafka topics) with different security or retention requirements, partition by the discriminating field (topic) to enable efficient access control and data lifecycle management.

**Delta Lake Partition Pruning**: Partitioning enables highly efficient DELETE operations because Delta Lake can skip entire partitions that don't match the WHERE clause, avoiding full table scans. For time-based retention policies on specific partitions, this is crucial for performance at scale.

**ACLs on Partitioned Data**: Cloud object storage (S3, ADLS, GCS) allows setting access controls at the directory/prefix level. When Delta tables are partitioned, each partition becomes a separate directory, enabling granular security policies without creating separate tables.

**PII Data Classification**: Binary encoding, encryption-at-rest, or other technical storage mechanisms don't change whether data is classified as PII. PII classification is based on the semantic content, not the storage format. Compliance requirements apply regardless of encoding.

**Retention Policy Implementation**: For differential retention policies (delete some data while keeping other data), partition-based approaches combined with scheduled DELETE statements provide a clean, maintainable solution. This is preferable to creating separate tables or relying on time travel features.

**Production Pattern**: In practice, you would implement this with:
```sql
-- Partitioned table creation
CREATE TABLE kafka_ingestion (
  key BINARY, value BINARY, topic STRING, 
  partition LONG, offset LONG, timestamp LONG
)
PARTITIONED BY (topic);

-- Scheduled job for PII cleanup
DELETE FROM kafka_ingestion 
WHERE topic = 'registration' 
AND timestamp < unix_timestamp() - (14 * 86400);
```

---

### Question 84

The data architect has decided that once data has been ingested from external sources into theDatabricks Lakehouse, table access controls will be leveraged to manage permissions for all production tables and views.The following logic was executed to grant privileges for interactive queries on a production database to the core engineering group.GRANT USAGE ON DATABASE prod TO eng;GRANT SELECT ON DATABASE prod TO eng;Assuming these are the only privileges that have been granted to the eng group and that these users are not workspace administrators, which statement describes their privileges?

- A.Group members have full permissions on the prod database and can also assign permissions to other users or groups.
- B.Group members are able to list all tables in the prod database but are not able to see the results of any queries on those tables.
- C.Group members are able to query and modify all tables and views in the prod database, but cannot create new tables or views.
- D.Group members are able to query all tables and views in the prod database, but cannot create or edit anything in the database.
- E.Group members are able to create, query, and modify all tables and views in the prod database, but cannot define custom functions.

The correct answer is **D. Group members are able to query all tables and views in the prod database, but cannot create or edit anything in the database.**

**Explanation**

The two GRANT statements provide specific, limited privileges:

1. **GRANT USAGE ON DATABASE prod TO eng**
   - Allows the group to "use" the database (i.e., access and see it exists)
   - This is a prerequisite for any other operations on objects within the database
   - Without USAGE, users cannot interact with the database at all

2. **GRANT SELECT ON DATABASE prod TO eng**
   - Grants read-only query access to all current and future tables/views in the database
   - Allows running SELECT queries to retrieve data
   - Does NOT allow any modifications (INSERT, UPDATE, DELETE, MERGE)
   - Does NOT allow creating new objects (CREATE TABLE, CREATE VIEW)

Combined, these privileges allow the eng group to:
- ✅ List and see tables/views in the prod database
- ✅ Run SELECT queries to read data from tables/views
- ❌ Create new tables, views, or other objects
- ❌ Modify data (INSERT, UPDATE, DELETE)
- ❌ Modify schema (ALTER TABLE, DROP TABLE)
- ❌ Grant privileges to others

This is a classic **read-only access pattern** for data consumers.

**Why the other options are incorrect:**

**A. Group members have full permissions on the prod database and can also assign permissions to other users or groups**
- False. They only have USAGE and SELECT, which are minimal read-only privileges.
- Full permissions would require ALL PRIVILEGES or specific grants like MODIFY, CREATE, and ownership.
- Granting permissions to others requires the GRANT option (e.g., `GRANT SELECT ... WITH GRANT OPTION`), which was not provided.
- This drastically overstates their actual permissions.

**B. Group members are able to list all tables in the prod database but are not able to see the results of any queries on those tables**
- False. The SELECT privilege explicitly allows them to see query results.
- USAGE alone would allow listing tables, but SELECT grants the ability to read data.
- This understates their actual permissions - they can definitely query and see results.

**C. Group members are able to query and modify all tables and views in the prod database, but cannot create new tables or views**
- False. SELECT privilege only allows reading data, not modifying it.
- Modification requires MODIFY privilege (for INSERT, UPDATE, DELETE, MERGE operations).
- No MODIFY privilege was granted, so they cannot modify data.
- The "query" part is correct, but "modify" is incorrect.

**E. Group members are able to create, query, and modify all tables and views in the prod database, but cannot define custom functions**
- False. They cannot create or modify anything.
- Creating tables requires CREATE privilege.
- Modifying data requires MODIFY privilege.
- Neither was granted - they only have read-only access.
- This significantly overstates their permissions.

**Key Takeaways (Interview-Ready)**

**Unity Catalog Privilege Model**: Databricks uses a hierarchical privilege system where USAGE is required at each level (catalog, database) before you can access objects within them. USAGE is the "door key" that allows you to even see and reference the database.

**SELECT vs MODIFY**: These are the two fundamental data access privileges. SELECT = read-only queries. MODIFY = write operations (INSERT, UPDATE, DELETE, MERGE). They are independent - you can have one without the other.

**Database-Level Grants Propagate**: When you grant privileges at the database level (like `GRANT SELECT ON DATABASE`), they automatically apply to all current tables/views and any future tables/views created in that database. This is more maintainable than granting on individual tables.

**Principle of Least Privilege**: The scenario demonstrates a common production pattern - granting read-only access (USAGE + SELECT) to data consumers who need to query data but shouldn't modify it. This protects data integrity while enabling analytics.

**Common Privilege Combinations**:
- **Read-only analyst**: USAGE + SELECT (like this question)
- **Data engineer**: USAGE + SELECT + MODIFY + CREATE
- **Table owner**: ALL PRIVILEGES (includes SELECT, MODIFY, CREATE, and ability to grant to others)
- **Admin**: Workspace admin role bypasses all table ACLs

**WITH GRANT OPTION**: To allow users to delegate their privileges to others, you must explicitly add WITH GRANT OPTION to the GRANT statement. Without it, users cannot grant permissions even if they have them.

**Production Best Practice**: Use groups (like "eng") rather than granting to individual users. This makes permission management scalable - add/remove users from groups rather than managing individual grants across hundreds of tables.

---

### Question 85

A distributed team of data analysts share computing resources on an interactive cluster with autoscaling configured. In order to better manage costs and query throughput, the workspace administrator is hoping to evaluate whether cluster upscaling is caused by many concurrent users or resource-intensive queries.In which location can one review the timeline for cluster resizing events?

- A.Workspace audit logs
- B.Driver's log file
- C.Ganglia
- D.Cluster Event Log
- E.Executor's log file

The correct answer is **D. Cluster Event Log.**

**Explanation**

The Cluster Event Log is the dedicated location in Databricks for tracking cluster lifecycle events, including:

- **Cluster resize events**: When autoscaling adds or removes nodes
- **Upscaling events**: Adding nodes due to increased workload
- **Downscaling events**: Removing nodes when resources are underutilized
- **Timestamps**: Exact times when resize events occurred
- **Reason codes**: Why the resize was triggered
- **Node counts**: Before and after node counts for each event
- **Other lifecycle events**: Starting, stopping, restarting, termination

To access the Cluster Event Log:
1. Navigate to the Compute page in the Databricks workspace
2. Click on the specific cluster
3. Select the "Event Log" tab

The Event Log provides a chronological timeline view that allows the administrator to correlate resize events with user activity or query patterns, making it perfect for analyzing whether upscaling is caused by concurrent users or resource-intensive queries.

**Why the other options are incorrect:**

**A. Workspace audit logs**
- Audit logs track user actions, workspace-level events, and security-related activities (who accessed what, permission changes, API calls, etc.).
- They do NOT track infrastructure or cluster resource management events like autoscaling.
- Audit logs are for compliance and security monitoring, not cluster performance analysis.
- You would use audit logs to see who created/deleted clusters, but not when those clusters resized.

**B. Driver's log file**
- The driver log contains application-level logging from the Spark driver process.
- It includes query execution details, Spark job information, and application errors.
- It does NOT contain cluster infrastructure events like node addition/removal.
- Driver logs are at the application layer; cluster resize events are at the infrastructure layer.
- Too low-level and doesn't capture cluster management events.

**C. Ganglia**
- Ganglia is a metrics monitoring system that shows real-time and historical resource utilization (CPU, memory, disk, network).
- It provides graphs and dashboards of cluster performance metrics.
- While you can observe when resource usage increased (which might correlate with upscaling), Ganglia doesn't explicitly log resize events with timestamps and reasons.
- It shows the "effect" (resource usage) but not the discrete "events" (cluster resized at time X).
- Ganglia is for performance monitoring, not event tracking.

**E. Executor's log file**
- Executor logs contain logging from individual Spark executor processes running on worker nodes.
- They capture task-level execution details, data processing logs, and executor-specific errors.
- Like driver logs, these are application-level logs and don't capture cluster infrastructure events.
- Even more granular than driver logs, focusing on individual task execution.
- Not the right place to look for cluster management events.

**Key Takeaways (Interview-Ready)**

**Cluster Event Log Purpose**: The Cluster Event Log is specifically designed for tracking cluster lifecycle and autoscaling events. It's the authoritative source for understanding when and why cluster configurations changed over time.

**Autoscaling Analysis Workflow**: To determine whether upscaling is caused by concurrent users vs resource-intensive queries:
1. Check Cluster Event Log for upscaling timestamps
2. Cross-reference with query history (SQL query history or Spark UI) to see what queries were running
3. Check Ganglia for resource utilization patterns at those times
4. Review user activity to identify concurrent sessions

**Different Logs for Different Purposes**:
- **Cluster Event Log**: Infrastructure events (resize, start, stop)
- **Audit logs**: Security and compliance (who did what)
- **Driver/Executor logs**: Application-level debugging (query execution details)
- **Ganglia**: Resource utilization metrics (CPU, memory trends)

**Autoscaling Triggers**: Databricks autoscaling can be triggered by:
- Pending Spark tasks waiting for resources
- High CPU/memory utilization across the cluster
- Large number of concurrent queries
- Single resource-intensive query requiring more executors

**Cost Management Best Practice**: Regular review of the Cluster Event Log helps identify patterns like:
- Frequent upscaling during specific hours (suggests sizing the base cluster larger)
- Rapid up/down cycling (suggests autoscaling configuration needs tuning)
- Prolonged periods at maximum size (suggests workload needs a larger cluster or optimization)

**Troubleshooting Cluster Performance**: When investigating cluster performance issues, follow this hierarchy:
1. **Cluster Event Log** → Did the cluster have enough resources?
2. **Ganglia** → What were the actual resource utilization patterns?
3. **Spark UI** → Which jobs/queries were consuming resources?
4. **Driver/Executor logs** → Why were specific queries slow or failing?

---

### Question 86

When evaluating the Ganglia Metrics for a given cluster with 3 executor nodes, which indicator would signal proper utilization of the VM's resources?

- A.The five Minute Load Average remains consistent/flat
- B.Bytes Received never exceeds 80 million bytes per second
- C.Network I/O never spikes
- D.Total Disk Space remains constant
- E.CPU Utilization is around 75%

The correct answer is **E. CPU Utilization is around 75%.**

**Explanation**

Proper resource utilization means the cluster is being efficiently used without being over-provisioned (wasted resources) or under-provisioned (performance bottlenecks). 

**CPU Utilization around 75%** indicates:
- **Good efficiency**: The VMs are actively processing work, not sitting idle
- **Headroom for spikes**: There's still ~25% capacity for handling bursts or temporary load increases
- **Not overloaded**: Not at 100% where tasks would queue and performance would degrade
- **Cost-effective**: Resources are being used productively without waste
- **Healthy balance**: Industry best practice suggests 70-80% utilization as optimal for production systems

This level of utilization suggests the cluster is appropriately sized for the workload - busy enough to justify the cost, but not so overloaded that it causes performance issues.

**Why the other options are incorrect:**

**A. The five Minute Load Average remains consistent/flat**
- A flat load average could indicate either consistent utilization OR no utilization at all (idle cluster).
- Flat doesn't mean "proper" - a consistently flat load average of 0.1 would indicate severe underutilization.
- Real workloads naturally have some variation; perfectly flat metrics are unusual.
- Load average being flat doesn't tell you whether resources are properly utilized - you need to know the actual value.
- Load average measures queued processes; it's more about contention than utilization.

**B. Bytes Received never exceeds 80 million bytes per second**
- This is an arbitrary threshold with no relationship to proper utilization.
- Network throughput depends on workload characteristics - some legitimate workloads transfer very little data, others transfer terabytes.
- Staying under a specific byte limit doesn't indicate whether compute resources (CPU, memory) are properly utilized.
- This would be relevant for network capacity planning but not overall VM resource utilization.
- The "never exceeds" language suggests trying to avoid hitting limits, not measuring utilization quality.

**C. Network I/O never spikes**
- Spikes in network I/O are normal and expected in distributed data processing.
- During shuffle operations, joins, or data loading, network traffic naturally spikes.
- "Never spikes" would actually suggest underutilization or overly conservative workloads.
- Healthy Spark clusters show variable network patterns based on query phases.
- Avoiding spikes isn't a goal - handling spikes efficiently is what matters.

**D. Total Disk Space remains constant**
- Total disk space is a static capacity metric, not a utilization metric.
- If total disk space remains constant, it just means you haven't added or removed storage.
- This says nothing about how much of that disk space is being used or how efficiently.
- Disk space utilization (percentage used) would be relevant, but "total disk space" is just the provisioned capacity.
- This is a configuration metric, not a performance indicator.

**Key Takeaways (Interview-Ready)**

**Optimal CPU Utilization Range**: For production clusters, target 70-80% CPU utilization on average. This balances efficiency (resources are being used) with resilience (capacity for unexpected spikes). Below 50% suggests over-provisioning; sustained 95%+ indicates under-provisioning.

**Resource Utilization vs Capacity**: There's a critical difference between staying under capacity limits and achieving proper utilization. Proper utilization means resources are actively working efficiently, not just staying below thresholds.

**Ganglia Metrics Interpretation**: When evaluating cluster health in Ganglia, look for:
- CPU utilization in the 70-80% range (like this question)
- Memory usage patterns that show data is being cached but not causing OOM
- Network I/O that varies with workload phases (spikes during shuffles are normal)
- Disk I/O proportional to the data being processed

**Autoscaling Relationship**: Understanding proper utilization helps configure autoscaling appropriately. If CPU consistently exceeds 80%, autoscaling should add nodes. If CPU consistently stays below 40-50%, the base cluster is oversized or autoscaling is too aggressive.

**Variable Metrics are Normal**: Distributed data processing workloads naturally show variable patterns - spikes in network during shuffles, CPU peaks during compute-heavy tasks, memory fluctuations during caching. Perfectly flat metrics often indicate problems (idle cluster, blocked queries) rather than health.

**Cost Optimization Strategy**: Monitoring CPU utilization is key to cost optimization:
- Consistently low utilization (< 40%) → Downsize cluster or reduce idle time
- Consistently high utilization (> 90%) → Upsize cluster or optimize queries
- Target range (70-80%) → Cluster is appropriately sized

**Multi-Dimensional Analysis**: While CPU utilization around 75% is a good single indicator, comprehensive cluster evaluation requires checking multiple dimensions: CPU, memory, disk I/O, network I/O, and task scheduling metrics. CPU alone doesn't tell the full story, but it's often the best single indicator of overall utilization efficiency.

---

### Question 87

Which of the following technologies can be used to identify key areas of text when parsing Spark Driver log4j output?

- A.Regex
- B.Julia
- C.pyspsark.ml.feature
- D.Scala Datasets
- E.C++

The correct answer is **A. Regex**.

**Explanation**

* **Regular expressions (Regex)** are commonly used to **parse, search, and extract patterns from log files**, including Spark Driver **log4j output**.
* They are ideal for identifying:

  * Error messages
  * Stack traces
  * Timestamps
  * Log levels (INFO, WARN, ERROR)
  * Specific keywords or patterns

Regex works regardless of the language generating the logs and is the standard tool for log analysis.

---

**Why Other Options Are Incorrect**

* **B. Julia** ❌ A programming language, not a log-parsing technique.
* **C. `pyspark.ml.feature`** ❌ Used for feature engineering in ML, not log parsing.
* **D. Scala Datasets** ❌ A data abstraction, not a text-pattern matching tool.
* **E. C++** ❌ A programming language; not a specific technology for identifying text patterns.

---

**Key Takeaway (Interview-Ready)**

* **Log analysis = Regex**
* When parsing Spark or log4j logs, regular expressions are the **primary and most effective tool** for extracting meaningful information.

---

### Question 88

You are testing a collection of mathematical functions, one of which calculates the area under a curve as described by another function.assert(myIntegrate(lambda x: x*x, 0, 3) [0] == 9)Which kind of test would the above line exemplify?

- A.Unit
- B.Manual
- C.Functional
- D.Integration
- E.End-to-end

The correct answer is **A. Unit**.

**Explanation**

* The assertion tests a **single, specific function**: `myIntegrate`.
* It provides:

  * A **controlled input** (a simple lambda function `x*x` and bounds `0` to `3`)
  * A **known expected output** (`9`)
* There are:

  * No external systems
  * No dependencies
  * No full workflow being validated

This is the definition of a **unit test**—testing one unit of code in isolation.

---

**Why Other Options Are Incorrect**

* **B. Manual** ❌ This is an automated assertion, not a manual test.
* **C. Functional** ❌ Functional tests validate application behavior from a user or business perspective.
* **D. Integration** ❌ Integration tests validate interactions between multiple components.
* **E. End-to-end** ❌ End-to-end tests validate full system workflows.

---

**Key Takeaway (Interview-Ready)**

* **Unit tests** validate **individual functions or methods** in isolation.
* Assertions with deterministic inputs and outputs are classic unit test examples.


---

### Question 89

A Databricks job has been configured with 3 tasks, each of which is a Databricks notebook. Task A does not depend on other tasks. Tasks B and C run in parallel, with each having a serial dependency on Task A.If task A fails during a scheduled run, which statement describes the results of this run?

- A.Because all tasks are managed as a dependency graph, no changes will be committed to the Lakehouse until all tasks have successfully been completed.
- B.Tasks B and C will attempt to run as configured; any changes made in task A will be rolled back due to task failure.
- C.Unless all tasks complete successfully, no changes will be committed to the Lakehouse; because task A failed, all commits will be rolled back automatically.
- D.Tasks B and C will be skipped; some logic expressed in task A may have been committed before task failure.
- E.Tasks B and C will be skipped; task A will not commit any changes because of stage failure.

The correct answer is **D**.

**D. Tasks B and C will be skipped; some logic expressed in task A may have been committed before task failure.**

**Explanation**

* In Databricks multi-task jobs, **task dependencies control execution order**, not transactional behavior.
* Since **Tasks B and C depend on Task A**, they **will not run** if Task A fails.
* Databricks jobs **do not provide automatic rollback across tasks**.
* If Task A performed writes **before the failure occurred**, those changes **may already be committed** to the Lakehouse.

---

**Why the other options are incorrect**

* **A / C** ❌ Databricks jobs are not globally transactional; there is no automatic “all-or-nothing” commit across tasks.
* **B** ❌ Tasks B and C will **not attempt to run** because their dependency (Task A) failed.
* **E** ❌ A task failure does not guarantee that no writes were committed; partial progress may persist.

---

**Key Takeaway (Interview-Ready)**

* Databricks job tasks are **orchestrated, not transactional**.
* **Failed upstream tasks prevent downstream tasks from running**, but **any writes already executed are not automatically rolled back**.
* If atomicity is required, it must be implemented explicitly in the pipeline logic.


---

### Question 90

Which statement regarding Spark configuration on the Databricks platform is true?

- A.The Databricks REST API can be used to modify the Spark configuration properties for an interactive cluster without interrupting jobs currently running on the cluster.
- B.Spark configurations set within a notebook will affect all SparkSessions attached to the same interactive cluster.
- C.Spark configuration properties can only be set for an interactive cluster by creating a global init script.
- D.Spark configuration properties set for an interactive cluster with the Clusters UI will impact all notebooks attached to that cluster.
- E.When the same Spark configuration property is set for an interactive cluster and a notebook attached to that cluster, the notebook setting will always be ignored.

The correct answer is **D**.

**D. Spark configuration properties set for an interactive cluster with the Clusters UI will impact all notebooks attached to that cluster.**

**Explanation**

* Spark configuration properties defined at the **cluster level** (via the **Clusters UI**) are applied when the cluster starts.
* These settings become the **default Spark configuration** for the cluster.
* As a result, **all notebooks and jobs attached to that interactive cluster inherit these configurations**.
* This is the standard way to enforce consistent Spark behavior (memory settings, shuffle configs, AQE flags, etc.) across multiple users and notebooks.

---

**Why the other options are incorrect**

**A. REST API changes without interruption**
❌ Incorrect. Most Spark config changes require a **cluster restart**, which interrupts running workloads.

**B. Notebook configs affect all SparkSessions**
❌ Incorrect. Spark configs set within a notebook typically affect **only that SparkSession**, not all sessions on the cluster.

**C. Only global init scripts can set configs**
❌ Incorrect. Spark configs can be set via:

* Clusters UI
* Init scripts
* Notebook-level `spark.conf.set()`

**E. Notebook settings are always ignored**
❌ Incorrect. **Notebook-level configs can override cluster defaults** (within allowed limits).

---

**Key Takeaway (Interview-Ready)**

* **Cluster-level Spark configs apply to all notebooks attached to that cluster**.
* **Notebook-level configs are scoped locally** and may override cluster defaults.
* Many Spark config changes require a **cluster restart** to take effect.


---

### Question 91

A developer has successfully configured their credentials for Databricks Repos and cloned a remote Git repository. They do not have privileges to make changes to the main branch, which is the only branch currently visible in their workspace.Which approach allows this user to share their code updates without the risk of overwriting the work of their teammates?

- A.Use Repos to checkout all changes and send the git diff log to the team.
- B.Use Repos to create a fork of the remote repository, commit all changes, and make a pull request on the source repository.
- C.Use Repos to pull changes from the remote Git repository; commit and push changes to a branch that appeared as changes were pulled.
- D.Use Repos to merge all differences and make a pull request back to the remote repository.
- E.Use Repos to create a new branch, commit all changes, and push changes to the remote Git repository.

The correct answer is **E**.

**E. Use Repos to create a new branch, commit all changes, and push changes to the remote Git repository.**

**Explanation**

* The developer:

  * Has access to the repository via **Databricks Repos**
  * **Does not have permission to push to `main`**
* The safest and standard Git workflow in this situation is:

  1. **Create a new branch** (feature branch)
  2. Commit changes to that branch
  3. Push the branch to the remote repository
  4. Open a **pull request (PR)** for review and merge by someone with the right permissions

This approach:

* Prevents overwriting teammates’ work
* Preserves full Git history
* Aligns with enterprise Git governance and code review practices
* Is fully supported by Databricks Repos

---

**Why the other options are incorrect**

**A. Send git diff logs**
❌ Not collaborative, not reviewable, and bypasses version control workflows.

**B. Create a fork**
❌ Forking is not required or typical when the user already has push access to the repo (just not to `main`).

**C. Pull changes and push to a branch that appears**
❌ Pulling does not automatically create a writable branch.

**D. Merge all differences and make a pull request**
❌ Merging into `main` locally is not allowed without permissions and risks conflicts.

---

**Key Takeaway (Interview-Ready)**

* In Databricks Repos, the safest way to contribute without `main` access is:

  > **Create a feature branch → commit → push → open a pull request**
* This avoids accidental overwrites and supports proper code review and collaboration.


---

### Question 92

In order to prevent accidental commits to production data, a senior data engineer has instituted a policy that all development work will reference clones of Delta Lake tables. After testing both DEEP and SHALLOW CLONE, development tables are created using SHALLOW CLONE.A few weeks after initial table creation, the cloned versions of several tables implemented as Type 1 Slowly Changing Dimension (SCD) stop working. The transaction logs for the source tables show that VACUUM was run the day before.Which statement describes why the cloned tables are no longer working?

- A.Because Type 1 changes overwrite existing records, Delta Lake cannot guarantee data consistency for cloned tables.
- B.Running VACUUM automatically invalidates any shallow clones of a table; DEEP CLONE should always be used when a cloned table will be repeatedly queried.
- C.Tables created with SHALLOW CLONE are automatically deleted after their default retention threshold of 7 days.
- D.The metadata created by the CLONE operation is referencing data files that were purged as invalid by the VACUUM command.
- E.The data files compacted by VACUUM are not tracked by the cloned metadata; running REFRESH on the cloned table will pull in recent changes.

The correct answer is **D**.

**D. The metadata created by the CLONE operation is referencing data files that were purged as invalid by the VACUUM command.**

**Explanation**

* **SHALLOW CLONE** creates a new table that:

  * Copies **only metadata**
  * Continues to reference the **same underlying data files** as the source table
* It does **not** copy data files into a new location.
* When **VACUUM** is run on the source table:

  * Old, no-longer-referenced data files are **physically deleted**
* Because the shallow clone still points to those original files:

  * Once VACUUM removes them, the clone’s metadata now references **nonexistent files**
  * Queries against the clone fail

This is expected behavior and a known limitation of **SHALLOW CLONE** when VACUUM is used on the source table.

---

**Why the other options are incorrect**

**A. Type 1 overwrites break clone consistency**
❌ Incorrect. Type 1 SCDs work fine with Delta; the issue is file deletion, not overwrite semantics.

**B. VACUUM always invalidates shallow clones**
❌ Incorrect as stated. VACUUM only breaks shallow clones **if it removes files the clone still references**. It’s not automatic in all cases.

**C. Shallow clones expire after 7 days**
❌ Incorrect. There is no automatic expiration for shallow clones.

**E. REFRESH will fix the clone**
❌ Incorrect. REFRESH updates metadata visibility, not missing data files.

---

**Key Takeaway (Interview-Ready)**

* **SHALLOW CLONE = shared data files**
* **VACUUM deletes old files**
* If a shallow clone still references those files, it will break
* Use **DEEP CLONE** when:

  * Tables must remain stable over time
  * VACUUM may be run on source tables
  * Development environments need isolation from production cleanup jobs


---

### Question 93

You are performing a join operation to combine values from a static userLookup table with a streaming DataFrame streamingDF.Which code block attempts to perform an invalid stream-static join?

- A.userLookup.join(streamingDF, ["userid"], how="inner")
- B.streamingDF.join(userLookup, ["user_id"], how="outer")
- C.streamingDF.join(userLookup, ["user_id”], how="left")
- D.streamingDF.join(userLookup, ["userid"], how="inner")
- E.userLookup.join(streamingDF, ["user_id"], how="right")

The correct answer is **B**.

**B. `streamingDF.join(userLookup, ["user_id"], how="outer")`**

---

**Explanation**

In **Structured Streaming**, joins between a **streaming DataFrame** and a **static DataFrame** have strict limitations:

**✅ Supported stream–static joins**

* **Inner join**
* **Left outer join** *(when the streaming DataFrame is on the left)*

**❌ Unsupported stream–static joins**

* **Full outer joins**
* **Right outer joins where the streaming DataFrame is on the right**
* Any join that would require Spark to maintain **unbounded state on both sides**

---

**Why option **B** is invalid**

* `how="outer"` is a **full outer join**
* Full outer joins are **not supported** when one side is streaming
* Spark cannot safely maintain state for unmatched rows from **both sides** of an unbounded stream

This will fail with an analysis error.

---

**Why the other options are valid**

* **A.** `inner` join → ✅ supported
* **C.** `left` join with streaming on the left → ✅ supported
* **D.** `inner` join → ✅ supported
* **E.** `right` join where streaming is on the right → treated as a left join with streaming preserved → ✅ supported

---

**Interview-ready takeaway**

> **Structured Streaming supports only INNER and LEFT OUTER joins with static tables (when streaming is on the left).
> FULL OUTER joins are not supported with streaming sources.**

If you want, I can also share:

* A **join support matrix** (stream–stream vs stream–static)
* Common **error messages** you’ll see in Spark UI for invalid joins


---

### Question 94

Spill occurs as a result of executing various wide transformations. However, diagnosing spill requires one to proactively look for key indicators.Where in the Spark UI are two of the primary indicators that a partition is spilling to disk?

- A.Query’s detail screen and Job’s detail screen
- B.Stage’s detail screen and Executor’s log files
- C.Driver’s and Executor’s log files
- D.Executor’s detail screen and Executor’s log files
- E.Stage’s detail screen and Query’s detail screen

The correct answer is **D**.

**D. Executor’s detail screen and Executor’s log files**

---

**Explanation**

**Spill to disk** happens during **wide transformations** (e.g., `join`, `groupBy`, `orderBy`) when in-memory data exceeds available executor memory. The most reliable indicators of spill are found at the **executor level**, because spilling is a **task/executor-local behavior**.

**Where to look in the Spark UI:**

1.  **Executor’s Detail Screen**
    
    -   Metrics such as:
        
        -   **Shuffle Spill (Memory)**
            
        -   **Shuffle Spill (Disk)**
            
    -   These counters directly indicate how much data was spilled due to memory pressure.
        
2.  **Executor Log Files**
    
    -   Logs contain messages like:
        
        -   `Spilling sort data to disk`
            
        -   `Spilling in-memory map to disk`
            
    -   These messages confirm **when and why** spilling occurred.
        

Together, these two locations provide both:

-   **Quantitative evidence** (metrics)
    
-   **Qualitative confirmation** (log messages)
    

---

**Why the other options are incorrect**

-   **A. Query + Job detail screens** ❌  
    These provide high-level execution info, not executor-level spill diagnostics.
    
-   **B. Stage detail + Executor logs** ❌  
    Stage metrics summarize performance but do not clearly isolate spill behavior per executor.
    
-   **C. Driver and Executor logs** ❌  
    Driver logs rarely report spill; spilling occurs on executors.
    
-   **E. Stage detail + Query detail** ❌  
    These screens do not expose spill metrics directly.
    

---

**Interview-ready takeaway**

> **Spill is an executor-level problem.**  
> To diagnose it, always check:
> 
> -   **Executor metrics (spill memory/disk)**
>     
> -   **Executor logs for spill messages**
>     

This is the fastest and most reliable way to confirm disk spill in Spark workloads.

---

### Question 95

![Question 95 Image 1](examtopics_images/question_95_img_1.png)

A task orchestrator has been configured to run two hourly tasks. First, an outside system writes Parquet data to a directory mounted at /mnt/raw_orders/. After this data is written, a Databricks job containing the following code is executed:Assume that the fields customer_id and order_id serve as a composite key to uniquely identify each order, and that the time field indicates when the record was queued in the source system.If the upstream system is known to occasionally enqueue duplicate entries for a single order hours apart, which statement is correct?

- A.Duplicate records enqueued more than 2 hours apart may be retained and the orders table may contain duplicate records with the same customer_id and order_id.
- B.All records will be held in the state store for 2 hours before being deduplicated and committed to the orders table.
- C.The orders table will contain only the most recent 2 hours of records and no duplicates will be present.
- D.Duplicate records arriving more than 2 hours apart will be dropped, but duplicates that arrive in the same batch may both be written to the orders table.
- E.The orders table will not contain duplicates, but records arriving more than 2 hours late will be ignored and missing from the table.

The correct answer is **A**.

**Explanation**
The streaming query uses `withWatermark("time", "2 hours")` together with `dropDuplicates(["customer_id", "order_id"])`.
This means Spark will only keep state for deduplication for **2 hours of event time**. If a duplicate record for the same `(customer_id, order_id)` arrives **more than 2 hours apart**, the earlier state may have been evicted, and Spark will treat the later record as new. As a result, duplicates that are spaced more than 2 hours apart can still be written to the `orders` table.

**Why the other options are incorrect**

* **B**: Records are not held and delayed for 2 hours before writing; watermark controls state retention, not output delay.
* **C**: Watermarking does not limit the table to only the most recent 2 hours of data.
* **D**: Duplicates in the same batch are handled by `dropDuplicates`; this option reverses the actual behavior.
* **E**: Records arriving more than 2 hours late are not automatically ignored unless they violate watermark constraints in aggregations; this statement overgeneralizes.

**Key takeaway (interview-ready)**
Watermark-based deduplication in Structured Streaming is **time-bounded**. Duplicates arriving **outside the watermark window can reappear**, even when `dropDuplicates` is used.


---

### Question 96

A junior data engineer is migrating a workload from a relational database system to the Databricks Lakehouse. The source system uses a star schema, leveraging foreign key constraints and multi-table inserts to validate records on write.Which consideration will impact the decisions made by the engineer while migrating this workload?

- A.Databricks only allows foreign key constraints on hashed identifiers, which avoid collisions in highly-parallel writes.
- B.Databricks supports Spark SQL and JDBC; all logic can be directly migrated from the source system without refactoring.
- C.Committing to multiple tables simultaneously requires taking out multiple table locks and can lead to a state of deadlock.
- D.All Delta Lake transactions are ACID compliant against a single table, and Databricks does not enforce foreign key constraints.
- E.Foreign keys must reference a primary key field; multi-table inserts must leverage Delta Lake’s upsert functionality.

The correct answer is **D**.

**D. All Delta Lake transactions are ACID compliant against a single table, and Databricks does not enforce foreign key constraints.**

**Explanation**

* Delta Lake provides **ACID guarantees at the individual table level**, not across multiple tables.
* Unlike traditional relational databases, **foreign key constraints are not enforced** in Delta Lake.
* As a result, workloads that rely on:

  * Foreign key validation on write
  * Multi-table atomic inserts
    must be **refactored** when migrating to the Lakehouse.
* Referential integrity must be handled through:

  * Application logic
  * ETL validation steps
  * Post-write checks or constraints implemented outside the database engine

This architectural difference directly impacts how the engineer designs ingestion and validation logic during migration.

**Key consideration (interview-ready)**
When moving from an RDBMS to Databricks, assume **no enforced foreign keys and no multi-table transactions**—design pipelines to validate and maintain integrity explicitly in code.


---

### Question 97

A data architect has heard about Delta Lake’s built-in versioning and time travel capabilities. For auditing purposes, they have a requirement to maintain a full record of all valid street addresses as they appear in the customers table.The architect is interested in implementing a Type 1 table, overwriting existing records with new values and relying on Delta Lake time travel to support long-term auditing. A data engineer on the project feels that a Type 2 table will provide better performance and scalability.Which piece of information is critical to this decision?

- A.Data corruption can occur if a query fails in a partially completed state because Type 2 tables require setting multiple fields in a single update.
- B.Shallow clones can be combined with Type 1 tables to accelerate historic queries for long-term versioning.
- C.Delta Lake time travel cannot be used to query previous versions of these tables because Type 1 changes modify data files in place.
- D.Delta Lake time travel does not scale well in cost or latency to provide a long-term versioning solution.
- E.Delta Lake only supports Type 0 tables; once records are inserted to a Delta Lake table, they cannot be modified.

The correct answer is **D**.

**D. Delta Lake time travel does not scale well in cost or latency to provide a long-term versioning solution.**

**Explanation**

* Delta Lake **time travel** is implemented by retaining old data files and transaction log entries.
* Over time, especially for frequently updated Type 1 tables:

  * The number of historical files grows
  * Storage costs increase
  * Queries against older versions become **slower and more expensive**
* Time travel is best suited for:

  * Short- to medium-term debugging
  * Accidental delete/update recovery
  * Operational rollback scenarios
* It is **not designed as a long-term auditing or historical tracking mechanism**.

For a requirement like **maintaining a full record of all valid street addresses over time**, a **Type 2 SCD**:

* Explicitly models history
* Scales better for long-term auditing
* Provides predictable query performance
* Avoids dependence on long retention periods for old Delta files

This scalability and cost consideration is the **critical factor** in deciding between Type 1 + time travel vs. Type 2.

**Key takeaway (interview-ready)**
Delta Lake time travel is powerful, but it is **not a replacement for Type 2 SCDs** when long-term historical auditing, predictable performance, and cost control are required.


---

### Question 98

![Question 98 Image 1](examtopics_images/question_98_img_1.png)

A table named user_ltv is being used to create a view that will be used by data analysts on various teams. Users in the workspace are configured into groups, which are used for setting up data access using ACLs.The user_ltv table has the following schema:email STRING, age INT, ltv INTThe following view definition is executed:An analyst who is not a member of the auditing group executes the following query:SELECT * FROM user_ltv_no_minorsWhich statement describes the results returned by this query?

- A.All columns will be displayed normally for those records that have an age greater than 17; records not meeting this condition will be omitted.
- B.All age values less than 18 will be returned as null values, all other columns will be returned with the values in user_ltv.
- C.All values for the age column will be returned as null values, all other columns will be returned with the values in user_ltv.
- D.All records from all columns will be displayed with the values in user_ltv.
- E.All columns will be displayed normally for those records that have an age greater than 18; records not meeting this condition will be omitted.

**Correct Answer: A**

---

**Explanation for correct option**

The view definition applies a **row-level filter** using a `CASE` expression in the `WHERE` clause:

* If the user **is a member of the auditing group**, the condition evaluates to `TRUE`, so **all rows are returned**.
* If the user **is not a member of the auditing group**, the condition becomes `age >= 18`.

Because the analyst is **not** a member of the auditing group, only rows where `age >= 18` are included in the result set.
All selected columns (`email`, `age`, `ltv`) are returned **unchanged** for those qualifying rows.

So:

* Records with `age >= 18` → fully visible
* Records with `age < 18` → completely omitted

This exactly matches **Option A**.

---

**Why the other options are incorrect**

* **B**: Incorrect because rows with `age < 18` are **filtered out**, not returned with `NULL` values. The logic is in the `WHERE` clause, not the `SELECT`.
* **C**: Incorrect because the `age` column is never masked or nulled; rows are either included or excluded.
* **D**: Incorrect because non-auditing users do **not** see all rows—minors are filtered out.
* **E**: Incorrect because the condition is `age >= 18`, not `age > 18`. Records with `age = 18` are included.

---

**Key takeaway (interview-ready)**

A `CASE` expression in a **WHERE clause** controls **row-level filtering**, not column masking.
When used with `is_member()`, it enables **group-based access control**, where unauthorized users see **fewer rows**, not modified values.


---

### Question 99

![Question 99 Image 1](examtopics_images/question_99_img_1.png)

The data governance team is reviewing code used for deleting records for compliance with GDPR. The following logic has been implemented to propagate delete requests from the user_lookup table to the user_aggregates table.Assuming that user_id is a unique identifying key and that all users that have requested deletion have been removed from the user_lookup table, which statement describes whether successfully executing the above logic guarantees that the records to be deleted from the user_aggregates table are no longer accessible and why?

- A.No; the Delta Lake DELETE command only provides ACID guarantees when combined with the MERGE INTO command.
- B.No; files containing deleted records may still be accessible with time travel until a VACUUM command is used to remove invalidated data files.
- C.Yes; the change data feed uses foreign keys to ensure delete consistency throughout the Lakehouse.
- D.Yes; Delta Lake ACID guarantees provide assurance that the DELETE command succeeded fully and permanently purged these records.
- E.No; the change data feed only tracks inserts and updates, not deleted records.

**Correct Answer: B**

**Explanation for correct option**
Delta Lake’s `DELETE` operation is **logically applied** by marking data files as removed in the transaction log, but the **physical Parquet files are not immediately deleted**. Because Delta Lake supports **time travel**, older versions of the table (including deleted records) remain accessible until a `VACUUM` operation is executed and the retention period has elapsed.
Therefore, even though the delete logic executes successfully, **the deleted records may still be accessible via time travel**, which means GDPR-style “hard delete” is not guaranteed until `VACUUM` is run.

**Why the other options are incorrect**

* **A**: Incorrect — `DELETE` in Delta Lake is fully ACID-compliant on a single table and does not require `MERGE INTO` for correctness.
* **C**: Incorrect — Change Data Feed (CDF) does not enforce referential integrity or foreign keys; it only exposes row-level changes.
* **D**: Incorrect — ACID guarantees correctness and consistency, not permanent physical deletion. Data is not permanently purged immediately.
* **E**: Incorrect — Delta Lake Change Data Feed explicitly tracks **inserts, updates, and deletes**, including `_change_type = 'delete'`.

**Key takeaway (interview-ready)**
In Delta Lake, **DELETE removes data logically, not physically**—records can still be accessed via **time travel** until `VACUUM` permanently removes the underlying files. For GDPR compliance, `VACUUM` is required.


---

### Question 100

The data engineering team has been tasked with configuring connections to an external database that does not have a supported native connector with Databricks. The external database already has data security configured by group membership. These groups map directly to user groups already created in Databricks that represent various teams within the company.A new login credential has been created for each group in the external database. The Databricks Utilities Secrets module will be used to make these credentials available to Databricks users.Assuming that all the credentials are configured correctly on the external database and group membership is properly configured on Databricks, which statement describes how teams can be granted the minimum necessary access to using these credentials?

- A."Manage" permissions should be set on a secret key mapped to those credentials that will be used by a given team.
- B."Read" permissions should be set on a secret key mapped to those credentials that will be used by a given team.
- C."Read" permissions should be set on a secret scope containing only those credentials that will be used by a given team.
- D."Manage" permissions should be set on a secret scope containing only those credentials that will be used by a given team.No additional configuration is necessary as long as all users are configured as administrators in the workspace where secrets have been added.

**Correct Answer: C**

**Explanation (concise):**
In Databricks Secrets, **access control is applied at the secret scope level**, not at individual secret keys. To grant **minimum necessary access**, each team should be given **Read permission on a secret scope** that contains **only the credentials intended for that team**. This allows users to use the secrets in code without being able to list, modify, or manage them.

**Why others are incorrect (brief):**

* **A & B:** Permissions cannot be set on individual secret keys—only on secret scopes.
* **D:** *Manage* permission is overly permissive and violates least-privilege principles.
* **E:** Making all users admins is insecure and unnecessary.

**Key takeaway (interview-ready):**
Databricks secrets follow **scope-level ACLs**—use **Read access on dedicated secret scopes** per team to enforce least privilege.


---

### Question 101

Which indicators would you look for in the Spark UI’s Storage tab to signal that a cached table is not performing optimally? Assume you are using Spark’s MEMORY_ONLY storage level.

- A.Size on Disk is < Size in Memory
- B.The RDD Block Name includes the “*” annotation signaling a failure to cache
- C.Size on Disk is > 0
- D.The number of Cached Partitions > the number of Spark Partitions
- E.On Heap Memory Usage is within 75% of Off Heap Memory Usage

**Correct Answer: C**

**Explanation for correct option**
With **MEMORY_ONLY** storage level, cached data should reside **entirely in memory**. If the **Size on Disk is greater than 0**, it indicates that Spark was **unable to keep all cached partitions in memory**, so some partitions were spilled or recomputed instead of being served from memory. This is a strong signal that the cache is not performing optimally—usually due to insufficient executor memory or oversized partitions.

**Why the other options are incorrect**

* **A.** `Size on Disk < Size in Memory` is not meaningful for MEMORY_ONLY; disk usage should ideally be **zero**, not comparatively smaller.
* **B.** The `*` annotation indicates partial caching, but it does not by itself diagnose performance issues as clearly as disk usage for MEMORY_ONLY.
* **D.** Cached partitions exceeding Spark partitions does not indicate poor caching behavior.
* **E.** Heap vs off-heap usage ratios are not a direct indicator of cache inefficiency for MEMORY_ONLY storage.

**Key takeaway (interview-ready)**
For **MEMORY_ONLY caching**, **any non-zero disk usage in the Storage tab is a red flag**—it means the cache is not fully resident in memory and performance benefits are being lost.


---

### Question 102

What is the first line of a Databricks Python notebook when viewed in a text editor?

- A.%python
- B.// Databricks notebook source
- C.# Databricks notebook source
- D.-- Databricks notebook source
- E.# MAGIC %python

**Correct Answer: C. `# Databricks notebook source`**

**Explanation for correct option**
When a Databricks **Python notebook** is exported or viewed in a text editor (for example, from Databricks Repos or via the workspace filesystem), the **first line** is always:

```
# Databricks notebook source
```

This comment marks the beginning of the notebook and is used by Databricks to delimit notebook cells when stored as plain text.

**Why the other options are incorrect**

* **A. `%python`** – This is a cell magic used *inside* Databricks notebooks, not the file header.
* **B. `// Databricks notebook source`** – `//` is not a Python comment.
* **D. `-- Databricks notebook source`** – This is a SQL-style comment, not Python.
* **E. `# MAGIC %python`** – This appears inside notebooks to indicate magic commands, not as the first line.

**Key takeaway (interview-ready)**
Databricks notebooks stored as text always begin with a language-specific comment header. For **Python notebooks**, the first line is `# Databricks notebook source`.


---

### Question 103

Which statement describes a key benefit of an end-to-end test?

- A.Makes it easier to automate your test suite
- B.Pinpoints errors in the building blocks of your application
- C.Provides testing coverage for all code paths and branches
- D.Closely simulates real world usage of your application
- E.Ensures code is optimized for a real-life workflow

**Correct Answer: D. Closely simulates real world usage of your application**

**Explanation for correct option**
End-to-end (E2E) tests validate the **entire application flow from start to finish**, exercising multiple components together (ingestion, processing, storage, and consumption). Their key benefit is that they **simulate real-world usage scenarios**, helping ensure the system behaves correctly under conditions similar to production.

**Why the other options are incorrect**

* **A. Makes it easier to automate your test suite** – Automation depends on tooling and design, not specifically on E2E tests.
* **B. Pinpoints errors in the building blocks of your application** – This is the primary goal of **unit tests**.
* **C. Provides testing coverage for all code paths and branches** – That is related to **code coverage**, not E2E testing.
* **E. Ensures code is optimized for a real-life workflow** – Performance optimization is not guaranteed by E2E tests.

**Key takeaway (interview-ready)**
End-to-end tests are valuable because they **validate complete workflows under realistic conditions**, catching integration issues that unit and integration tests may miss.


---

### Question 104

The Databricks CLI is used to trigger a run of an existing job by passing the job_id parameter. The response that the job run request has been submitted successfully includes a field run_id.Which statement describes what the number alongside this field represents?

- A.The job_id and number of times the job has been run are concatenated and returned.
- B.The total number of jobs that have been run in the workspace.
- C.The number of times the job definition has been run in this workspace.
- D.The job_id is returned in this field.
- E.The globally unique ID of the newly triggered run.

**Correct Answer: E**

**E. The globally unique ID of the newly triggered run.**

**Explanation for correct option**

* When you trigger a Databricks job via the CLI (or REST API), Databricks creates a **new job run**.
* The response includes a **`run_id`**, which uniquely identifies **that specific execution instance**.
* This `run_id` is used to:

  * Query run status
  * Fetch logs and outputs
  * Cancel or rerun that exact execution

Each run gets its own unique `run_id`, even if it’s the same job definition.

**Why the other options are incorrect**

* **A**: `run_id` is not derived from `job_id` or a counter.
* **B**: It does not represent a workspace-wide count.
* **C**: It is not a per-job execution counter.
* **D**: `job_id` and `run_id` are distinct identifiers.

**Key takeaway (interview-ready)**

* **`job_id`** identifies the job definition.
* **`run_id`** uniquely identifies **one specific execution** of that job.


---

### Question 105

![Question 105 Image 1](examtopics_images/question_105_img_1.png)

The data science team has created and logged a production model using MLflow. The model accepts a list of column names and returns a new column of type DOUBLE.The following code correctly imports the production model, loads the customers table containing the customer_id key column into a DataFrame, and defines the feature columns needed for the model.Which code block will output a DataFrame with the schema "customer_id LONG, predictions DOUBLE"?

- A.df.map(lambda x:model(x[columns])).select("customer_id, predictions")
- B.df.select("customer_id", model(*columns).alias("predictions"))
- C.model.predict(df, columns)
- D.df.select("customer_id", pandas_udf(model, columns).alias("predictions"))
- E.df.apply(model, columns).select("customer_id, predictions")

**Correct Answer: B**

```python
df.select("customer_id", model(*columns).alias("predictions"))
```

**Explanation for correct option**

* `mlflow.pyfunc.spark_udf(...)` returns a **Spark UDF**.
* Spark UDFs are applied by passing **columns as arguments**, not DataFrames.
* `model(*columns)` applies the model to the feature columns and returns a column of type `DOUBLE`.
* Selecting `"customer_id"` along with the UDF output produces the schema:
  **`customer_id LONG, predictions DOUBLE`**, exactly as required.

**Why the other options are incorrect**

* **A**: `df.map` is not valid for Spark DataFrames (it’s an RDD operation).
* **C**: `model.predict` is not how Spark UDFs are invoked in distributed execution.
* **D**: `pandas_udf` is not used here; the model is already wrapped as a Spark UDF.
* **E**: `df.apply` is not a valid Spark DataFrame API.

**Key takeaway (interview-ready)**

* When using **MLflow `spark_udf`**, apply the model like any Spark UDF:
  **`df.select(..., model(*feature_columns))`**
* Do **not** use `.predict()`, `map`, or pandas UDFs in this pattern.


---

### Question 106

A nightly batch job is configured to ingest all data files from a cloud object storage container where records are stored in a nested directory structure YYYY/MM/DD. The data for each date represents all records that were processed by the source system on that date, noting that some records may be delayed as they await moderator approval. Each entry represents a user review of a product and has the following schema:user_id STRING, review_id BIGINT, product_id BIGINT, review_timestamp TIMESTAMP, review_text STRINGThe ingestion job is configured to append all data for the previous date to a target table reviews_raw with an identical schema to the source system. The next step in the pipeline is a batch write to propagate all new records inserted into reviews_raw to a table where data is fully deduplicated, validated, and enriched.Which solution minimizes the compute costs to propagate this batch of data?

- A.Perform a batch read on the reviews_raw table and perform an insert-only merge using the natural composite key user_id, review_id, product_id, review_timestamp.
- B.Configure a Structured Streaming read against the reviews_raw table using the trigger once execution mode to process new records as a batch job.
- C.Use Delta Lake version history to get the difference between the latest version of reviews_raw and one version prior, then write these records to the next table.
- D.Filter all records in the reviews_raw table based on the review_timestamp; batch append those records produced in the last 48 hours.
- E.Reprocess all records in reviews_raw and overwrite the next table in the pipeline.

**Correct Answer: C**

**C. Use Delta Lake version history to get the difference between the latest version of `reviews_raw` and one version prior, then write these records to the next table.**

---

**Explanation for correct option**

* The ingestion job **appends data once per day** to the `reviews_raw` Delta table.
* Delta Lake tracks **table versions transactionally**.
* By reading **only the new data added since the previous version** (using Delta version history / change detection), the pipeline:

  * Processes **only newly ingested records**
  * Avoids rescanning the entire table
  * Minimizes compute, I/O, and shuffle costs
* This is the most **cost-efficient batch pattern** when upstream writes are append-only and versioned.

---

**Why the other options are incorrect**

* **A**: Insert-only MERGE still requires scanning the source data and evaluating merge conditions, which is more expensive than reading only new files.
* **B**: Structured Streaming with `trigger once` works, but introduces **streaming overhead** and checkpointing that is unnecessary for a simple daily batch.
* **D**: Filtering by `review_timestamp` is unreliable because delayed records may arrive outside the chosen window, leading to either missed or reprocessed data.
* **E**: Reprocessing and overwriting all records is the **most expensive** option in terms of compute and I/O.

---

**Key takeaway (interview-ready)**

* For **append-only Delta tables**, the lowest-cost batch propagation pattern is to **process only newly committed data using Delta table versioning or change detection**, not full-table scans or streaming constructs.


---

### Question 107

Which statement describes Delta Lake optimized writes?

- A.Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.
- B.An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.
- C.Data is queued in a messaging bus instead of committing data directly to memory; all data is committed from the messaging bus in one batch once the job is complete.
- D.Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.
- E.A shuffle occurs prior to writing to try to group similar data together resulting in fewer files instead of each executor writing multiple files based on directory partitions.

**Correct Answer: E. A shuffle occurs prior to writing to try to group similar data together resulting in fewer files instead of each executor writing multiple files based on directory partitions.**

**Why it's correct**

Delta Lake optimized writes is a feature that automatically performs an adaptive shuffle operation before writing data to optimize file sizes. Specifically:
- Adds a shuffle/repartition step before the write operation
- Groups data intelligently to produce fewer, larger files (closer to the target file size)
- Prevents the "small files problem" where each executor writes many tiny files
- Works automatically without requiring manual OPTIMIZE commands
- Particularly effective for partitioned tables where each executor might otherwise write small files to multiple partition directories
- Enabled via `spark.databricks.delta.optimizeWrite.enabled = true`

This reduces the number of files created during writes, improving both write performance and subsequent read performance.

**Why others are wrong**

* **A. Before Jobs cluster terminates, OPTIMIZE is executed on all tables** – This describes auto-optimize or a custom cleanup job, not optimized writes. Optimized writes happen during the write operation itself, not as a post-processing step at cluster termination.

* **B. An asynchronous job runs after write completes to detect if files could be further compacted** – This describes auto-optimize (or auto-compaction), which is a separate feature that runs OPTIMIZE in the background after writes. Optimized writes prevent small files proactively during writes, not reactively afterward.

* **C. Data is queued in a messaging bus instead of committing directly to memory** – This is completely fabricated. Delta Lake doesn't use a messaging bus architecture for writes. Data is written directly to cloud storage through standard Spark operations.

* **D. Optimized writes use logical partitions instead of directory partitions** – False. Optimized writes don't change the partitioning strategy from physical directories to logical metadata. Delta Lake still uses directory-based partitioning; optimized writes just reduce the number of files per partition.

**Key takeaway**

Optimized writes (enabled via configuration) automatically adds a shuffle step before writing data to produce fewer, right-sized files instead of many small files. This is different from auto-optimize, which runs OPTIMIZE commands after writes complete. Optimized writes prevent the small files problem proactively during the write operation itself.

---

### Question 108

Which statement describes the default execution mode for Databricks Auto Loader?

- A.Cloud vendor-specific queue storage and notification services are configured to track newly arriving files; the target table is materialized by directly querying all valid files in the source directory.
- B.New files are identified by listing the input directory; the target table is materialized by directly querying all valid files in the source directory.
- C.Webhooks trigger a Databricks job to run anytime new data arrives in a source directory; new data are automatically merged into target tables using rules inferred from the data.
- D.New files are identified by listing the input directory; new files are incrementally and idempotently loaded into the target Delta Lake table.
- E.Cloud vendor-specific queue storage and notification services are configured to track newly arriving files; new files are incrementally and idempotently loaded into the target Delta Lake table.

**Correct Answer: D. New files are identified by listing the input directory; new files are incrementally and idempotently loaded into the target Delta Lake table.**

**Why it's correct**

Auto Loader's **default execution mode is directory listing**, where it scans the input directory to identify new files. Files are then incrementally processed and loaded into the target table with idempotent writes (preventing duplicates if the job reruns).

**Why others are wrong**

* **A. Cloud vendor queue/notification services with direct querying** – This describes file notification mode (not default) combined with incorrect behavior. Auto Loader doesn't materialize tables by "directly querying" source files; it processes them incrementally through Spark streaming.

* **B. Directory listing with direct querying of all valid files** – While directory listing is the default mode, Auto Loader doesn't query all files directly. It incrementally processes only new files and maintains checkpoints to track what's been processed.

* **C. Webhooks trigger Databricks job with automatic merge** – Auto Loader doesn't use webhooks. It runs as a Structured Streaming job that continuously monitors for new files. There's no webhook-triggered architecture.

* **E. Cloud vendor queue/notification services with incremental loading** – This describes **file notification mode**, which is the advanced (non-default) mode that uses cloud-native services like AWS SNS/SQS or Azure Event Grid for better scalability at higher file volumes.

**Key takeaway**

Auto Loader defaults to **directory listing mode** for simplicity and ease of setup. For production workloads with high file arrival rates (millions of files), switch to **file notification mode** by setting `.option("cloudFiles.useNotifications", "true")` for better performance and lower costs.

---

### Question 109

A Delta Lake table representing metadata about content posts from users has the following schema:user_id LONG, post_text STRING, post_id STRING, longitude FLOAT, latitude FLOAT, post_time TIMESTAMP, date DATEBased on the above schema, which column is a good candidate for partitioning the Delta Table?

- A.post_time
- B.latitude
- C.post_id
- D.user_id
- E.date

**Correct Answer: E. date**

**Why it's correct**

The `date` column is the best partitioning candidate because it has low cardinality (one value per day), creates naturally balanced partitions, and aligns with common query patterns (time-based filtering). Partitioning by date enables efficient partition pruning for queries like "get posts from last 7 days" without scanning the entire table.

**Why others are wrong**

* **A. post_time** – Timestamp has extremely high cardinality (unique per second/millisecond), creating massive numbers of tiny partitions. This causes poor performance and metadata overhead ("small files problem" at partition level).

* **B. latitude** – Float values have very high cardinality with unpredictable distribution. Would create thousands of unbalanced partitions with no meaningful query benefit since geographic queries rarely filter on exact latitude values.

* **C. post_id** – Unique identifier has maximum cardinality (one partition per post), defeating the entire purpose of partitioning. Would create millions of single-record partitions.

* **D. user_id** – While lower cardinality than post_id, user activity varies greatly (power users vs casual users), creating highly skewed partitions. Also, most queries filter by time, not specific users.

**Key takeaway**

Good partition columns have **low-to-medium cardinality** (hundreds to thousands of distinct values, not millions), create **balanced partitions**, and align with **common query filters**. Date/timestamp columns (at day/month level) are ideal. Avoid high-cardinality columns like IDs, timestamps, or continuous numeric values.

---

### Question 110

A large company seeks to implement a near real-time solution involving hundreds of pipelines with parallel updates of many tables with extremely high volume and high velocity data.Which of the following solutions would you implement to achieve this requirement?

- A.Use Databricks High Concurrency clusters, which leverage optimized cloud storage connections to maximize data throughput.
- B.Partition ingestion tables by a small time duration to allow for many data files to be written in parallel.
- C.Configure Databricks to save all data to attached SSD volumes instead of object storage, increasing file I/O significantly.
- D.Isolate Delta Lake tables in their own storage containers to avoid API limits imposed by cloud vendors.
- E.Store all tables in a single database to ensure that the Databricks Catalyst Metastore can load balance overall throughput.

**Correct answer**
**D**

**Explanation**
With hundreds of pipelines writing in parallel at very high volume and velocity, the primary bottleneck is often **cloud object storage API limits** (for example, request rate limits per container/bucket). Isolating Delta Lake tables into **separate storage containers** spreads load across multiple endpoints, reducing throttling and enabling higher sustained throughput for near real-time ingestion.

**Why others are wrong**

* **A**: High Concurrency clusters optimize compute sharing, not object storage API limits.
* **B**: Fine-grained partitioning can worsen small-file problems and does not solve storage API throttling.
* **C**: Databricks is designed for object storage; local SSDs are ephemeral and unsuitable for durable Lakehouse storage.
* **E**: Databases are logical constructs; they do not influence storage-level throughput or load balancing.

**Key takeaway (interview-ready)**
At extreme scale, near real-time Lakehouse pipelines are often limited by **object storage API throughput**, not Spark compute—**isolating tables across storage containers** is a proven pattern to scale write concurrency.


---

### Question 111

Which describes a method of installing a Python package scoped at the notebook level to all nodes in the currently active cluster?

- A.Run source env/bin/activate in a notebook setup script
- B.Use b in a notebook cell
- C.Use %pip install in a notebook cell
- D.Use %sh pip install in a notebook cell
- E.Install libraries from PyPI using the cluster UI

**Correct answer**
**C**

**Explanation**
Using **`%pip install`** in a notebook cell installs the Python package **at the notebook scope** and ensures it is **available on all nodes of the currently active cluster**. Databricks manages synchronization and restarts the Python environment as needed so executors see the package consistently.

**Why others are wrong**

* **A**: Virtualenv activation is not supported for cluster-wide distribution in Databricks notebooks.
* **B**: `%b` is not a valid Databricks magic for installing packages.
* **D**: `%sh pip install` installs only on the **driver node**, not across executors.
* **E**: Cluster UI installs are **cluster-scoped**, not notebook-scoped.

**Key takeaway (interview-ready)**
Use **`%pip install`** for **notebook-scoped, cluster-wide** Python package installation on Databricks.


---

### Question 112

Each configuration below is identical to the extent that each cluster has 400 GB total of RAM 160 total cores and only one Executor per VM.Given an extremely long-running job for which completion must be guaranteed, which cluster configuration will be able to guarantee completion of the job in light of one or more VM failures?

- A.• Total VMs: 8• 50 GB per Executor• 20 Cores / Executor
- B.• Total VMs: 16• 25 GB per Executor• 10 Cores / Executor
- C.• Total VMs: 1• 400 GB per Executor• 160 Cores/Executor
- D.• Total VMs: 4• 100 GB per Executor• 40 Cores / Executor
- E.• Total VMs: 2• 200 GB per Executor• 80 Cores / Executor

**Correct answer**
**B**

**Explanation**
To **guarantee completion** of an extremely long-running Spark job in the presence of **one or more VM failures**, the cluster must tolerate executor loss. This is best achieved by **having more executors with smaller resource footprints**.

Option **B** (16 VMs, 25 GB RAM, 10 cores per executor) provides the **highest level of fault tolerance**:

* Losing one or more VMs removes only a small fraction of total compute and memory.
* Spark can reschedule failed tasks on remaining executors.
* No single executor becomes a critical point of failure.

**Why others are wrong**

* **A**: Fewer executors → higher impact if a VM fails.
* **C**: Single executor → any VM failure kills the job.
* **D**: Too few executors; loss of one VM significantly reduces capacity.
* **E**: Still too few executors; each VM is a large blast radius.

**Key takeaway (interview-ready)**
For long-running Spark jobs where completion must be guaranteed, **maximize the number of executors and minimize per-executor size** to improve fault tolerance and resilience to VM failures.


---

### Question 113

A Delta Lake table in the Lakehouse named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.Immediately after each update succeeds, the data engineering team would like to determine the difference between the new version and the previous version of the table.Given the current implementation, which method can be used?

- A.Execute a query to calculate the difference between the new version and the previous version using Delta Lake’s built-in versioning and lime travel functionality.
- B.Parse the Delta Lake transaction log to identify all newly written data files.
- C.Parse the Spark event logs to identify those rows that were updated, inserted, or deleted.
- D.Execute DESCRIBE HISTORY customer_churn_params to obtain the full operation metrics for the update, including a log of all records that have been added or modified.
- E.Use Delta Lake’s change data feed to identify those records that have been updated, inserted, or deleted.

**Correct answer**
**A**

**Explanation**
Because the table is **overwritten nightly**, Delta Lake creates a **new table version** on each run. Delta Lake’s **time travel** allows querying two different versions of the same table (for example, `VERSION AS OF n` and `VERSION AS OF n-1`) and computing a diff directly in SQL. This works immediately after the overwrite without changing the existing pipeline.

**Why others are wrong**

* **B**: Parsing the transaction log files directly is low-level, unsupported for analytics, and unnecessary.
* **C**: Spark event logs capture execution details, not row-level data changes.
* **D**: `DESCRIBE HISTORY` shows operation metadata, not the actual row-level differences.
* **E**: Change Data Feed must be enabled **in advance** and is not available retroactively for existing overwrite logic.

**Key takeaway (interview-ready)**
For overwrite-based pipelines, **Delta Lake time travel** is the simplest way to compare the current table with its previous version—no pipeline changes required.


---

### Question 114

![Question 114 Image 1](examtopics_images/question_114_img_1.png)

![Question 114 Image 2](examtopics_images/question_114_img_2.png)

A data team’s Structured Streaming job is configured to calculate running aggregates for item sales to update a downstream marketing dashboard. The marketing team has introduced a new promotion, and they would like to add a new field to track the number of times this promotion code is used for each item. A junior data engineer suggests updating the existing query as follows. Note that proposed changes are in bold.Original query:Proposed query:Which step must also be completed to put the proposed query into production?

- A.Specify a new checkpointLocation
- B.Remove .option('mergeSchema', 'true') from the streaming write
- C.Increase the shuffle partitions to account for additional aggregates
- D.Run REFRESH TABLE delta.‛/item_agg‛

**Correct Answer: A. Specify a new checkpointLocation**

**Why it's correct**

When you modify the schema of a Structured Streaming query (adding the `new_member_promo` column), you must specify a **new checkpointLocation**. The checkpoint stores the query's state and schema metadata. Reusing the old checkpoint with a different schema causes incompatibility errors because Structured Streaming expects the schema to match what's in the checkpoint.

**Why others are wrong**

* **B. Remove .option('mergeSchema', 'true')** – Incorrect. The `mergeSchema` option is actually necessary here to handle schema evolution in the target Delta table. Removing it would prevent the new column from being added to the existing table and cause the write to fail.

* **C. Increase shuffle partitions for additional aggregates** – Irrelevant to putting the query into production. Shuffle partitions affect performance/parallelism but don't address the schema change issue. The query will fail due to checkpoint incompatibility regardless of partition count.

* **D. Run REFRESH TABLE delta./item_agg** – REFRESH TABLE is unnecessary. Delta tables automatically reflect new data and schema changes. This command is for refreshing metadata caches in certain scenarios but doesn't solve the checkpoint schema mismatch problem.

**Key takeaway**

When modifying the schema of a Structured Streaming query (adding/removing columns or changing aggregations), you **must use a new checkpointLocation** to avoid schema conflicts. The old checkpoint contains metadata about the previous schema and cannot be reused. For production deployments with schema changes, always create a fresh checkpoint directory.

---

### Question 115

When using CLI or REST API to get results from jobs with multiple tasks, which statement correctly describes the response structure?

- A.Each run of a job will have a unique job_id; all tasks within this job will have a unique job_id
- B.Each run of a job will have a unique job_id; all tasks within this job will have a unique task_id
- C.Each run of a job will have a unique orchestration_id; all tasks within this job will have a unique run_id
- D.Each run of a job will have a unique run_id; all tasks within this job will have a unique task_id
- E.Each run of a job will have a unique run_id; all tasks within this job will also have a unique run_id

**Correct answer**
**E**

**Explanation**
In Databricks Jobs (via CLI or REST API), a **job run** has a single **parent `run_id`**, and **each task within that run is executed as its own run**, with its **own unique `run_id`**. Task-level runs are linked to the parent run through fields like `parent_run_id` and `task_key`.

**Why others are wrong**

* **A**: `job_id` identifies the job definition, not individual runs or tasks.
* **B**: Tasks do not get a `task_id` in API responses; they get their own `run_id`.
* **C**: There is no `orchestration_id` concept in Databricks Jobs APIs.
* **D**: Tasks are not identified by `task_id`; they are identified by separate `run_id`s.

**Key takeaway (interview-ready)**
In multi-task Databricks jobs, **each task execution is its own run with a unique `run_id`**, even though they all belong to a single parent job run.


---

### Question 116

The data engineering team is configuring environments for development, testing, and production before beginning migration on a new data pipeline. The team requires extensive testing on both the code and data resulting from code execution, and the team wants to develop and test against data as similar to production data as possible.A junior data engineer suggests that production data can be mounted to the development and testing environments, allowing pre-production code to execute against production data. Because all users have admin privileges in the development environment, the junior data engineer has offered to configure permissions and mount this data for the team.Which statement captures best practices for this situation?

- A.All development, testing, and production code and data should exist in a single, unified workspace; creating separate environments for testing and development complicates administrative overhead.
- B.In environments where interactive code will be executed, production data should only be accessible with read permissions; creating isolated databases for each environment further reduces risks.
- C.As long as code in the development environment declares USE dev_db at the top of each notebook, there is no possibility of inadvertently committing changes back to production data sources.
- D.Because Delta Lake versions all data and supports time travel, it is not possible for user error or malicious actors to permanently delete production data; as such, it is generally safe to mount production data anywhere.
- E.Because access to production data will always be verified using passthrough credentials, it is safe to mount data to any Databricks development environment.

**Correct answer**
**B**

**Explanation**
Best practice is to **isolate environments** and **limit permissions**. If production data must be accessed from development or testing, it should be **read-only**, and data should be exposed via **separate databases or schemas** to prevent accidental writes, deletes, or schema changes while still enabling realistic testing.

**Why others are wrong**

* **A**: A single unified workspace increases blast radius and risk; environment isolation is a core Lakehouse best practice.
* **C**: `USE dev_db` does not prevent accidental writes to production tables if they are accessible.
* **D**: Delta Lake time travel does **not** eliminate the risk of data loss (e.g., VACUUM, retention policies, malicious deletes).
* **E**: Passthrough credentials do not replace the need for environment isolation and least-privilege access.

**Key takeaway (interview-ready)**
Production data can be used for testing **only with strict read-only access and environment isolation**; never allow writable production data in interactive dev or test environments.


---

### Question 117

A data engineer, User A, has promoted a pipeline to production by using the REST API to programmatically create several jobs. A DevOps engineer, User B, has configured an external orchestration tool to trigger job runs through the REST API. Both users authorized the REST API calls using their personal access tokens.A workspace admin, User C, inherits responsibility for managing this pipeline. User C uses the Databricks Jobs UI to take "Owner" privileges of each job. Jobs continue to be triggered using the credentials and tooling configured by User B.An application has been configured to collect and parse run information returned by the REST API. Which statement describes the value returned in the creator_user_name field?

- A.Once User C takes "Owner" privileges, their email address will appear in this field; prior to this, User A’s email address will appear in this field.
- B.User B’s email address will always appear in this field, as their credentials are always used to trigger the run.
- C.User A’s email address will always appear in this field, as they still own the underlying notebooks.
- D.Once User C takes "Owner" privileges, their email address will appear in this field; prior to this, User B’s email address will appear in this field.
- E.User C will only ever appear in this field if they manually trigger the job, otherwise it will indicate User B.


1\. Correct answer: The correct option is **C.** ==**User A’s email address will always appear in this field, as they still own the underlying notebooks**==**.**

**2\. Explanation**  
The `creator_user_name` field in the [Databricks Jobs REST API](https://www.databricks.com/) is an immutable field that stores the user who originally created the job. This field serves as an audit trail and is not affected by changes to job ownership or by which user's credentials are used to trigger specific job runs. User A created the job programmatically, so their email address remains in this field indefinitely.

---

**3\. Why others are wrong**

-   **A. Once User C takes "Owner" privileges, their email address will appear in this field; prior to this, User A’s email address will appear in this field.** The `creator_user_name` field never changes to User C's address; the field is immutable .
-   **B. User B’s email address will always appear in this field, as their credentials are always used to trigger the run.** User B's credentials trigger the *runs*, but User A *created* the job. The creator field is tied to the initial creation event, not subsequent triggers .
-   **D. Once User C takes "Owner" privileges, their email address will appear in this field; prior to this, User B’s email address will appear in this field.** This is incorrect as neither the owner change nor the triggering user affects the immutable creator field .
-   **E. User C will only ever appear in this field if they manually trigger the job, otherwise it will indicate User B.** User C will never appear in the *creator* field; only the original creator (User A) appears there .

**4\. Key takeaway (interview ready)**  
The `creator_user_name` field in Databricks Jobs is a fixed, immutable audit field identifying the original job creator. Job ownership changes or the credentials used to trigger runs (the `run_as` user) do not alter this historical record.

---

### Question 118

![Question 118 Image 1](examtopics_images/question_118_img_1.png)

A member of the data engineering team has submitted a short notebook that they wish to schedule as part of a larger data pipeline. Assume that the commands provided below produce the logically correct results when run as presented.Which command should be removed from the notebook before scheduling it as a job?

- A.Cmd 2
- B.Cmd 3
- C.Cmd 4
- D.Cmd 5

**Correct answer**
**D. Cmd 5**

**Explanation**
`display(finalDF)` is an **interactive, visualization-only action** intended for exploratory analysis. When scheduling a notebook as a **production job**, this command adds unnecessary overhead and provides no value to the pipeline’s outcome.

**Why others are wrong**

* **Cmd 2 (`printSchema`)**: Lightweight and harmless; often useful for debugging but not harmful in jobs.
* **Cmd 3 (`select`)**: Core transformation logic required for flattening the data.
* **Cmd 4 (`drop`)**: Required to remove the nested column before writing.

**Key takeaway (interview-ready)**
Before scheduling a Databricks notebook as a job, **remove interactive commands like `display()`**, which are meant for exploration—not production pipelines.


---

### Question 119

Which statement regarding Spark configuration on the Databricks platform is true?

- A.The Databricks REST API can be used to modify the Spark configuration properties for an interactive cluster without interrupting jobs currently running on the cluster.
- B.Spark configurations set within a notebook will affect all SparkSessions attached to the same interactive cluster.
- C.When the same Spark configuration property is set for an interactive cluster and a notebook attached to that cluster, the notebook setting will always be ignored.
- D.Spark configuration properties set for an interactive cluster with the Clusters UI will impact all notebooks attached to that cluster.

**Correct answer**
**D**

**Explanation**
Spark configuration properties set at the **cluster level** (via the Databricks Clusters UI) apply to **all notebooks attached to that cluster**. This ensures consistent behavior across users and workloads running on the same interactive cluster.

**Why others are wrong**

* **A**: Modifying Spark configs via the REST API typically requires a **cluster restart**, interrupting running jobs.
* **B**: Notebook-level Spark configs affect **only the SparkSession for that notebook**, not all sessions on the cluster.
* **C**: Notebook-level Spark configs can **override cluster-level settings** in many cases; they are not always ignored.

**Key takeaway (interview-ready)**
On Databricks, **cluster-level Spark configurations apply to all attached notebooks**, while notebook-level configs are session-scoped and may override cluster defaults.

---

### Question 120

The business reporting team requires that data for their dashboards be updated every hour. The total processing time for the pipeline that extracts, transforms, and loads the data for their pipeline runs in 10 minutes.Assuming normal operating conditions, which configuration will meet their service-level agreement requirements with the lowest cost?

- A.Configure a job that executes every time new data lands in a given directory
- B.Schedule a job to execute the pipeline once an hour on a new job cluster
- C.Schedule a Structured Streaming job with a trigger interval of 60 minutes
- D.Schedule a job to execute the pipeline once an hour on a dedicated interactive cluster

**Correct answer**
**B**

**Explanation**
Scheduling a **batch job once per hour on a new job cluster** meets the SLA (hourly updates, 10-minute runtime) at the **lowest cost**. Job clusters spin up only when needed and terminate after completion, avoiding idle compute costs.

**Why others are wrong**

* **A**: Event-driven execution may trigger more frequently than required, increasing cost.
* **C**: Structured Streaming with a 60-minute trigger keeps resources allocated continuously, costing more than batch.
* **D**: A dedicated interactive cluster incurs continuous cost even when the pipeline is idle.

**Key takeaway (interview-ready)**
For predictable, periodic workloads with short runtimes, **scheduled batch jobs on job clusters** are the most cost-efficient solution.


---

### Question 121

A Databricks SQL dashboard has been configured to monitor the total number of records present in a collection of Delta Lake tables using the following query pattern:SELECT COUNT (*) FROM table -Which of the following describes how results are generated each time the dashboard is updated?

- A.The total count of rows is calculated by scanning all data files
- B.The total count of rows will be returned from cached results unless REFRESH is run
- C.The total count of records is calculated from the Delta transaction logs
- D.The total count of records is calculated from the parquet file metadata

**Correct answer**
**C**

**Explanation**
Delta Lake maintains **row count statistics in the transaction log**. For queries like `SELECT COUNT(*) FROM table`, Databricks SQL can efficiently compute the result by **reading metadata from the Delta transaction log**, avoiding a full scan of data files.

**Why others are wrong**

* **A**: Delta Lake avoids full file scans for `COUNT(*)` when statistics are available.
* **B**: Results are not returned from a generic cache; they reflect the current committed Delta version.
* **D**: Parquet metadata alone does not reliably store total row counts across files in a way Spark uses for this query.

**Key takeaway (interview-ready)**
In Delta Lake, `COUNT(*)` is optimized by leveraging **transaction log statistics**, making it much faster than scanning Parquet data files.


---

### Question 122

![Question 122 Image 1](examtopics_images/question_122_img_1.png)

A Delta Lake table was created with the below query:Consider the following query:DROP TABLE prod.sales_by_store -If this statement is executed by a workspace admin, which result will occur?

- A.Data will be marked as deleted but still recoverable with Time Travel.
- B.The table will be removed from the catalog but the data will remain in storage.
- C.The table will be removed from the catalog and the data will be deleted.
- D.An error will occur because Delta Lake prevents the deletion of production data.

---

### Question 123

A developer has successfully configured their credentials for Databricks Repos and cloned a remote Git repository. They do not have privileges to make changes to the main branch, which is the only branch currently visible in their workspace.Which approach allows this user to share their code updates without the risk of overwriting the work of their teammates?

- A.Use Repos to create a new branch, commit all changes, and push changes to the remote Git repository.
- B.Use Repos to create a fork of the remote repository, commit all changes, and make a pull request on the source repository.
- C.Use Repos to pull changes from the remote Git repository; commit and push changes to a branch that appeared as changes were pulled.
- D.Use Repos to merge all differences and make a pull request back to the remote repository.

---

### Question 124

![Question 124 Image 1](examtopics_images/question_124_img_1.png)

The security team is exploring whether or not the Databricks secrets module can be leveraged for connecting to an external database.After testing the code with all Python variables being defined with strings, they upload the password to the secrets module and configure the correct permissions for the currently active user. They then modify their code to the following (leaving all other variables unchanged).Which statement describes what will happen when the above code is executed?

- A.The connection to the external table will succeed; the string "REDACTED" will be printed.
- B.An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the encoded password will be saved to DBFS.
- C.An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the password will be printed in plain text.
- D.The connection to the external table will succeed; the string value of password will be printed in plain text.

---

### Question 125

![Question 125 Image 1](examtopics_images/question_125_img_1.png)

The data science team has created and logged a production model using MLflow. The model accepts a list of column names and returns a new column of type DOUBLE.The following code correctly imports the production model, loads the customers table containing the customer_id key column into a DataFrame, and defines the feature columns needed for the model.Which code block will output a DataFrame with the schema "customer_id LONG, predictions DOUBLE"?

- A.df.map(lambda x:model(x[columns])).select("customer_id, predictions")
- B.df.select("customer_id",model(*columns).alias("predictions"))
- C.model.predict(df, columns)
- D.df.apply(model, columns).select("customer_id, predictions")

---

### Question 126

![Question 126 Image 1](examtopics_images/question_126_img_1.png)

A junior member of the data engineering team is exploring the language interoperability of Databricks notebooks. The intended outcome of the below code is to register a view of all sales that occurred in countries on the continent of Africa that appear in the geo_lookup table.Before executing the code, running SHOW TABLES on the current database indicates the database contains only two tables: geo_lookup and sales.What will be the outcome of executing these command cells m order m an interactive notebook?

- A.Both commands will succeed. Executing SHOW TABLES will show that countries_af and sales_af have been registered as views.
- B.Cmd 1 will succeed. Cmd 2 will search all accessible databases for a table or view named countries_af: if this entity exists, Cmd 2 will succeed.
- C.Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable representing a PySpark DataFrame.
- D.Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable containing a list of strings.

---

### Question 127

The data science team has requested assistance in accelerating queries on free-form text from user reviews. The data is currently stored in Parquet with the below schema:item_id INT, user_id INT, review_id INT, rating FLOAT, review STRINGThe review column contains the full text of the review left by the user. Specifically, the data science team is looking to identify if any of 30 key words exist in this field.A junior data engineer suggests converting this data to Delta Lake will improve query performance.Which response to the junior data engineer’s suggestion is correct?

- A.Delta Lake statistics are not optimized for free text fields with high cardinality.
- B.Delta Lake statistics are only collected on the first 4 columns in a table.
- C.ZORDER ON review will need to be run to see performance gains.
- D.The Delta log creates a term matrix for free text fields to support selective filtering.

---

### Question 128

The data engineering team has configured a job to process customer requests to be forgotten (have their data deleted). All user data that needs to be deleted is stored in Delta Lake tables using default table settings.The team has decided to process all deletions from the previous week as a batch job at 1am each Sunday. The total duration of this job is less than one hour. Every Monday at 3am, a batch job executes a series of VACUUM commands on all Delta Lake tables throughout the organization.The compliance officer has recently learned about Delta Lake's time travel functionality. They are concerned that this might allow continued access to deleted data.Assuming all delete logic is correctly implemented, which statement correctly addresses this concern?

- A.Because the VACUUM command permanently deletes all files containing deleted records, deleted records may be accessible with time travel for around 24 hours.
- B.Because the default data retention threshold is 24 hours, data files containing deleted records will be retained until the VACUUM job is run the following day.
- C.Because the default data retention threshold is 7 days, data files containing deleted records will be retained until the VACUUM job is run 8 days later.
- D.Because Delta Lake's delete statements have ACID guarantees, deleted records will be permanently purged from all storage systems as soon as a delete job completes.

---

### Question 129

Assuming that the Databricks CLI has been installed and configured correctly, which Databricks CLI command can be used to upload a custom Python Wheel to object storage mounted with the DBFS for use with a production job?

- A.configure
- B.fs
- C.workspace
- D.libraries

---

### Question 130

![Question 130 Image 1](examtopics_images/question_130_img_1.png)

![Question 130 Image 2](examtopics_images/question_130_img_2.png)

![Question 130 Image 3](examtopics_images/question_130_img_3.png)

The following table consists of items found in user carts within an e-commerce website.The following MERGE statement is used to update this table using an updates view, with schema evolution enabled on this table.How would the following update be handled?

- A.The update throws an error because changes to existing columns in the target schema are not supported.
- B.The new nested Field is added to the target schema, and dynamically read as NULL for existing unmatched records.
- C.The update is moved to a separate "rescued" column because it is missing a column expected in the target schema.
- D.The new nested field is added to the target schema, and files underlying existing records are updated to include NULL values for the new field.

---

### Question 131

A data engineer has been using a Databricks SQL dashboard to monitor the cleanliness of the input data to an ELT job. The ELT job has its Databricks SQL query that returns the number of input records containing unexpected NULL values. The data engineer wants their entire team to be notified via a messaging webhook whenever this value reaches 100.Which approach can the data engineer use to notify their entire team via a messaging webhook whenever the number of NULL values reaches 100?

- A.They can set up an Alert with a custom template.
- B.They can set up an Alert with a new email alert destination.
- C.They can set up an Alert with a new webhook alert destination.
- D.They can set up an Alert with one-time notifications.

---

### Question 132

An hourly batch job is configured to ingest data files from a cloud object storage container where each batch represent all records produced by the source system in a given hour. The batch job to process these records into the Lakehouse is sufficiently delayed to ensure no late-arriving data is missed. The user_id field represents a unique key for the data, which has the following schema:user_id BIGINT, username STRING, user_utc STRING, user_region STRING, last_login BIGINT, auto_pay BOOLEAN, last_updated BIGINTNew records are all ingested into a table named account_history which maintains a full record of all data in the same schema as the source. The next table in the system is named account_current and is implemented as a Type 1 table representing the most recent value for each unique user_id.Which implementation can be used to efficiently update the described account_current table as part of each hourly batch job assuming there are millions of user accounts and tens of thousands of records processed hourly?

- A.Filter records in account_history using the last_updated field and the most recent hour processed, making sure to deduplicate on username; write a merge statement to update or insert the most recent value for each username.
- B.Use Auto Loader to subscribe to new files in the account_history directory; configure a Structured Streaming trigger available job to batch update newly detected files into the account_current table.
- C.Overwrite the account_current table with each batch using the results of a query against the account_history table grouping by user_id and filtering for the max value of last_updated.
- D.Filter records in account_history using the last_updated field and the most recent hour processed, as well as the max last_login by user_id write a merge statement to update or insert the most recent value for each user_id.

---

### Question 133

![Question 133 Image 1](examtopics_images/question_133_img_1.png)

![Question 133 Image 2](examtopics_images/question_133_img_2.png)

The business intelligence team has a dashboard configured to track various summary metrics for retail stores. This includes total sales for the previous day alongside totals and averages for a variety of time periods. The fields required to populate this dashboard have the following schema:For demand forecasting, the Lakehouse contains a validated table of all itemized sales updated incrementally in near real-time. This table, named products_per_order, includes the following fields:Because reporting on long-term sales trends is less volatile, analysts using the new dashboard only require data to be refreshed once daily. Because the dashboard will be queried interactively by many users throughout a normal business day, it should return results quickly and reduce total compute associated with each materialization.Which solution meets the expectations of the end users while controlling and limiting possible costs?

- A.Populate the dashboard by configuring a nightly batch job to save the required values as a table overwritten with each update.
- B.Use Structured Streaming to configure a live dashboard against the products_per_order table within a Databricks notebook.
- C.Define a view against the products_per_order table and define the dashboard against this view.
- D.Use the Delta Cache to persist the products_per_order table in memory to quickly update the dashboard with each query.

---

### Question 134

A Delta lake table with CDF enabled table in the Lakehouse named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.The churn prediction model used by the ML team is fairly stable in production. The team is only interested in making predictions on records that have changed in the past 24 hours.Which approach would simplify the identification of these changed records?

- A.Apply the churn model to all rows in the customer_churn_params table, but implement logic to perform an upsert into the predictions table that ignores rows where predictions have not changed.
- B.Convert the batch job to a Structured Streaming job using the complete output mode; configure a Structured Streaming job to read from the customer_churn_params table and incrementally predict against the churn model.
- C.Replace the current overwrite logic with a merge statement to modify only those records that have changed; write logic to make predictions on the changed records identified by the change data feed.
- D.Modify the overwrite logic to include a field populated by calling spark.sql.functions.current_timestamp() as data are being written; use this field to identify records written on a particular date.

---

### Question 135

![Question 135 Image 1](examtopics_images/question_135_img_1.png)

A view is registered with the following code:Both users and orders are Delta Lake tables.Which statement describes the results of querying recent_orders?

- A.All logic will execute when the view is defined and store the result of joining tables to the DBFS; this stored data will be returned when the view is queried.
- B.Results will be computed and cached when the view is defined; these cached results will incrementally update as new records are inserted into source tables.
- C.All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query finishes.
- D.All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query began.

---

### Question 136

A data ingestion task requires a one-TB JSON dataset to be written out to Parquet with a target part-file size of 512 MB. Because Parquet is being used instead of Delta Lake, built-in file-sizing features such as Auto-Optimize & Auto-Compaction cannot be used.Which strategy will yield the best performance without shuffling data?

- A.Set spark.sql.files.maxPartitionBytes to 512 MB, ingest the data, execute the narrow transformations, and then write to parquet.
- B.Set spark.sql.shuffle.partitions to 2,048 partitions (1TB*1024*1024/512), ingest the data, execute the narrow transformations, optimize the data by sorting it (which automatically repartitions the data), and then write to parquet.
- C.Set spark.sql.adaptive.advisoryPartitionSizeInBytes to 512 MB bytes, ingest the data, execute the narrow transformations, coalesce to 2,048 partitions (1TB*1024*1024/512), and then write to parquet.
- D.Ingest the data, execute the narrow transformations, repartition to 2,048 partitions (1TB* 1024*1024/512), and then write to parquet.

---

### Question 137

Identify how the count_if function and the count where x is null can be usedConsider a table random_values with below data.What would be the output of below query?select count_if(col > 1) as count_a. count(*) as count_b.count(col1) as count_c from random_values col1012NULL -23

- A.3 6 5
- B.4 6 5
- C.3 6 6
- D.4 6 6

---

### Question 138

![Question 138 Image 1](examtopics_images/question_138_img_1.png)

A junior data engineer has been asked to develop a streaming data pipeline with a grouped aggregation using DataFrame df. The pipeline needs to calculate the average humidity and average temperature for each non-overlapping five-minute interval. Events are recorded once per minute per device.Streaming DataFrame df has the following schema:"device_id INT, event_time TIMESTAMP, temp FLOAT, humidity FLOAT"Code block:Which line of code correctly fills in the blank within the code block to complete this task?

- A.to_interval("event_time", "5 minutes").alias("time")
- B.window("event_time", "5 minutes").alias("time")
- C."event_time"
- D.lag("event_time", "10 minutes").alias("time")

---

### Question 139

A Structured Streaming job deployed to production has been resulting in higher than expected cloud storage costs. At present, during normal execution, each microbatch of data is processed in less than 3s; at least 12 times per minute, a microbatch is processed that contains 0 records. The streaming write was configured using the default trigger settings. The production job is currently scheduled alongside many other Databricks jobs in a workspace with instance pools provisioned to reduce start-up time for jobs with batch execution.Holding all other variables constant and assuming records need to be processed in less than 10 minutes, which adjustment will meet the requirement?

- A.Set the trigger interval to 3 seconds; the default trigger interval is consuming too many records per batch, resulting in spill to disk that can increase volume costs.
- B.Use the trigger once option and configure a Databricks job to execute the query every 10 minutes; this approach minimizes costs for both compute and storage.
- C.Set the trigger interval to 10 minutes; each batch calls APIs in the source storage account, so decreasing trigger frequency to maximum allowable threshold should minimize this cost.
- D.Set the trigger interval to 500 milliseconds; setting a small but non-zero trigger interval ensures that the source is not queried too frequently.

---

### Question 140

Which statement describes Delta Lake optimized writes?

- A.Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.
- B.An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.
- C.A shuffle occurs prior to writing to try to group similar data together resulting in fewer files instead of each executor writing multiple files based on directory partitions.
- D.Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.

---

### Question 141

Which statement characterizes the general programming model used by Spark Structured Streaming?

- A.Structured Streaming leverages the parallel processing of GPUs to achieve highly parallel data throughput.
- B.Structured Streaming is implemented as a messaging bus and is derived from Apache Kafka.
- C.Structured Streaming relies on a distributed network of nodes that hold incremental state values for cached stages.
- D.Structured Streaming models new data arriving in a data stream as new rows appended to an unbounded table.

---

### Question 142

Which configuration parameter directly affects the size of a spark-partition upon ingestion of data into Spark?

- A.spark.sql.files.maxPartitionBytes
- B.spark.sql.autoBroadcastJoinThreshold
- C.spark.sql.adaptive.advisoryPartitionSizeInBytes
- D.spark.sql.adaptive.coalescePartitions.minPartitionNum

---

### Question 143

A Spark job is taking longer than expected. Using the Spark UI, a data engineer notes that the Min, Median, and Max Durations for tasks in a particular stage show the minimum and median time to complete a task as roughly the same, but the max duration for a task to be roughly 100 times as long as the minimum.Which situation is causing increased duration of the overall job?

- A.Task queueing resulting from improper thread pool assignment.
- B.Spill resulting from attached volume storage being too small.
- C.Network latency due to some cluster nodes being in different regions from the source data
- D.Skew caused by more data being assigned to a subset of spark-partitions.

---

### Question 144

Each configuration below is identical to the extent that each cluster has 400 GB total of RAM, 160 total cores and only one Executor per VM.Given an extremely long-running job for which completion must be guaranteed, which cluster configuration will be able to guarantee completion of the job in light of one or more VM failures?

- A.• Total VMs: 8• 50 GB per Executor• 20 Cores / Executor
- B.• Total VMs: 16• 25 GB per Executor• 10 Cores / Executor
- C.• Total VMs: 1• 400 GB per Executor• 160 Cores/Executor
- D.• Total VMs: 4• 100 GB per Executor• 40 Cores / Executor

---

### Question 145

![Question 145 Image 1](examtopics_images/question_145_img_1.png)

A task orchestrator has been configured to run two hourly tasks. First, an outside system writes Parquet data to a directory mounted at /mnt/raw_orders/. After this data is written, a Databricks job containing the following code is executed:Assume that the fields customer_id and order_id serve as a composite key to uniquely identify each order, and that the time field indicates when the record was queued in the source system.If the upstream system is known to occasionally enqueue duplicate entries for a single order hours apart, which statement is correct?

- A.Duplicate records enqueued more than 2 hours apart may be retained and the orders table may contain duplicate records with the same customer_id and order_id.
- B.All records will be held in the state store for 2 hours before being deduplicated and committed to the orders table.
- C.The orders table will contain only the most recent 2 hours of records and no duplicates will be present.
- D.The orders table will not contain duplicates, but records arriving more than 2 hours late will be ignored and missing from the table.

---

### Question 146

A data engineer is configuring a pipeline that will potentially see late-arriving, duplicate records.In addition to de-duplicating records within the batch, which of the following approaches allows the data engineer to deduplicate data against previously processed records as it is inserted into a Delta table?

- A.Rely on Delta Lake schema enforcement to prevent duplicate records.
- B.VACUUM the Delta table after each batch completes.
- C.Perform an insert-only merge with a matching condition on a unique key.
- D.Perform a full outer join on a unique key and overwrite existing data.

---

### Question 147

![Question 147 Image 1](examtopics_images/question_147_img_1.png)

A junior data engineer seeks to leverage Delta Lake's Change Data Feed functionality to create a Type 1 table representing all of the values that have ever been valid for all rows in a bronze table created with the property delta.enableChangeDataFeed = true. They plan to execute the following code as a daily job:Which statement describes the execution and results of running the above query multiple times?

- A.Each time the job is executed, newly updated records will be merged into the target table, overwriting previous values with the same primary keys.
- B.Each time the job is executed, the entire available history of inserted or updated records will be appended to the target table, resulting in many duplicate entries.
- C.Each time the job is executed, only those records that have been inserted or updated since the last execution will be appended to the target table, giving the desired result.
- D.Each time the job is executed, the differences between the original and current versions are calculated; this may result in duplicate entries for some records.

---

### Question 148

A DLT pipeline includes the following streaming tables:•	raw_iot ingests raw device measurement data from a heart rate tracking device.•	bpm_stats incrementally computes user statistics based on BPM measurements from raw_iot.How can the data engineer configure this pipeline to be able to retain manually deleted or updated records in the raw_iot table, while recomputing the downstream table bpm_stats table when a pipeline update is run?

- A.Set the pipelines.reset.allowed property to false on raw_iot
- B.Set the skipChangeCommits flag to true on raw_iot
- C.Set the pipelines.reset.allowed property to false on bpm_stats
- D.Set the skipChangeCommits flag to true on bpm_stats

---

### Question 149

A data pipeline uses Structured Streaming to ingest data from Apache Kafka to Delta Lake. Data is being stored in a bronze table, and includes the Kafka-generated timestamp, key, and value. Three months after the pipeline is deployed, the data engineering team has noticed some latency issues during certain times of the day.A senior data engineer updates the Delta Table's schema and ingestion logic to include the current timestamp (as recorded by Apache Spark) as well as the Kafka topic and partition. The team plans to use these additional metadata fields to diagnose the transient processing delays.Which limitation will the team face while diagnosing this problem?

- A.New fields will not be computed for historic records.
- B.Spark cannot capture the topic and partition fields from a Kafka source.
- C.Updating the table schema requires a default value provided for each field added.
- D.Updating the table schema will invalidate the Delta transaction log metadata.

---

### Question 150

![Question 150 Image 1](examtopics_images/question_150_img_1.png)

A nightly job ingests data into a Delta Lake table using the following code:The next step in the pipeline requires a function that returns an object that can be used to manipulate new records that have not yet been processed to the next table in the pipeline.Which code snippet completes this function definition?def new_records():

- A.return spark.readStream.table("bronze")
- B.return spark.read.option("readChangeFeed", "true").table ("bronze")
- C.
- D.
- ![alt text](examtopics_images/question_150_img_2.png)

---



### Question 151

A junior data engineer is working to implement logic for a Lakehouse table named silver_device_recordings. The source data contains 100 unique fields in a highly nested JSON structure.The silver_device_recordings table will be used downstream to power several production monitoring dashboards and a production model. At present, 45 of the 100 fields are being used in at least one of these applications.The data engineer is trying to determine the best approach for dealing with schema declaration given the highly-nested structure of the data and the numerous fields.Which of the following accurately presents information about Delta Lake and Databricks that may impact their decision-making process?

- A.The Tungsten encoding used by Databricks is optimized for storing string data; newly-added native support for querying JSON strings means that string types are always most efficient.
- B.Because Delta Lake uses Parquet for data storage, data types can be easily evolved by just modifying file footer information in place.
- C.Schema inference and evolution on Databricks ensure that inferred types will always accurately match the data types used by downstream systems.
- D.Because Databricks will infer schema using types that allow all observed data to be processed, setting types manually provides greater assurance of data quality enforcement.

---

### Question 152

![Question 152 Image 1](examtopics_images/question_152_img_1.png)

The data engineering team maintains the following code:Assuming that this code produces logically correct results and the data in the source tables has been de-duplicated and validated, which statement describes what will occur when this code is executed?

- A.A batch job will update the enriched_itemized_orders_by_account table, replacing only those rows that have different values than the current version of the table, using accountID as the primary key.
- B.The enriched_itemized_orders_by_account table will be overwritten using the current valid version of data in each of the three tables referenced in the join logic.
- C.No computation will occur until enriched_itemized_orders_by_account is queried; upon query materialization, results will be calculated using the current valid version of data in each of the three tables referenced in the join logic.
- D.An incremental job will detect if new rows have been written to any of the source tables; if new rows are detected, all results will be recalculated and used to overwrite the enriched_itemized_orders_by_account table.

---

### Question 153

The data engineering team is configuring environments for development, testing, and production before beginning migration on a new data pipeline. The team requires extensive testing on both the code and data resulting from code execution, and the team wants to develop and test against data as similar to production data as possible.A junior data engineer suggests that production data can be mounted to the development and testing environments, allowing pre-production code to execute against production data. Because all users have admin privileges in the development environment, the junior data engineer has offered to configure permissions and mount this data for the team.Which statement captures best practices for this situation?

- A.All development, testing, and production code and data should exist in a single, unified workspace; creating separate environments for testing and development complicates administrative overhead.
- B.In environments where interactive code will be executed, production data should only be accessible with read permissions; creating isolated databases for each environment further reduces risks.
- C.Because access to production data will always be verified using passthrough credentials, it is safe to mount data to any Databricks development environment.
- D.Because Delta Lake versions all data and supports time travel, it is not possible for user error or malicious actors to permanently delete production data; as such, it is generally safe to mount production data anywhere.

---

### Question 154

The data architect has mandated that all tables in the Lakehouse should be configured as external Delta Lake tables.Which approach will ensure that this requirement is met?

- A.Whenever a database is being created, make sure that the LOCATION keyword is used.
- B.When the workspace is being configured, make sure that external cloud object storage has been mounted.
- C.Whenever a table is being created, make sure that the LOCATION keyword is used.
- D.When tables are created, make sure that the UNMANAGED keyword is used in the CREATE TABLE statement.

---

### Question 155

The marketing team is looking to share data in an aggregate table with the sales organization, but the field names used by the teams do not match, and a number of marketing-specific fields have not been approved for the sales org.Which of the following solutions addresses the situation while emphasizing simplicity?

- A.Create a view on the marketing table selecting only those fields approved for the sales team; alias the names of any fields that should be standardized to the sales naming conventions.
- B.Create a new table with the required schema and use Delta Lake's DEEP CLONE functionality to sync up changes committed to one table to the corresponding table.
- C.Use a CTAS statement to create a derivative table from the marketing table; configure a production job to propagate changes.
- D.Add a parallel table write to the current production pipeline, updating a new sales table that varies as required from the marketing table.

---

### Question 156

A Delta Lake table representing metadata about content posts from users has the following schema:user_id LONG, post_text STRING, post_id STRING, longitude FLOAT, latitude FLOAT, post_time TIMESTAMP, date DATEThis table is partitioned by the date column. A query is run with the following filter:longitude < 20 & longitude > -20Which statement describes how data will be filtered?

- A.Statistics in the Delta Log will be used to identify partitions that might Include files in the filtered range.
- B.No file skipping will occur because the optimizer does not know the relationship between the partition column and the longitude.
- C.The Delta Engine will scan the parquet file footers to identify each row that meets the filter criteria.
- D.Statistics in the Delta Log will be used to identify data files that might include records in the filtered range.

---

### Question 157

A small company based in the United States has recently contracted a consulting firm in India to implement several new data engineering pipelines to power artificial intelligence applications. All the company's data is stored in regional cloud storage in the United States.The workspace administrator at the company is uncertain about where the Databricks workspace used by the contractors should be deployed.Assuming that all data governance considerations are accounted for, which statement accurately informs this decision?

- A.Databricks runs HDFS on cloud volume storage; as such, cloud virtual machines must be deployed in the region where the data is stored.
- B.Databricks workspaces do not rely on any regional infrastructure; as such, the decision should be made based upon what is most convenient for the workspace administrator.
- C.Cross-region reads and writes can incur significant costs and latency; whenever possible, compute should be deployed in the same region the data is stored.
- D.Databricks notebooks send all executable code from the user’s browser to virtual machines over the open internet; whenever possible, choosing a workspace region near the end users is the most secure.

---

### Question 158

![Question 158 Image 1](examtopics_images/question_158_img_1.png)

A CHECK constraint has been successfully added to the Delta table named activity_details using the following logic:A batch job is attempting to insert new records to the table, including a record where latitude = 45.50 and longitude = 212.67.Which statement describes the outcome of this batch insert?

- A.The write will insert all records except those that violate the table constraints; the violating records will be reported in a warning log.
- B.The write will fail completely because of the constraint violation and no records will be inserted into the target table.
- C.The write will insert all records except those that violate the table constraints; the violating records will be recorded to a quarantine table.
- D.The write will include all records in the target table; any violations will be indicated in the boolean column named valid_coordinates.

---

### Question 159

A junior data engineer is migrating a workload from a relational database system to the Databricks Lakehouse. The source system uses a star schema, leveraging foreign key constraints and multi-table inserts to validate records on write.Which consideration will impact the decisions made by the engineer while migrating this workload?

- A.Databricks only allows foreign key constraints on hashed identifiers, which avoid collisions in highly-parallel writes.
- B.Foreign keys must reference a primary key field; multi-table inserts must leverage Delta Lake’s upsert functionality.
- C.Committing to multiple tables simultaneously requires taking out multiple table locks and can lead to a state of deadlock.
- D.All Delta Lake transactions are ACID compliant against a single table, and Databricks does not enforce foreign key constraints.

---

### Question 160

A data architect has heard about Delta Lake’s built-in versioning and time travel capabilities. For auditing purposes, they have a requirement to maintain a full record of all valid street addresses as they appear in the customers table.The architect is interested in implementing a Type 1 table, overwriting existing records with new values and relying on Delta Lake time travel to support long-term auditing. A data engineer on the project feels that a Type 2 table will provide better performance and scalability.Which piece of information is critical to this decision?

- A.Data corruption can occur if a query fails in a partially completed state because Type 2 tables require setting multiple fields in a single update.
- B.Shallow clones can be combined with Type 1 tables to accelerate historic queries for long-term versioning.
- C.Delta Lake time travel cannot be used to query previous versions of these tables because Type 1 changes modify data files in place.
- D.Delta Lake time travel does not scale well in cost or latency to provide a long-term versioning solution.

---

### Question 161

![Question 161 Image 1](examtopics_images/question_161_img_1.png)

A data engineer wants to join a stream of advertisement impressions (when an ad was shown) with another stream of user clicks on advertisements to correlate when impressions led to monetizable clicks.In the code below, Impressions is a streaming DataFrame with a watermark ("event_time", "10 minutes")The data engineer notices the query slowing down significantly.Which solution would improve the performance?

- A.Joining on event time constraint: clickTime >= impressionTime AND clickTime <= impressionTime interval 1 hour
- B.Joining on event time constraint: clickTime + 3 hours < impressionTime - 2 hours
- C.Joining on event time constraint: clickTime == impressionTime using a leftOuter join
- D.Joining on event time constraint: clickTime >= impressionTime - interval 3 hours and removing watermarks

---

### Question 162

A junior data engineer has manually configured a series of jobs using the Databricks Jobs UI. Upon reviewing their work, the engineer realizes that they are listed as the "Owner" for each job. They attempt to transfer "Owner" privileges to the "DevOps" group, but cannot successfully accomplish this task.Which statement explains what is preventing this privilege transfer?

- A.Databricks jobs must have exactly one owner; "Owner" privileges cannot be assigned to a group.
- B.The creator of a Databricks job will always have "Owner" privileges; this configuration cannot be changed.
- C.Only workspace administrators can grant "Owner" privileges to a group.
- D.A user can only transfer job ownership to a group if they are also a member of that group.

---

### Question 163

![Question 163 Image 1](examtopics_images/question_163_img_1.png)

A table named user_ltv is being used to create a view that will be used by data analysts on various teams. Users in the workspace are configured into groups, which are used for setting up data access using ACLs.The user_ltv table has the following schema:email STRING, age INT, ltv INTThe following view definition is executed:An analyst who is not a member of the auditing group executes the following query:SELECT * FROM user_ltv_no_minorsWhich statement describes the results returned by this query?

- A.All columns will be displayed normally for those records that have an age greater than 17; records not meeting this condition will be omitted.
- B.All age values less than 18 will be returned as null values, all other columns will be returned with the values in user_ltv.
- C.All values for the age column will be returned as null values, all other columns will be returned with the values in user_ltv.
- D.All columns will be displayed normally for those records that have an age greater than 18; records not meeting this condition will be omitted.

---

### Question 164

All records from an Apache Kafka producer are being ingested into a single Delta Lake table with the following schema:key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONGThere are 5 unique topics being ingested. Only the "registration" topic contains Personal Identifiable Information (PII). The company wishes to restrict access to PII. The company also wishes to only retain records containing PII in this table for 14 days after initial ingestion. However, for non-PII information, it would like to retain these records indefinitely.Which solution meets the requirements?

- A.All data should be deleted biweekly; Delta Lake's time travel functionality should be leveraged to maintain a history of non-PII information.
- B.Data should be partitioned by the registration field, allowing ACLs and delete statements to be set for the PII directory.
- C.Data should be partitioned by the topic field, allowing ACLs and delete statements to leverage partition boundaries.
- D.Separate object storage containers should be specified based on the partition field, allowing isolation at the storage level.

---

### Question 165

![Question 165 Image 1](examtopics_images/question_165_img_1.png)

The data governance team is reviewing code used for deleting records for compliance with GDPR. The following logic has been implemented to propagate delete requests from the user_lookup table to the user_aggregates table.Assuming that user_id is a unique identifying key and that all users that have requested deletion have been removed from the user_lookup table, which statement describes whether successfully executing the above logic guarantees that the records to be deleted from the user_aggregates table are no longer accessible and why?

- A.No; the Delta Lake DELETE command only provides ACID guarantees when combined with the MERGE INTO command.
- B.No; files containing deleted records may still be accessible with time travel until a VACUUM command is used to remove invalidated data files.
- C.No; the change data feed only tracks inserts and updates, not deleted records.
- D.Yes; Delta Lake ACID guarantees provide assurance that the DELETE command succeeded fully and permanently purged these records.

---

### Question 166

![Question 166 Image 1](examtopics_images/question_166_img_1.png)

![Question 166 Image 2](examtopics_images/question_166_img_2.png)

An external object storage container has been mounted to the location /mnt/finance_eda_bucket.The following logic was executed to create a database for the finance team:After the database was successfully created and permissions configured, a member of the finance team runs the following code:If all users on the finance team are members of the finance group, which statement describes how the tx_sales table will be created?

- A.A logical table will persist the query plan to the Hive Metastore in the Databricks control plane.
- B.An external table will be created in the storage container mounted to /mnt/finance_eda_bucket.
- C.A managed table will be created in the DBFS root storage container.
- D.An managed table will be created in the storage container mounted to /mnt/finance_eda_bucket.

---

### Question 167

A data engineer is designing a data pipeline. The source system generates files in a shared directory that is also used by other processes. As a result, the files should be kept as is and will accumulate in the directory. The data engineer needs to identify which files are new since the previous run in the pipeline, and set up the pipeline to only ingest those new files with each run.Which of the following tools can the data engineer use to solve this problem?

- A.Unity Catalog
- B.Delta Lake
- C.Databricks SQL
- D.Auto Loader

---

### Question 168

What is the retention of job run history?

- A.It is retained until you export or delete job run logs
- B.It is retained for 30 days, during which time you can deliver job run logs to DBFS or S3
- C.It is retained for 60 days, during which you can export notebook run results to HTML
- D.It is retained for 60 days, after which logs are archived

---

### Question 169

A data engineer, User A, has promoted a new pipeline to production by using the REST API to programmatically create several jobs. A DevOps engineer, User B, has configured an external orchestration tool to trigger job runs through the REST API. Both users authorized the REST API calls using their personal access tokens.Which statement describes the contents of the workspace audit logs concerning these events?

- A.Because the REST API was used for job creation and triggering runs, a Service Principal will be automatically used to identify these events.
- B.Because User A created the jobs, their identity will be associated with both the job creation events and the job run events.
- C.Because these events are managed separately, User A will have their identity associated with the job creation events and User B will have their identity associated with the job run events.
- D.Because the REST API was used for job creation and triggering runs, user identity will not be captured in the audit logs.

---

### Question 170

A distributed team of data analysts share computing resources on an interactive cluster with autoscaling configured. In order to better manage costs and query throughput, the workspace administrator is hoping to evaluate whether cluster upscaling is caused by many concurrent users or resource-intensive queries.In which location can one review the timeline for cluster resizing events?

- A.Workspace audit logs
- B.Driver's log file
- C.Ganglia
- D.Cluster Event Log

---

### Question 171

When evaluating the Ganglia Metrics for a given cluster with 3 executor nodes, which indicator would signal proper utilization of the VM's resources?

- A.The five Minute Load Average remains consistent/flat
- B.CPU Utilization is around 75%
- C.Network I/O never spikes
- D.Total Disk Space remains constant

---

### Question 172

The data engineer is using Spark's MEMORY_ONLY storage level.Which indicators should the data engineer look for in the Spark UI's Storage tab to signal that a cached table is not performing optimally?

- A.On Heap Memory Usage is within 75% of Off Heap Memory Usage
- B.The RDD Block Name includes the “*” annotation signaling a failure to cache
- C.Size on Disk is > 0
- D.The number of Cached Partitions > the number of Spark Partitions

---

### Question 173

![Question 173 Image 1](examtopics_images/question_173_img_1.png)

Review the following error traceback:Which statement describes the error being raised?

- A.There is a syntax error because the heartrate column is not correctly identified as a column.
- B.There is no column in the table named heartrateheartrateheartrate
- C.There is a type error because a column object cannot be multiplied.
- D.There is a type error because a DataFrame object cannot be multiplied.

---

### Question 174

What is a method of installing a Python package scoped at the notebook level to all nodes in the currently active cluster?

- A.Run source env/bin/activate in a notebook setup script
- B.Install libraries from PyPI using the cluster UI
- C.Use %pip install in a notebook cell
- D.Use %sh pip install in a notebook cell

---

### Question 175

A Databricks single-task workflow fails at the last task due to an error in a notebook. The data engineer fixes the mistake in the notebook.What should the data engineer do to rerun the workflow?

- A.Repair the task
- B.Rerun the pipeline
- C.Restart the cluster
- D.Switch the cluster

---

### Question 176

Incorporating unit tests into a PySpark application requires upfront attention to the design of your jobs, or a potentially significant refactoring of existing code.Which benefit offsets this additional effort?

- A.Improves the quality of your data
- B.Validates a complete use case of your application
- C.Troubleshooting is easier since all steps are isolated and tested individually
- D.Ensures that all steps interact correctly to achieve the desired end result

---

### Question 177

What describes integration testing?

- A.It validates an application use case.
- B.It validates behavior of individual elements of an application,
- C.It requires an automated testing framework.
- D.It validates interactions between subsystems of your application.

---

### Question 178

The Databricks CLI is used to trigger a run of an existing job by passing the job_id parameter. The response that the job run request has been submitted successfully includes a field run_id.Which statement describes what the number alongside this field represents?

- A.The job_id and number of times the job has been run are concatenated and returned.
- B.The globally unique ID of the newly triggered run.
- C.The number of times the job definition has been run in this workspace.
- D.The job_id is returned in this field.

---

### Question 179

A Databricks job has been configured with three tasks, each of which is a Databricks notebook. Task A does not depend on other tasks. Tasks B and C run in parallel, with each having a serial dependency on task A.What will be the resulting state if tasks A and B complete successfully but task C fails during a scheduled run?

- A.All logic expressed in the notebook associated with tasks A and B will have been successfully completed; some operations in task C may have completed successfully.
- B.Unless all tasks complete successfully, no changes will be committed to the Lakehouse; because task C failed, all commits will be rolled back automatically.
- C.Because all tasks are managed as a dependency graph, no changes will be committed to the Lakehouse until all tasks have successfully been completed.
- D.All logic expressed in the notebook associated with tasks A and B will have been successfully completed; any changes made in task C will be rolled back due to task failure.

---

### Question 180

When scheduling Structured Streaming jobs for production, which configuration automatically recovers from query failures and keeps costs low?

- A.Cluster: New Job Cluster;Retries: Unlimited;Maximum Concurrent Runs: 1
- B.Cluster: New Job Cluster;Retries: Unlimited;Maximum Concurrent Runs: Unlimited
- C.Cluster: Existing All-Purpose Cluster;Retries: Unlimited;Maximum Concurrent Runs: 1
- D.Cluster: New Job Cluster;Retries: None;Maximum Concurrent Runs: 1

---

### Question 181

![Question 181 Image 1](examtopics_images/question_181_img_1.png)

A Delta Lake table was created with the below query:Realizing that the original query had a typographical error, the below code was executed:ALTER TABLE prod.sales_by_stor RENAME TO prod.sales_by_storeWhich result will occur after running the second command?

- A.The table reference in the metastore is updated.
- B.All related files and metadata are dropped and recreated in a single ACID transaction.
- C.The table name change is recorded in the Delta transaction log.
- D.A new Delta transaction log is created for the renamed table.

---

### Question 182

![Question 182 Image 1](examtopics_images/question_182_img_1.png)

The data engineering team has configured a Databricks SQL query and alert to monitor the values in a Delta Lake table. The recent_sensor_recordings table contains an identifying sensor_id alongside the timestamp and temperature for the most recent 5 minutes of recordings.The below query is used to create the alert:The query is set to refresh each minute and always completes in less than 10 seconds. The alert is set to trigger when mean (temperature) > 120. Notifications are triggered to be sent at most every 1 minute.If this alert raises notifications for 3 consecutive minutes and then stops, which statement must be true?

- A.The total average temperature across all sensors exceeded 120 on three consecutive executions of the query
- B.The average temperature recordings for at least one sensor exceeded 120 on three consecutive executions of the query
- C.The source query failed to update properly for three consecutive minutes and then restarted
- D.The maximum temperature recording for at least one sensor exceeded 120 on three consecutive executions of the query

---

### Question 183

A junior developer complains that the code in their notebook isn't producing the correct results in the development environment. A shared screenshot reveals that while they're using a notebook versioned with Databricks Repos, they're using a personal branch that contains old logic. The desired branch named dev-2.3.9 is not available from the branch selection dropdown.Which approach will allow this developer to review the current logic for this notebook?

- A.Use Repos to make a pull request use the Databricks REST API to update the current branch to dev-2.3.9
- B.Use Repos to pull changes from the remote Git repository and select the dev-2.3.9 branch.
- C.Use Repos to checkout the dev-2.3.9 branch and auto-resolve conflicts with the current branch
- D.Use Repos to merge the current branch and the dev-2.3.9 branch, then make a pull request to sync with the remote repository

---

### Question 184

Two of the most common data locations on Databricks are the DBFS root storage and external object storage mounted with dbutils.fs.mount().Which of the following statements is correct?

- A.DBFS is a file system protocol that allows users to interact with files stored in object storage using syntax and guarantees similar to Unix file systems.
- B.By default, both the DBFS root and mounted data sources are only accessible to workspace administrators.
- C.The DBFS root is the most secure location to store data, because mounted storage volumes must have full public read and write permissions.
- D.The DBFS root stores files in ephemeral block volumes attached to the driver, while mounted directories will always persist saved data to external storage between sessions.

---

### Question 185

An upstream system has been configured to pass the date for a given batch of data to the Databricks Jobs API as a parameter. The notebook to be scheduled will use this parameter to load data with the following code:df = spark.read.format("parquet").load(f"/mnt/source/(date)")Which code block should be used to create the date Python variable used in the above code block?

- A.date = spark.conf.get("date")
- B.import sysdate = sys.argv[1]
- C.date = dbutils.notebooks.getParam("date")
- D.dbutils.widgets.text("date", "null")date = dbutils.widgets.get("date")

---

### Question 186

The Databricks workspace administrator has configured interactive clusters for each of the data engineering groups. To control costs, clusters are set to terminate after 30 minutes of inactivity. Each user should be able to execute workloads against their assigned clusters at any time of the day.Assuming users have been added to a workspace but not granted any permissions, which of the following describes the minimal permissions a user would need to start and attach to an already configured cluster.

- A."Can Manage" privileges on the required cluster
- B.Cluster creation allowed, "Can Restart" privileges on the required cluster
- C.Cluster creation allowed, "Can Attach To" privileges on the required cluster
- D."Can Restart" privileges on the required cluster

---

### Question 187

![Question 187 Image 1](examtopics_images/question_187_img_1.png)

The data science team has created and logged a production model using MLflow. The following code correctly imports and applies the production model to output the predictions as a new DataFrame named preds with the schema "customer_id LONG, predictions DOUBLE, date DATE".The data science team would like predictions saved to a Delta Lake table with the ability to compare all predictions across time. Churn predictions will be made at most once per day.Which code block accomplishes this task while minimizing potential compute costs?

- A.preds.write.mode("append").saveAsTable("churn_preds")
- B.preds.write.format("delta").save("/preds/churn_preds")
- C.
- D.

---

### Question 188

![Question 188 Image 1](examtopics_images/question_188_img_1.png)

The following code has been migrated to a Databricks notebook from a legacy workload:The code executes successfully and provides the logically correct results, however, it takes over 20 minutes to extract and load around 1 GB of data.Which statement is a possible explanation for this behavior?

- A.%sh triggers a cluster restart to collect and install Git. Most of the latency is related to cluster startup time.
- B.Instead of cloning, the code should use %sh pip install so that the Python code can get executed in parallel across all nodes in a cluster.
- C.%sh does not distribute file moving operations; the final line of code should be updated to use %fs instead.
- D.%sh executes shell code on the driver node. The code does not take advantage of the worker nodes or Databricks optimized Spark.

---

### Question 189

A Delta table of weather records is partitioned by date and has the below schema:date DATE, device_id INT, temp FLOAT, latitude FLOAT, longitude FLOATTo find all the records from within the Arctic Circle, you execute a query with the below filter:latitude > 66.3Which statement describes how the Delta engine identifies which files to load?

- A.All records are cached to an operational database and then the filter is applied
- B.The Parquet file footers are scanned for min and max statistics for the latitude column
- C.The Hive metastore is scanned for min and max statistics for the latitude column
- D.The Delta log is scanned for min and max statistics for the latitude column

---

### Question 190

In order to prevent accidental commits to production data, a senior data engineer has instituted a policy that all development work will reference clones of Delta Lake tables. After testing both DEEP and SHALLOW CLONE, development tables are created using SHALLOW CLONE.A few weeks after initial table creation, the cloned versions of several tables implemented as Type 1 Slowly Changing Dimension (SCD) stop working. The transaction logs for the source tables show that VACUUM was run the day before.Which statement describes why the cloned tables are no longer working?

- A.Because Type 1 changes overwrite existing records, Delta Lake cannot guarantee data consistency for cloned tables.
- B.Running VACUUM automatically invalidates any shallow clones of a table; DEEP CLONE should always be used when a cloned table will be repeatedly queried.
- C.The data files compacted by VACUUM are not tracked by the cloned metadata; running REFRESH on the cloned table will pull in recent changes.
- D.The metadata created by the CLONE operation is referencing data files that were purged as invalid by the VACUUM command.

---

### Question 191

![Question 191 Image 1](examtopics_images/question_191_img_1.png)

A junior data engineer has configured a workload that posts the following JSON to the Databricks REST API endpoint 2.0/jobs/create.Assuming that all configurations and referenced resources are available, which statement describes the result of executing this workload three times?

- A.The logic defined in the referenced notebook will be executed three times on the referenced existing all purpose cluster.
- B.The logic defined in the referenced notebook will be executed three times on new clusters with the configurations of the provided cluster ID.
- C.Three new jobs named "Ingest new data" will be defined in the workspace, but no jobs will be executed.
- D.One new job named "Ingest new data" will be defined in the workspace, but it will not be executed.

---

### Question 192

A Delta Lake table in the Lakehouse named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.Immediately after each update succeeds, the data engineering team would like to determine the difference between the new version and the previous version of the table.Given the current implementation, which method can be used?

- A.Execute a query to calculate the difference between the new version and the previous version using Delta Lake’s built-in versioning and lime travel functionality.
- B.Parse the Delta Lake transaction log to identify all newly written data files.
- C.Parse the Spark event logs to identify those rows that were updated, inserted, or deleted.
- D.Execute DESCRIBE HISTORY customer_churn_params to obtain the full operation metrics for the update, including a log of all records that have been added or modified.

---

### Question 193

![Question 193 Image 1](examtopics_images/question_193_img_1.png)

A view is registered with the following code:Both users and orders are Delta Lake tables.Which statement describes the results of querying recent_orders?

- A.The versions of each source table will be stored in the table transaction log; query results will be saved to DBFS with each query.
- B.All logic will execute when the table is defined and store the result of joining tables to the DBFS; this stored data will be returned when the table is queried.
- C.All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query finishes.
- D.All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query began.

---

### Question 194

A data engineer is performing a join operation to combine values from a static userLookup table with a streaming DataFrame streamingDF.Which code block attempts to perform an invalid stream-static join?

- A.userLookup.join(streamingDF, ["user_id"], how="right")
- B.streamingDF.join(userLookup, ["user_id"], how="inner")
- C.userLookup.join(streamingDF, ["user_id"), how="inner")
- D.userLookup.join(streamingDF, ["user_id"], how="left")

---

### Question 195

![Question 195 Image 1](examtopics_images/question_195_img_1.png)

A junior data engineer has been asked to develop a streaming data pipeline with a grouped aggregation using DataFrame df. The pipeline needs to calculate the average humidity and average temperature for each non-overlapping five-minute interval. Incremental state information should be maintained for 10 minutes for late-arriving data.Streaming DataFrame df has the following schema:"device_id INT, event_time TIMESTAMP, temp FLOAT, humidity FLOAT"Code block:Choose the response that correctly fills in the blank within the code block to complete this task.

- A.withWatermark("event_time", "10 minutes")
- B.awaitArrival("event_time", "10 minutes")
- C.await("event_time + ‘10 minutes'")
- D.slidingWindow("event_time", "10 minutes")

---

### Question 196

![Question 196 Image 1](examtopics_images/question_196_img_1.png)

A data architect has designed a system in which two Structured Streaming jobs will concurrently write to a single bronze Delta table. Each job is subscribing to a different topic from an Apache Kafka source, but they will write data with the same schema. To keep the directory structure simple, a data engineer has decided to nest a checkpoint directory to be shared by both streams.The proposed directory structure is displayed below:Which statement describes whether this checkpoint directory structure is valid for the given scenario and why?

- A.No; Delta Lake manages streaming checkpoints in the transaction log.
- B.Yes; both of the streams can share a single checkpoint directory.
- C.No; only one stream can write to a Delta Lake table.
- D.No; each of the streams needs to have its own checkpoint directory.

---

### Question 197

A Structured Streaming job deployed to production has been experiencing delays during peak hours of the day. At present, during normal execution, each microbatch of data is processed in less than 3 seconds. During peak hours of the day, execution time for each microbatch becomes very inconsistent, sometimes exceeding 30 seconds. The streaming write is currently configured with a trigger interval of 10 seconds.Holding all other variables constant and assuming records need to be processed in less than 10 seconds, which adjustment will meet the requirement?

- A.Decrease the trigger interval to 5 seconds; triggering batches more frequently allows idle executors to begin processing the next batch while longer running tasks from previous batches finish.
- B.Decrease the trigger interval to 5 seconds; triggering batches more frequently may prevent records from backing up and large batches from causing spill.
- C.The trigger interval cannot be modified without modifying the checkpoint directory; to maintain the current stream state, increase the number of shuffle partitions to maximize parallelism.
- D.Use the trigger once option and configure a Databricks job to execute the query every 10 seconds; this ensures all backlogged records are processed with each batch.

---

### Question 198

Which statement describes the default execution mode for Databricks Auto Loader?

- A.Cloud vendor-specific queue storage and notification services are configured to track newly arriving files; new files are incrementally and idempotently loaded into the target Delta Lake table.
- B.New files are identified by listing the input directory; the target table is materialized by directly querying all valid files in the source directory.
- C.Webhooks trigger a Databricks job to run anytime new data arrives in a source directory; new data are automatically merged into target tables using rules inferred from the data.
- D.New files are identified by listing the input directory; new files are incrementally and idempotently loaded into the target Delta Lake table.

---

### Question 199

Which statement describes the correct use of pyspark.sql.functions.broadcast?

- A.It marks a column as having low enough cardinality to properly map distinct values to available partitions, allowing a broadcast join.
- B.It marks a column as small enough to store in memory on all executors, allowing a broadcast join.
- C.It caches a copy of the indicated table on all nodes in the cluster for use in all future queries during the cluster lifetime.
- D.It marks a DataFrame as small enough to store in memory on all executors, allowing a broadcast join.

---

### Question 200

Spill occurs as a result of executing various wide transformations. However, diagnosing spill requires one to proactively look for key indicators.Where in the Spark UI are two of the primary indicators that a partition is spilling to disk?

- A.Stage’s detail screen and Query’s detail screen
- B.Stage’s detail screen and Executor’s log files
- C.Driver’s and Executor’s log files
- D.Executor’s detail screen and Executor’s log files

---

### Question 201

![Question 201 Image 1](examtopics_images/question_201_img_1.png)

An upstream source writes Parquet data as hourly batches to directories named with the current date. A nightly batch job runs the following code to ingest all data from the previous day as indicated by the date variable:Assume that the fields customer_id and order_id serve as a composite key to uniquely identify each order.If the upstream system is known to occasionally produce duplicate entries for a single order hours apart, which statement is correct?

- A.Each write to the orders table will only contain unique records, and only those records without duplicates in the target table will be written.
- B.Each write to the orders table will only contain unique records, but newly written records may have duplicates already present in the target table.
- C.Each write to the orders table will only contain unique records; if existing records with the same key are present in the target table, these records will be overwritten.
- D.Each write to the orders table will run deduplication over the union of new and existing records, ensuring no duplicate records are present.

---

### Question 202

![Question 202 Image 1](examtopics_images/question_202_img_1.png)

A junior data engineer on your team has implemented the following code block.The view new_events contains a batch of records with the same schema as the events Delta table. The event_id field serves as a unique key for this table.When this query is executed, what will happen with new records that have the same event_id as an existing record?

- A.They are merged.
- B.They are ignored.
- C.They are updated.
- D.They are inserted.

---

### Question 203

A new data engineer notices that a critical field was omitted from an application that writes its Kafka source to Delta Lake. This happened even though the critical field was in the Kafka source. That field was further missing from data written to dependent, long-term storage. The retention threshold on the Kafka service is seven days. The pipeline has been in production for three months.Which describes how Delta Lake can help to avoid data loss of this nature in the future?

- A.The Delta log and Structured Streaming checkpoints record the full history of the Kafka producer.
- B.Delta Lake schema evolution can retroactively calculate the correct value for newly added fields, as long as the data was in the original source.
- C.Delta Lake automatically checks that all fields present in the source data are included in the ingestion layer.
- D.Ingesting all raw data and metadata from Kafka to a bronze Delta table creates a permanent, replayable history of the data state.

---

### Question 204

![Question 204 Image 1](examtopics_images/question_204_img_1.png)

The data engineering team maintains the following code:Assuming that this code produces logically correct results and the data in the source table has been de-duplicated and validated, which statement describes what will occur when this code is executed?

- A.The silver_customer_sales table will be overwritten by aggregated values calculated from all records in the gold_customer_lifetime_sales_summary table as a batch job.
- B.A batch job will update the gold_customer_lifetime_sales_summary table, replacing only those rows that have different values than the current version of the table, using customer_id as the primary key.
- C.The gold_customer_lifetime_sales_summary table will be overwritten by aggregated values calculated from all records in the silver_customer_sales table as a batch job.
- D.An incremental job will detect if new rows have been written to the silver_customer_sales table; if new rows are detected, all aggregates will be recalculated and used to overwrite the gold_customer_lifetime_sales_summary table.

---

### Question 205

The data engineering team is migrating an enterprise system with thousands of tables and views into the Lakehouse. They plan to implement the target architecture using a series of bronze, silver, and gold tables. Bronze tables will almost exclusively be used by production data engineering workloads, while silver tables will be used to support both data engineering and machine learning workloads. Gold tables will largely serve business intelligence and reporting purposes. While personal identifying information (PII) exists in all tiers of data, pseudonymization and anonymization rules are in place for all data at the silver and gold levels.The organization is interested in reducing security concerns while maximizing the ability to collaborate across diverse teams.Which statement exemplifies best practices for implementing this system?

- A.Isolating tables in separate databases based on data quality tiers allows for easy permissions management through database ACLs and allows physical separation of default storage locations for managed tables.
- B.Because databases on Databricks are merely a logical construct, choices around database organization do not impact security or discoverability in the Lakehouse.
- C.Storing all production tables in a single database provides a unified view of all data assets available throughout the Lakehouse, simplifying discoverability by granting all users view privileges on this database.
- D.Working in the default Databricks database provides the greatest security when working with managed tables, as these will be created in the DBFS root.

---

### Question 206

The data architect has mandated that all tables in the Lakehouse should be configured as external (also known as "unmanaged") Delta Lake tables.Which approach will ensure that this requirement is met?

- A.When a database is being created, make sure that the LOCATION keyword is used.
- B.When the workspace is being configured, make sure that external cloud object storage has been mounted.
- C.When data is saved to a table, make sure that a full file path is specified alongside the USING DELTA clause.
- D.When tables are created, make sure that the UNMANAGED keyword is used in the CREATE TABLE statement.

---

### Question 207

To reduce storage and compute costs, the data engineering team has been tasked with curating a series of aggregate tables leveraged by business intelligence dashboards, customer-facing applications, production machine learning models, and ad hoc analytical queries.The data engineering team has been made aware of new requirements from a customer-facing application, which is the only downstream workload they manage entirely. As a result, an aggregate table used by numerous teams across the organization will need to have a number of fields renamed, and additional fields will also be added.Which of the solutions addresses the situation while minimally interrupting other teams in the organization without increasing the number of tables that need to be managed?

- A.Send all users notice that the schema for the table will be changing; include in the communication the logic necessary to revert the new table schema to match historic queries.
- B.Configure a new table with all the requisite fields and new names and use this as the source for the customer-facing application; create a view that maintains the original data schema and table name by aliasing select fields from the new table.
- C.Create a new table with the required schema and new fields and use Delta Lake's deep clone functionality to sync up changes committed to one table to the corresponding table.
- D.Replace the current table definition with a logical view defined with the query logic currently writing the aggregate table; create a new table to power the customer-facing application.

---

### Question 208

A Delta Lake table representing metadata about content posts from users has the following schema:user_id LONG, post_text STRING, post_id STRING, longitude FLOAT, latitude FLOAT, post_time TIMESTAMP, date DATEBased on the above schema, which column is a good candidate for partitioning the Delta Table?

- A.post_time
- B.date
- C.post_id
- D.user_id

---

### Question 209

![Question 209 Image 1](examtopics_images/question_209_img_1.png)

The downstream consumers of a Delta Lake table have been complaining about data quality issues impacting performance in their applications. Specifically, they have complained that invalid latitude and longitude values in the activity_details table have been breaking their ability to use other geolocation processes.A junior engineer has written the following code to add CHECK constraints to the Delta Lake table:A senior engineer has confirmed the above logic is correct and the valid ranges for latitude and longitude are provided, but the code fails when executed.Which statement explains the cause of this failure?

- A.The current table schema does not contain the field valid_coordinates; schema evolution will need to be enabled before altering the table to add a constraint.
- B.The activity_details table already exists; CHECK constraints can only be added during initial table creation.
- C.The activity_details table already contains records that violate the constraints; all existing data must pass CHECK constraints in order to add them to an existing table.
- D.The activity_details table already contains records; CHECK constraints can only be added prior to inserting values into a table.

---

### Question 210

What is true for Delta Lake?

- A.Views in the Lakehouse maintain a valid cache of the most recent versions of source tables at all times.
- B.Primary and foreign key constraints can be leveraged to ensure duplicate values are never entered into a dimension table.
- C.Delta Lake automatically collects statistics on the first 32 columns of each table which are leveraged in data skipping based on query filters.
- D.Z-order can only be applied to numeric values stored in Delta Lake tables.

---

### Question 211

![Question 211 Image 1](examtopics_images/question_211_img_1.png)

The view updates represents an incremental batch of all newly ingested data to be inserted or updated in the customers table.The following logic is used to process these records.Which statement describes this implementation?

- A.The customers table is implemented as a Type 2 table; old values are overwritten and new customers are appended.
- B.The customers table is implemented as a Type 2 table; old values are maintained but marked as no longer current and new values are inserted.
- C.The customers table is implemented as a Type 0 table; all writes are append only with no changes to existing values.
- D.The customers table is implemented as a Type 1 table; old values are overwritten by new values and no history is maintained.

---

### Question 212

A team of data engineers are adding tables to a DLT pipeline that contain repetitive expectations for many of the same data quality checks. One member of the team suggests reusing these data quality rules across all tables defined for this pipeline.What approach would allow them to do this?

- A.Add data quality constraints to tables in this pipeline using an external job with access to pipeline configuration files.
- B.Use global Python variables to make expectations visible across DLT notebooks included in the same pipeline.
- C.Maintain data quality rules in a separate Databricks notebook that each DLT notebook or file can import as a library.
- D.Maintain data quality rules in a Delta table outside of this pipeline's target schema, providing the schema name as a pipeline parameter.

---

### Question 213

The DevOps team has configured a production workload as a collection of notebooks scheduled to run daily using the Jobs UI. A new data engineering hire is onboarding to the team and has requested access to one of these notebooks to review the production logic.What are the maximum notebook permissions that can be granted to the user without allowing accidental changes to production code or data?

- A.Can manage
- B.Can edit
- C.Can run
- D.Can read

---

### Question 214

![Question 214 Image 1](examtopics_images/question_214_img_1.png)

A table named user_ltv is being used to create a view that will be used by data analysts on various teams. Users in the workspace are configured into groups, which are used for setting up data access using ACLs.The user_ltv table has the following schema:email STRING, age INT, ltv INTThe following view definition is executed:An analyst who is not a member of the marketing group executes the following query:SELECT * FROM email_ltv -Which statement describes the results returned by this query?

- A.Three columns will be returned, but one column will be named "REDACTED" and contain only null values.
- B.Only the email and ltv columns will be returned; the email column will contain all null values.
- C.The email and ltv columns will be returned with the values in user_ltv.
- D.Only the email and ltv columns will be returned; the email column will contain the string "REDACTED" in each row.

---

### Question 215

![Question 215 Image 1](examtopics_images/question_215_img_1.png)

The data governance team has instituted a requirement that all tables containing Personal Identifiable Information (PII) must be clearly annotated. This includes adding column comments, table comments, and setting the custom table property "contains_pii" = true.The following SQL DDL statement is executed to create a new table:Which command allows manual confirmation that these three requirements have been met?

- A.DESCRIBE EXTENDED dev.pii_test
- B.DESCRIBE DETAIL dev.pii_test
- C.SHOW TBLPROPERTIES dev.pii_test
- D.DESCRIBE HISTORY dev.pii_test

---

### Question 216

![Question 216 Image 1](examtopics_images/question_216_img_1.png)

The data governance team is reviewing code used for deleting records for compliance with GDPR. They note the following logic is used to delete records from the Delta Lake table named users.Assuming that user_id is a unique identifying key and that delete_requests contains all users that have requested deletion, which statement describes whether successfully executing the above logic guarantees that the records to be deleted are no longer accessible and why?

- A.Yes; Delta Lake ACID guarantees provide assurance that the DELETE command succeeded fully and permanently purged these records.
- B.No; files containing deleted records may still be accessible with time travel until a VACUUM command is used to remove invalidated data files.
- C.Yes; the Delta cache immediately updates to reflect the latest data files recorded to disk.
- D.No; the Delta Lake DELETE command only provides ACID guarantees when combined with the MERGE INTO command.

---

### Question 217

The data architect has decided that once data has been ingested from external sources into theDatabricks Lakehouse, table access controls will be leveraged to manage permissions for all production tables and views.The following logic was executed to grant privileges for interactive queries on a production database to the core engineering group.GRANT USAGE ON DATABASE prod TO eng;GRANT SELECT ON DATABASE prod TO eng;Assuming these are the only privileges that have been granted to the eng group and that these users are not workspace administrators, which statement describes their privileges?

- A.Group members are able to create, query, and modify all tables and views in the prod database, but cannot define custom functions.
- B.Group members are able to list all tables in the prod database but are not able to see the results of any queries on those tables.
- C.Group members are able to query and modify all tables and views in the prod database, but cannot create new tables or views.
- D.Group members are able to query all tables and views in the prod database, but cannot create or edit anything in the database.

---

### Question 218

![Question 218 Image 1](examtopics_images/question_218_img_1.png)

A user wants to use DLT expectations to validate that a derived table report contains all records from the source, included in the table validation_copy.The user attempts and fails to accomplish this by adding an expectation to the report table definition.Which approach would allow using DLT expectations to validate all expected records are present in this table?

- A.Define a temporary table that performs a left outer join on validation_copy and report, and define an expectation that no report key values are null
- B.Define a SQL UDF that performs a left outer join on two tables, and check if this returns null values for report key values in a DLT expectation for the report table
- C.Define a view that performs a left outer join on validation_copy and report, and reference this view in DLT expectations for the report table
- D.Define a function that performs a left outer join on validation_copy and report, and check against the result in a DLT expectation for the report table

---

### Question 219

A user new to Databricks is trying to troubleshoot long execution times for some pipeline logic they are working on. Presently, the user is executing code cell-by-cell, using display() calls to confirm code is producing the logically correct results as new transformations are added to an operation. To get a measure of average time to execute, the user is running each cell multiple times interactively.Which of the following adjustments will get a more accurate measure of how code is likely to perform in production?

- A.The Jobs UI should be leveraged to occasionally run the notebook as a job and track execution time during incremental code development because Photon can only be enabled on clusters launched for scheduled jobs.
- B.The only way to meaningfully troubleshoot code execution times in development notebooks is to use production-sized data and production-sized clusters with Run All execution.
- C.Production code development should only be done using an IDE; executing code against a local build of open source Spark and Delta Lake will provide the most accurate benchmarks for how code will perform in production.
- D.Calling display() forces a job to trigger, while many transformations will only add to the logical query plan; because of caching, repeated execution of the same logic does not provide meaningful results.

---

### Question 220

Where in the Spark UI can one diagnose a performance problem induced by not leveraging predicate push-down?

- A.In the Executor’s log file, by grepping for "predicate push-down"
- B.In the Stage’s Detail screen, in the Completed Stages table, by noting the size of data read from the Input column
- C.In the Query Detail screen, by interpreting the Physical Plan
- D.In the Delta Lake transaction log. by noting the column statistics

---

### Question 221

A data engineer needs to capture pipeline settings from an existing setting in the workspace, and use them to create and version a JSON file to create a new pipeline.Which command should the data engineer enter in a web terminal configured with the Databricks CLI?

- A.Use list pipelines to get the specs for all pipelines; get the pipeline spec from the returned results; parse and use this to create a pipeline
- B.Stop the existing pipeline; use the returned settings in a reset command
- C.Use the get command to capture the settings for the existing pipeline; remove the pipeline_id and rename the pipeline; use this in a create command
- D.Use the clone command to create a copy of an existing pipeline; use the get JSON command to get the pipeline definition; save this to git

---

### Question 222

Which Python variable contains a list of directories to be searched when trying to locate required modules?

- A.importlib.resource_path
- B.sys.path
- C.os.path
- D.pypi.path

---

### Question 223

None


---

### Question 224

None


---

### Question 225

Which REST API call can be used to review the notebooks configured to run as tasks in a multi-task job?

- A./jobs/runs/list
- B./jobs/list
- C./jobs/runs/get
- D./jobs/get

---

### Question 226

A Data Engineer wants to run unit tests using common Python testing frameworks on Python functions defined across several Databricks notebooks currently used in production.How can the data engineer run unit tests against functions that work with data in production?

- A.Define and import unit test functions from a separate Databricks notebook
- B.Define and unit test functions using Files in Repos
- C.Run unit tests against non-production data that closely mirrors production
- D.Define unit tests and functions within the same notebook

---

### Question 227

![Question 227 Image 1](examtopics_images/question_227_img_1.png)

![Question 227 Image 2](examtopics_images/question_227_img_2.png)

A data engineer wants to refactor the following DLT code, which includes multiple table definitions with very similar code.In an attempt to programmatically create these tables using a parameterized table definition, the data engineer writes the following code.The pipeline runs an update with this refactored code, but generates a different DAG showing incorrect configuration values for these tables.How can the data engineer fix this?

- A.Wrap the for loop inside another table definition, using generalized names and properties to replace with those from the inner table definition.
- B.Convert the list of configuration values to a dictionary of table settings, using table names as keys.
- C.Move the table definition into a separate function, and make calls to this function using different input parameters inside the for loop.
- D.Load the configuration values for these tables from a separate file, located at a path provided by a pipeline parameter.

---

### Question 228

A data engineer has created a 'transactions' Delta table on Databricks that should be used by the analytics team. The analytics team wants to use the table with another tool which requires Apache Iceberg format.What should the data engineer do?

- A.Require the analytics team to use a tool which supports Delta table.
- B.Create an Iceberg copy of the 'transactions' Delta table which can be used by the analytics team.
- C.Convert the 'transactions' Delta to Iceberg and enable uniform so that the table can be read as a Delta table.
- D.Enable uniform on the transactions table to 'iceberg' so that the table can be read as an Iceberg table.

---

### Question 229

A junior data engineer is working to implement logic for a Lakehouse table named silver_device_recordings. The source data contains 100 unique fields in a highly nested JSON structure.The silver_device_recordings table will be used downstream for highly selective joins on a number of fields, and will also be leveraged by the machine learning team to filter on a handful of relevant fields. In total, 15 fields have been identified that will often be used for filter and join logic.The data engineer is trying to determine the best approach for dealing with these nested fields before declaring the table schema.Which of the following accurately presents information about Delta Lake and Databricks that may impact their decision-making process?

- A.Because Delta Lake uses Parquet for data storage, Dremel encoding information for nesting can be directly referenced by the Delta transaction log.
- B.Schema inference and evolution on Databricks ensure that inferred types will always accurately match the data types used by downstream systems.
- C.The Tungsten encoding used by Databricks is optimized for storing string data; newly-added native support for querying JSON strings means that string types are always most efficient.
- D.By default, Delta Lake collects statistics on the first 32 columns in a table; these statistics are leveraged for data skipping when executing selective queries.

---

### Question 230

A platform engineer is creating catalogs and schemas for the development team to use.The engineer has created an initial catalog, Catalog_A, and initial schema, Schema_A. The engineer has also granted USE CATALOG, USE SCHEMA, and CREATE TABLE to the development team so that the engineer can begin populating the schema with new tables.Despite being owner of the catalog and schema, the engineer noticed that they do not have access to the underlying tables in Schema_A.What explains the engineer's lack of access to the underlying tables?

- A.The owner of the schema does not automatically have permission to tables within the schema, but can grant them to themselves at any point.
- B.Users granted with USE CATALOG can modify the owner's permissions to downstream tables.
- C.Permissions explicitly given by the table creator are the only way the Platform Engineer could access the underlying tables in their schema.
- D.The platform engineer needs to execute a REFRESH statement as the table permissions did not automatically update for owners.

---

### Question 231

A data engineer has created a new cluster using shared access mode with default configurations. The data engineer needs to allow the development team access to view the driver logs if needed.What are the minimal cluster permissions that allow the development team to accomplish this?

- A.CAN VIEW
- B.CAN RESTART
- C.CAN ATTACH TO
- D.CAN MANAGE

---

### Question 232

A data engineer wants to create a cluster using the Databricks CLI for a big ETL pipeline. The cluster should have five workers and one driver of type i3.xlarge and should use the '14.3.x-scala2.12' runtime.Which command should the data engineer use?

- A.databricks compute add 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster
- B.databricks clusters create 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster
- C.databricks compute create 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster
- D.databricks clusters add 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster

---

### Question 233

A 'transactions' table has been liquid clustered on the columns 'product_id’, ’user_id' and 'event_date'.Which operation lacks support for cluster on write?

- A.CTAS and RTAS statements
- B.spark.writeStream.format(’delta').mode(’append’)
- C.spark.write.format('delta’).mode('append')
- D.INSERT INTO operations

---

### Question 234

![Question 234 Image 1](examtopics_images/question_234_img_1.png)

The data governance team has instituted a requirement that the "user" table containing Personal Identifiable Information (PII) must have the appropriate masking on the SSN column. This means that anyone outside of the HRAdminGroup should see masked social security numbers as ***-**-****.The team created a masking function:What does the data governance team need to do next to achieve this goal?

- A.CREATE TABLE users -(name STRING);ALTER TABLE users CREATE COLUMN ssn CREATE MASK ssn_mask;
- B.CREATE TABLE users -(name STRING, int STRING);ALTER TABLE users ALTER COLUMN ssn CREATE MASK if is_member('HRAdminGroup');
- C.CREATE TABLE users -(name STRING, ssn INT MASKED ssn_mask);
- D.CREATE TABLE users -(name STRING, ssn STRING);ALTER TABLE users ALTER COLUMN ssn SET MASK ssn_mask;

---

### Question 235

A data engineer needs to create an application that will collect information about the latest job run including the repair history.How should the data engineer format the request?

- A.Call/api/2.1/jobs/runs/list with the run_id and include_history parameters
- B.Call/api/2.1/jobs/runs/get with the run_id and include_history parameters
- C.Call/api/2.1/jobs/runs/get with the job_id and include_history parameters
- D.Call/api/2.1/jobs/runs/list with the job_id and include_history parameters

---

### Question 236

A data engineer is working in an interactive notebook with many transformations before outputting the result from display(df.collect() ). The notebook includes wide transformations and a cross join.The data engineer is getting the following error: "The spark driver has stopped unexpectedly and is restarting. Your notebook will be automatically reattached."Which action should the data engineer take?

- A.Run the notebook on a single node cluster to keep driver from falling.
- B.Rewrite their code to avoid putting memory pressure on the driver node.
- C.Check into the Spark UI to see how many jobs are assigned to each stage as they are employing fewer executors.
- D.Look at the compute metrics UI to see if the executors have higher than 90% memory utilization.

---

### Question 237

An analytics team wants run an experiment in the short term on the customer transaction Delta table (with 20 billions records) created by the data engineering team in Databricks SQL.Which strategy should the data engineering team use to ensure minimal downtime and no impact on the ongoing ETL processes?

- A.Deep clone the table for the analytics team.
- B.Create a new table for the analytics team using a CTAS statement.
- C.Shallow clone the table for the analytics team.
- D.Give access to the table for the analytics team.

---

### Question 238

A data team is working to optimize an existing large, fast-growing table 'orders' with high cardinality columns, which experiences significant data skew and requires frequent concurrent writes. The team notice that the columns 'user_id', 'event_timestamp' and 'product_id' are heavily used in analytical queries and filters, although those keys may be subject to change in the future due to different business requirements.Which partitioning strategy should the team choose to optimize the table for immediate data skipping, incremental management over time, and flexibility?

- A.Partition the table with: ALTER TABLE orders PARTITION BY user_id, product_id, event_timestamp
- B.Use z-order after partitiing the table: OPTIMIZE orders ZORDER BY (user_id, product_id) WHERE event_timestamp = current date () - 1 DAY
- C.Cluster the table with: ALTER TABLE orders CLUSTER BY user_id, product_id, event_timestamp
- D.Z-order the table with OPTIMIZE orders ZORDER BY (user_id, product_id, event_timestamp)

---

