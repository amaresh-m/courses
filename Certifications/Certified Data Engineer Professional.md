
# Practice Exam Questions: Databricks Certified Data Engineer Professional 


- [Practice Exam Questions: Databricks Certified Data Engineer Professional](#practice-exam-questions-databricks-certified-data-engineer-professional)
  - [Question 1:](#question-1)
  - [Question 2:](#question-2)




## Question 1:
An hourly batch job is configured to ingest data files from a cloud object storage container where each batch represent all records produced by the source system in a given hour. The batch job to process these records into the Lakehouse is sufficiently delayed to ensure no late-arriving data is missed. The user_id field represents a unique key for the data, which has the following schema: user_id BIGINT, username STRING, user_utc STRING, user_region STRING, last_login BIGINT, auto_pay BOOLEAN, last_updated BIGINT
New records are all ingested into a table named account_history which maintains a full record of all data in the same schema as the source. The next table in the system is named account_current and is implemented as a Type 1 table representing the most recent value for each unique user_id.
Assuming there are millions of user accounts and tens of thousands of records processed hourly, which implementation can be used to efficiently update the described account_current table as part of each hourly batch job?

A. Use Auto Loader to subscribe to new files in the account_history directory; configure a Structured Streaming trigger once job to batch update newly detected files into the account_current table.
B. Overwrite the account_current table with each batch using the results of a query against the account_history table grouping by user_id and filtering for the max value of last_updated.
C. Filter records in account_history using the last_updated field and the most recent hour processed, as well as the max last_iogin by user_id write a merge statement to update or insert the most recent value for each user_id.
D. Use Delta Lake version history to get the difference between the latest version of account_history and one version prior, then write these records to account_current.
E. Filter records in account_history using the last_updated field and the most recent hour processed, making sure to deduplicate on username; write a merge statement to update or insert the most recent value for each username.


**Topic Mapping**

This question falls under the following sections of the November 30, 2025 (2026 exam) guide:

-   **Domain 3: Data Transformation, Cleansing, and Quality (10%)**
    -   Sub-topic: Designing and implementing  **Medallion Architecture**  and  **Slowly Changing Dimensions (SCD)**.
-   **Domain 1: Developing Code for Data Processing (22%)**
    -   Sub-topic: Using  **MERGE**  and  **Delta Lake APIs**  for complex data handling.

----------

**Important Keywords to Focus On**

-   **"Type 1 table"**: This is a critical indicator. SCD Type 1 means you only store the  **most recent**  data, overwriting existing records without maintaining historical versions.
-   **"Efficiently update"**: With  **millions of records**  in the target and only  **tens of thousands**  incoming, you need an incremental approach (MERGE) rather than a full overwrite.
-   **"user_id ... unique key"**: This identifies the primary key to use for matching records.
-   **"last_updated ... max value"**: This defines the logic to ensure only the latest state from the batch is applied.
-   **"Hourly batch job"**: The solution must fit a periodic scheduled window.

----------

**Correct Option: C**

**Explanation:**  
Option C describes the standard, most efficient way to implement an SCD Type 1 update in a Delta Lake environment.

1.  **Filtering & Incremental Processing**: By filtering  `account_history`  for only the most recent hour (the incremental batch), you avoid reading millions of historical records unnecessarily.
2.  **Deduplication**: Using the  `max`  of  `last_updated`  (or  `last_login`) for each  `user_id`  within that batch ensures that if multiple updates occurred for the same user during that hour, only the absolute latest version is staged for the update.
3.  **MERGE Statement**: The  `MERGE INTO`  command is the optimized tool for "upserts" in Delta Lake. It checks if the  `user_id`  exists in the  `account_current`  table; if it does, it  **updates**  the record in-place (SCD Type 1 logic); if not, it  **inserts**  a new record.

----------

**Why Other Options are Incorrect**

-   **A. Use Auto Loader/Streaming**: While Auto Loader is great for ingestion, simply triggering a streaming job to "batch update" doesn't inherently handle the complex deduplication or Type 1 merge logic required for the existing millions of records in a structured way.
-   **B. Overwrite the table**: Overwriting  `account_current`  with every hourly batch is extremely  **inefficient**. As the table grows to millions of users, reading the entire history and rewriting the whole table every hour would consume excessive compute resources and time.
-   **D. Delta Lake version history**: This approach (using  `VERSION AS OF`) is used for auditing or time travel, but it is not a standard or efficient way to identify changed records for an SCD Type 1 merge, especially when records are already organized in a history table.
-   **E. Deduplicate on username**: This is logically incorrect. The question explicitly states that  **`user_id`**  is the unique key. Deduplicating on  `username`  could lead to data loss or corruption if multiple users shared a name or if a user changed their name.
  

---


## Question 2:
A Structured Streaming job deployed to production has been experiencing delays during peak hours of the day. At present, during normal execution, each microbatch of data is processed in less than 3 seconds. During peak hours of the day, execution time for each microbatch becomes very inconsistent, sometimes exceeding 30 seconds. The streaming write is currently configured with a trigger interval of 10 seconds.
Holding all other variables constant and assuming records need to be processed in less than 10 seconds, which adjustment will meet the requirement?

A. Decrease the trigger interval to 5 seconds; triggering batches more frequently allows idle executors to begin processing the next batch while longer running tasks from previous batches finish.
B. Increase the trigger interval to 30 seconds; setting the trigger interval near the maximum execution time observed for each batch is always best practice to ensure no records are dropped.
C. The trigger interval cannot be modified without modifying the checkpoint directory; to maintain the current stream state, increase the number of shuffle partitions to maximize parallelism.
D. Use the trigger once option and configure a Databricks job to execute the query every 10 seconds; this ensures all backlogged records are processed with each batch.
E. Decrease the trigger interval to 5 seconds; triggering batches more frequently may prevent records from backing up and large batches from causing spill.



**Topic Mapping**

This question aligns with the November 30, 2025 (2026 exam) guide under:

-   **Domain 6: Cost & Performance Optimization (13%)**
    -   Sub-topic: Tuning streaming applications and managing  **micro-batch sizing**.
-   **Domain 9: Debugging and Deploying (10%)**
    -   Sub-topic: Identifying and resolving performance bottlenecks in production pipelines.

----------

**Important Keywords to Focus On**

-   **"Peak hours ... delays"**: The issue is load-dependent, suggesting that too many records are accumulating between triggers.
-   **"Trigger interval of 10 seconds"**: This is the current fixed wait time between starting micro-batches.
-   **"Execution time ... inconsistent ... exceeding 30 seconds"**: This indicates that when the 10-second wait is over, the resulting batch is so large that it takes 30 seconds to process, causing a backlog.
-   **"Assuming records need to be processed in less than 10 seconds"**: This is the strict requirement that the final solution must meet.

----------

**Correct Option: E**

**Explanation:**  
Option E addresses the root cause of the performance degradation during peak hours.

1.  **Reducing Batch Size**: By decreasing the trigger interval from 10 seconds to 5 seconds, the engine "wakes up" more frequently to pull data. This results in  **fewer records per micro-batch**.
2.  **Preventing Resource Exhaustion**: Smaller batches are less likely to cause  **spill to disk**  (which happens when data exceeds the executor's memory capacity). Spill is a primary reason for the "very inconsistent" execution times mentioned in the scenario.
3.  **Meeting Latency Requirements**: Processing smaller batches more often allows the system to stay under the 10-second threshold by ensuring the compute work for any single batch remains small and manageable.

----------

**Why Other Options are Incorrect**

-   **A. Concurrent Processing Misconception**: Structured Streaming (in micro-batch mode)  **cannot**  begin a new batch while the previous one is still finishing. It processes batches sequentially. Decreasing the interval doesn't let "idle executors" work on a future batch while a current one is running.
-   **B. Increasing Interval**: Increasing the interval to 30 seconds would actually  **worsen**  the problem. It would allow even more data to accumulate, creating massive batches that would likely take even longer than 30 seconds to process and increase the risk of memory errors.
-   **C. Checkpoint Misconception**: You  **can**  change the trigger interval without deleting or modifying the checkpoint directory. While increasing shuffle partitions can help parallelism, it does not address the fundamental issue of batch size accumulation described.
-   **D. Trigger Once at 10s**: Using "Trigger Once" (or the modern  `AvailableNow`) every 10 seconds via a scheduled job introduces significant  **overhead**  from job startup and cluster initialization. It is less efficient than a continuous stream and does not solve the batch-size-to-memory ratio problem as effectively as a tighter streaming trigger.

---

## Question 3:

Which statement describes Delta Lake optimized writes?

A. Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.
B. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.
C. A shuffle occurs prior to writing to try to group similar data together resulting in fewer files instead of each executor writing multiple files based on directory partitions.
D. Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.


**Topic Mapping**

This question aligns with the November 30, 2025 (2026 exam) guide under:

-   **Domain 6: Cost & Performance Optimization (13%)**
    -   Sub-topic: Understanding and configuring Delta Lake features for performance, including  `OPTIMIZE`, Z-Ordering, and file management.

----------

**Important Keywords to Focus On**

-   **"Delta Lake optimized writes"**: Refers to a specific configuration feature in  [Databricks](https://docs.databricks.com/aws/en/delta/)  that affects how data is written during the initial write operation, aiming to reduce the small file problem inherent in distributed systems.
-   **"fewer files"**: The core goal of optimized writes is to improve file sizing during the initial write stage, contrasting with the default behavior where many small files might be generated.

----------

**Correct Option: C**

**Explanation:**  
Option C accurately describes the behavior of "optimized writes" in  Databricks Delta Lake.

-   **Default behavior**: Without optimized writes, each Spark executor might write multiple small files to different directory partitions, leading to the "small file problem" which harms read performance.
-   **Optimized writes behavior**: When enabled, the feature ensures that before the data is written, a shuffle occurs to redistribute data such that each executor writes significantly  **fewer, larger files**  to the output locations. This results in an initial write that is already closer to an optimal file size distribution, reducing the need for immediate post-write  `OPTIMIZE`  commands.

----------

**Why Other Options are Incorrect**

-   **A. OPTIMIZE on termination**: This is not the mechanism for "optimized writes".  `OPTIMIZE`  is an explicit command run after a write or via a scheduled job. The optimized writes feature impacts the write operation itself.
-   **B. Asynchronous post-write job**: This describes the functionality of  **Delta Live Tables (DLT) Enhanced Autoscaling**  or perhaps auto-optimize, but the core "optimized writes" feature is a synchronous part of the write command.
-   **D. Logical vs. Directory partitions**: Delta Lake still uses directory partitioning (physical partitioning) for physical data layout when a partition schema is defined. The optimized writes feature improves file sizing within those physical partitions; it does not replace physical partitioning with purely logical metadata boundaries.

---

## Question 4:

The downstream consumers of a Delta Lake table have been complaining about data quality issues impacting performance in their applications. Specifically, they have complained that invalid latitude and longitude values in the activity_details table have been breaking their ability to use other geolocation processes.
A junior engineer has written the following code to add CHECK constraints to the Delta Lake table:

A senior engineer has confirmed the above logic is correct and the valid ranges for latitude and longitude are provided, but the code fails when executed.
Which statement explains the cause of this failure?

A. Because another team uses this table to support a frequently running application, two-phase locking is preventing the operation from committing.
B. The activity_details table already exists; CHECK constraints can only be added during initial table creation.
C. The activity_details table already contains records that violate the constraints; all existing data must pass CHECK constraints in order to add them to an existing table.
D. The activity_details table already contains records; CHECK constraints can only be added prior to inserting values into a table.
E. The current table schema does not contain the field valid_coordinates; schema evolution will need to be enabled before altering the table to add a constraint.


**Topic Mapping**

This question aligns with the November 30, 2025 (2026 exam) guide under:

-   **Domain 3: Data Transformation, Cleansing, and Quality (10%)**
    -   Sub-topic: Implementing  **Data Quality rules**  and constraints (e.g., Delta Lake constraints, Expectations).
-   **Domain 7: Ensuring Data Security and Compliance (10%)**
    -   Sub-topic: Managing table properties and metadata.

----------

**Important Keywords to Focus On**

-   **"Downstream consumers ... complaining about data quality"**: Indicates that bad data is  **already present**  in the table.
-   **"fails when executed"**: The  `ALTER TABLE ... ADD CONSTRAINT`  command is syntactically correct but cannot be applied.
-   **"valid ranges"**: The logic of the constraint is mathematically sound.
-   **"existing data must pass"**: This is the fundamental rule for applying schema-level constraints to existing tables.

----------

**Correct Option: C**

**Explanation:**  
In Delta Lake, when you add a  `CHECK`  constraint to an existing table, Databricks immediately performs a  **validation scan**  of the existing data.

1.  **Validation Requirement**: A constraint cannot be successfully added if any row currently in the table violates the new rule.
2.  **Scenario Context**: The prompt explicitly states consumers are complaining about "invalid latitude and longitude values," confirming that the table contains records that fall outside the valid ranges.
3.  **Resolution**: To fix this, the engineer would first need to delete or update the invalid records to be within the valid ranges before re-running the  `ADD CONSTRAINT`  command.

----------

**Why Other Options are Incorrect**

-   **A. Two-phase locking**: Delta Lake uses  **Optimistic Concurrency Control (OCC)**, not two-phase locking. While conflicts can happen, they typically result in a concurrent modification error, not a failure to add a metadata constraint due to "frequent use."
-   **B. Only during creation**: This is false. Delta Lake is flexible; you can add constraints to existing tables at any time using  `ALTER TABLE`.
-   **D. Only prior to inserting values**: This is similar to B and is also false. You can add constraints to a table that has data, provided that the existing data happens to comply with the constraint logic.
-   **E. Schema evolution**: This is a red herring.  `valid_coordinates`  is not a field; it is the  **name of the constraint**  being created. The fields involved are latitude and longitude, which already exist in the table.
  ---


