# Databricks Certified Data Engineer Professional - Practice Questions with Detailed Explanations

---

## Question 1: Efficient Type 1 Dimension Table Updates

An hourly batch job is configured to ingest data files from a cloud object storage container where each batch represent all records produced by the source system in a given hour. The batch job to process these records into the Lakehouse is sufficiently delayed to ensure no late-arriving data is missed. The user_id field represents a unique key for the data, which has the following schema: 

**user_id BIGINT, username STRING, user_utc STRING, user_region STRING, last_login BIGINT, auto_pay BOOLEAN, last_updated BIGINT**

New records are all ingested into a table named account_history which maintains a full record of all data in the same schema as the source. The next table in the system is named account_current and is implemented as a Type 1 table representing the most recent value for each unique user_id.

Assuming there are millions of user accounts and tens of thousands of records processed hourly, which implementation can be used to efficiently update the described account_current table as part of each hourly batch job?

**A.** Use Auto Loader to subscribe to new files in the account_history directory; configure a Structured Streaming trigger once job to batch update newly detected files into the account_current table.

**B.** Overwrite the account_current table with each batch using the results of a query against the account_history table grouping by user_id and filtering for the max value of last_updated.

**C.** Filter records in account_history using the last_updated field and the most recent hour processed, as well as the max last_login by user_id; write a merge statement to update or insert the most recent value for each user_id.

**D.** Use Delta Lake version history to get the difference between the latest version of account_history and one version prior, then write these records to account_current.

**E.** Filter records in account_history using the last_updated field and the most recent hour processed, making sure to deduplicate on username; write a merge statement to update or insert the most recent value for each username.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 1: Developing Code for Data Processing using Python and SQL**
  - Sub-topic: Build and manage reliable, production-ready data pipelines for batch and streaming data using Lakeflow Spark Declarative Pipelines and Autoloader.
  - Sub-topic: Compare Spark Structured Streaming and Lakeflow Spark Declarative Pipelines to determine the optimal approach for building scalable ETL pipelines.

- **Section 3: Data Transformation, Cleansing, and Quality**
  - Sub-topic: Write efficient Spark SQL and PySpark code to apply advanced data transformations, including window functions, joins, and aggregations.

- **Section 10: Data Modelling**
  - Sub-topic: Design and implement scalable data models using Delta Lake to manage large datasets.

---

### Important Keywords to Focus On

- **"Type 1 table"**: Represents only the most recent value for each unique key (Slowly Changing Dimension Type 1).
- **"millions of user accounts and tens of thousands of records hourly"**: Performance and efficiency are critical; full table rewrites would be inefficient.
- **"most recent value for each unique user_id"**: The solution must handle deduplication by user_id and select the latest record based on timestamp.
- **"last_updated field"**: This is the primary timestamp column to compare for determining recency within the batch.
- **"efficiently update"**: Indicates the solution should minimize unnecessary data movement and processing overhead.

---

### Correct Option: C

**Explanation:**

In Delta Lake, updating a Type 1 dimension table efficiently requires:

1. **Incremental Processing**: Rather than reprocessing the entire account_history table (as option B does), filter for only records from the most recent hour using the last_updated field. This reduces the data volume being processed from millions of records to tens of thousands.

2. **Deduplication by Primary Key**: For each unique user_id, select only the record with the maximum last_login value within the batch window. This ensures only the most recent state is captured and de-duplicates any records that may have multiple updates within the same hour.

3. **MERGE Statement**: The MERGE INTO command is the optimal Databricks pattern for updating dimension tables because it:
   - Supports both INSERT and UPDATE operations in a single ACID transaction
   - Only processes and transfers changed records, not the entire table
   - Efficiently matches on the user_id key to determine insert vs. update behavior
   - Provides atomicity, ensuring consistency even if the job is interrupted

4. **Efficiency**: This approach processes only the new hourly batch of records, making it far more efficient than rewriting the entire account_current table. With millions of accounts but only tens of thousands of new/updated records per hour, this incremental approach provides optimal performance and cost efficiency.

---

### Why Other Options are Incorrect

- **A. Auto Loader with Structured Streaming trigger once**: While Auto Loader and Structured Streaming are valid patterns, using "trigger once" with Auto Loader adds unnecessary complexity for a batch job that runs hourly. Option C is simpler, more direct, and more cost-effective for scheduled batch hourly jobs. Additionally, Structured Streaming has checkpoint overhead that's unnecessary for simple batch jobs.

- **B. Overwrite the entire account_current table**: This is inefficient because it requires:
  - Reading and processing ALL millions of records in account_history every hour
  - Rewriting the entire account_current table even if only thousands of records changed
  - Higher compute cost and longer execution time
  - No built-in optimization from Delta's MERGE operation

- **D. Delta Lake version history**: While Delta Lake does track version history, using it to compute differences between versions is not the recommended pattern for dimension table updates. This approach also doesn't explicitly handle user_id deduplication and requires additional manual logic to parse version differences.

- **E. Deduplicating on username instead of user_id**: This is incorrect because:
  - The primary key is user_id, not username
  - Usernames may not be unique or may change over time
  - The requirement explicitly states user_id is the unique identifier
  - This would cause incorrect merge logic and potential data inconsistencies with duplicate or missing records

---

---

## Question 2: Structured Streaming Performance During Peak Hours

A Structured Streaming job deployed to production has been experiencing delays during peak hours of the day. At present, during normal execution, each microbatch of data is processed in less than 3 seconds. During peak hours of the day, execution time for each microbatch becomes very inconsistent, sometimes exceeding 30 seconds. The streaming write is currently configured with a trigger interval of 10 seconds.

Holding all other variables constant and assuming records need to be processed in less than 10 seconds, which adjustment will meet the requirement?

**A.** Decrease the trigger interval to 5 seconds; triggering batches more frequently allows idle executors to begin processing the next batch while longer-running tasks from previous batches finish.

**B.** Increase the trigger interval to 30 seconds; setting the trigger interval near the maximum execution time observed for each batch is always best practice to ensure no records are dropped.

**C.** The trigger interval cannot be modified without modifying the checkpoint directory; to maintain the current stream state, increase the number of shuffle partitions to maximize parallelism.

**D.** Use the trigger once option and configure a Databricks job to execute the query every 10 seconds; this ensures all backlogged records are processed with each batch.

**E.** Decrease the trigger interval to 5 seconds; triggering batches more frequently may prevent records from backing up and large batches from causing spill.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 1: Developing Code for Data Processing using Python and SQL**
  - Sub-topic: Compare Spark Structured Streaming and Lakeflow Spark Declarative Pipelines to determine the optimal approach for building scalable ETL pipelines.
  - Sub-topic: Build and manage reliable, production-ready data pipelines for batch and streaming data using Lakeflow Spark Declarative Pipelines and Autoloader.

- **Section 6: Cost & Performance Optimisation**
  - Sub-topic: Understand the optimization techniques used by Databricks to ensure the performance of queries on large datasets.

---

### Important Keywords to Focus On

- **"delays during peak hours"**: Microbatches are exceeding the 10-second trigger window, causing backlog and queuing.
- **"execution time for each microbatch becomes very inconsistent, sometimes exceeding 30 seconds"**: Peak processing times are 3x the current trigger interval, indicating resource saturation.
- **"assuming records need to be processed in less than 10 seconds"**: This is the strict SLA requirement that must be met.
- **"trigger interval of 10 seconds"**: When microbatch processing takes > 10 seconds, new records queue up in the source, creating a cascading slowdown effect.
- **"Holding all other variables constant"**: Only the trigger interval configuration can be modified; adding resources or changing infrastructure is not an option.

---

### Correct Option: E

**Explanation:**

The core issue is that during peak hours, microbatch processing times (up to 30 seconds) exceed the trigger interval (10 seconds). This creates a backlog cascade:

1. **The Problem with Current Configuration**: 
   - Trigger fires every 10 seconds, but processing takes 30 seconds
   - Records accumulate in the source buffer, waiting for the previous batch to complete
   - As batches grow larger due to accumulation, they take even longer to process
   - This cascading effect grows exponentially, violating the SLA

2. **Why Decreasing Trigger Interval to 5 Seconds Works**:
   - The streaming engine initiates microbatches more frequently (every 5 seconds instead of 10)
   - While one microbatch is processing, the streaming engine can queue the next smaller microbatch separately
   - Smaller, more frequent batches reduce per-batch processing time
   - Less data in each batch means less memory pressure and reduced likelihood of data spill

3. **Prevention of Data Spill (Key Insight)**:
   - Data spill occurs when the executor's memory is exhausted and data is written to disk
   - Spilled data causes massive performance degradation (disk I/O is orders of magnitude slower than memory)
   - Smaller batches keep memory usage bounded and prevent spill situations
   - Option E correctly identifies both mechanisms: "prevent records from backing up and large batches from causing spill"

4. **Why This Meets the SLA**: With 5-second triggers:
   - The system maintains better control over batch sizes
   - Overlapping batch processing (one batch processing while the next is queued) improves throughput
   - While individual batches might still take 6 batches' worth of time, the parallelism prevents backlog accumulation
   - Processing delays become bounded and predictable

---

### Why Other Options are Incorrect

- **A. Decreasing to 5 seconds (but incorrect reasoning)**: While the conclusion is correct, the reasoning in option A is incomplete and technically inaccurate. It mentions "idle executors" being able to start the next batch, which is not the primary mechanism at play. Option E provides the more complete and technically accurate explanation focusing on data accumulation and spill prevention.

- **B. Increase to 30 seconds**: This is the opposite of what's needed and would catastrophically worsen the problem:
  - Records would queue up even more between trigger events (20 seconds of accumulation per cycle)
  - Larger batches would form, taking even longer to process
  - The cascading effect would accelerate, violating the SLA requirement of < 10 seconds
  - This is fundamentally misunderstanding streaming backpressure

- **C. Cannot modify trigger interval without modifying checkpoint directory**: This is false. Trigger intervals are a runtime configuration parameter that can be modified independently from the checkpoint. The checkpoint directory stores state metadata, not trigger configuration. Additionally, changing shuffle partitions doesn't address the fundamental problem of batch accumulation.

- **D. Use trigger once with a Databricks job every 10 seconds**: This is similar to the current configuration (10-second intervals) and would not improve performance. "Trigger once" means the query runs until it catches up to the source entirely, which could take arbitrarily long if there's a backlog. This is impractical for streaming jobs needing consistent low-latency processing.

---

---

## Question 3: Delta Lake Data Filtering with Partitioned Tables

A Delta Lake table representing metadata about content posts from users has the following schema:

**user_id LONG, post_text STRING, post_id STRING, longitude FLOAT, latitude FLOAT, post_time TIMESTAMP, date DATE**

This table is partitioned by the date column. A query is run with the following filter:

**longitude < 20 & longitude > -20**

Which statement describes how data will be filtered?

**A.** Statistics in the Delta Log will be used to identify partitions that might include files in the filtered range.

**B.** No file skipping will occur because the optimizer does not know the relationship between the partition column and the longitude.

**C.** The Delta Engine will scan the parquet file footers to identify each row that meets the filter criteria.

**D.** Statistics in the Delta Log will be used to identify data files that might include records in the filtered range.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 6: Cost & Performance Optimisation**
  - Sub-topic: Understand the optimization techniques used by Databricks to ensure the performance of queries on large datasets (data skipping, file pruning, etc.).

- **Section 10: Data Modelling**
  - Sub-topic: Design and implement scalable data models using Delta Lake to manage large datasets.
  - Sub-topic: Simplify data layout decisions and optimize query performance using Liquid Clustering.

---

### Important Keywords to Focus On

- **"partitioned by the date column"**: The table uses date-based partitioning, NOT longitude-based partitioning.
- **"longitude < 20 & longitude > -20"**: The filter is on a non-partitioned column, testing data skipping capabilities.
- **"data skipping"** and **"file pruning"**: These are Delta Lake optimization features that use statistics stored in the Delta Log at the file level.
- **"Delta Log statistics"**: Min/max values and null count metadata for each column in each data file (Parquet format).
- **"parquet file footers"**: Row-group level statistics contained within Parquet files themselves (different from Delta Log file-level statistics).

---

### Correct Option: D

**Explanation:**

Delta Lake's **data skipping** feature works independently of table partitioning by leveraging column-level statistics stored in the Delta Log:

1. **Delta Log File-Level Statistics**:
   - For every column in every Parquet data file, Delta Lake stores min, max, and null count statistics in the transaction log
   - These statistics are stored at the **file level** in the Delta Log, separate from partition metadata
   - The statistics are computed during the write operation and persist in the `_delta_log` directory

2. **File-Level Filtering with Data Skipping**:
   - When the query filter is `longitude < 20 & longitude > -20`, the optimizer performs this algorithm:
     - Read the min/max statistics for the longitude column from the Delta Log
     - For each data file, extract the range [min_longitude, max_longitude]
     - Check if this range overlaps with the filter range [-20, 20]
     - If a file's longitude range is entirely outside [-20, 20] (e.g., min=25, max=180), that file is skipped entirely
     - If a file's range overlaps with [-20, 20] (e.g., min=-30, max=50), the file must be scanned

3. **Row-Level Filtering**:
   - Once a file is determined to be relevant, Spark applies the filter to individual rows within that file
   - Parquet file footers contain row-group statistics that enable further fine-grained pruning within the file

4. **Why This Works Regardless of Partitioning**: 
   - Data skipping is orthogonal to the partitioning strategy
   - Data skipping works on ANY column that has statistics in the Delta Log
   - The longitude column has min/max statistics recorded regardless of whether the table is partitioned by date
   - Partitioning is a coarse-grained optimization; data skipping is a file-level optimization that operates independently

5. **Key Distinction**:
   - Option A refers to "partitions," but data skipping operates at the **file level**, not partition level
   - Partitions are logical groupings based on the partition column (date); files are physical data objects
   - Data skipping provides more granular optimization within and across partitions

---

### Why Other Options are Incorrect

- **A. Statistics in the Delta Log will be used to identify partitions**: This uses incorrect terminology and demonstrates a misunderstanding of the optimization mechanism. The Delta Log statistics are used to identify **files**, not partitions. Partitions are determined by the partition column (date). The key optimization is **file skipping** within partitions, which is more granular and efficient than partition-level pruning.

- **B. No file skipping will occur because the optimizer does not know the relationship between the partition column and the longitude**: This is a fundamental misunderstanding of how data skipping works. The optimizer doesn't need to know a relationship between the partition column and the filter column. Data skipping is an independent file-level optimization that applies to ANY column with recorded statistics in the Delta Log. The fact that the table is partitioned by date does not prevent data skipping on the longitude column.

- **C. The Delta Engine will scan the parquet file footers to identify each row that meets the filter criteria**: While Parquet file footers DO contain row-group statistics, this option is incorrect because:
  - It says "identify each row," implying full row-level scanning, which contradicts the optimization principle
  - The primary filtering decision is made using Delta Log statistics (file-level decision) BEFORE opening Parquet files
  - Parquet footers are scanned only AFTER a file is determined to be relevant
  - This option misses the point that Delta Log statistics enable **skipping entire files**, providing the biggest performance benefit

---

---

## Question 4: CHECK Constraints on Existing Delta Tables

The downstream consumers of a Delta Lake table have been complaining about data quality issues impacting performance in their applications. Specifically, they have complained that invalid latitude and longitude values in the activity_details table have been breaking their ability to use other geolocation processes.

A junior engineer has written the following code to add CHECK constraints to the Delta Lake table:

```sql
ALTER TABLE activity_details 
ADD CONSTRAINT valid_coordinates 
CHECK (latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180)
```

A senior engineer has confirmed the above logic is correct and the valid ranges for latitude and longitude are provided, but the code fails when executed.

Which statement explains the cause of this failure?

**A.** Because another team uses this table to support a frequently running application, two-phase locking is preventing the operation from committing.

**B.** The activity_details table already exists; CHECK constraints can only be added during initial table creation.

**C.** The activity_details table already contains records that violate the constraints; all existing data must pass CHECK constraints in order to add them to an existing table.

**D.** The activity_details table already contains records; CHECK constraints can only be added prior to inserting values into a table.

**E.** The current table schema does not contain the field valid_coordinates; schema evolution will need to be enabled before altering the table to add a constraint.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 3: Data Transformation, Cleansing, and Quality (10%)**
  - Sub-topic: Implementing Data Quality rules and constraints (e.g., Delta Lake constraints, Expectations).

- **Section 7: Ensuring Data Security and Compliance**
  - Sub-topic: Managing table properties and metadata.

---

### Important Keywords to Focus On

- **"Downstream consumers ... complaining about data quality"**: Indicates that bad data is **already present** in the table (this is the critical clue).
- **"fails when executed"**: The `ALTER TABLE ... ADD CONSTRAINT` command is syntactically correct but cannot complete execution.
- **"valid ranges"**: The logic of the constraint is mathematically sound and correctly specified.
- **"existing data must pass CHECK constraints"**: This is the fundamental rule for applying schema-level constraints to existing Delta Lake tables.

---

### Correct Option: C

**Explanation:**

In Delta Lake, when you add a `CHECK` constraint to an existing table, Databricks immediately performs a **validation scan** of the existing data:

1. **Validation Requirement**: A constraint cannot be successfully added to an existing table if any row currently stored in the table violates the new rule. This is a fundamental ACID property ensuring data integrity.

2. **Scenario Context**: The prompt explicitly states:
   - Downstream consumers are complaining about "invalid latitude and longitude values"
   - This confirms the table contains records that fall outside the valid ranges specified in the CHECK constraint
   - When the ALTER TABLE command executes, it scans all existing data against the new constraints

3. **Why the Command Fails**: 
   - Databricks performs a full scan of the activity_details table to validate that all existing records satisfy the constraint
   - It finds records where latitude or longitude are outside the specified ranges
   - The validation fails, and the ALTER TABLE operation aborts without applying the constraint
   - No records are added, modified, or deleted; the table remains in its original state

4. **Resolution Path**: To fix this situation, the engineer would:
   - First, identify and remediate the invalid records (DELETE or UPDATE them to meet the constraint requirements)
   - Then, re-run the `ALTER TABLE ... ADD CONSTRAINT` command
   - Only once all existing data passes the constraint can the constraint be successfully added

5. **Why This is Good Design**: This validation requirement ensures that:
   - No constraint is applied that would be violated by existing data
   - Data integrity is maintained at all times
   - Future writes will be rejected if they violate constraints

---

### Why Other Options are Incorrect

- **A. Two-phase locking**: Delta Lake uses **Optimistic Concurrency Control (OCC)**, not two-phase locking. While concurrent modification conflicts can occur in Delta Lake, they typically result in a `ConcurrentModificationException`, not a constraint validation failure. Additionally, adding a metadata constraint is different from writing data and doesn't trigger the same concurrency mechanisms.

- **B. Only during creation**: This is false. Delta Lake is flexible and fully supports schema evolution. Constraints can be added to existing tables at any time using `ALTER TABLE`. This is a key feature for adding data quality rules retroactively to existing schemas.

- **D. Only prior to inserting values**: This is similar to option B and equally false. You can add constraints to tables that already contain data, provided that the existing data complies with the constraint logic. The distinction is about data compliance, not insertion timing.

- **E. Schema evolution requirement for "valid_coordinates"**: This is a red herring that misunderstands the constraint syntax. `valid_coordinates` is the **name of the constraint** being created, not a field in the table schema. The fields involved in the constraint (latitude and longitude) already exist in the table. Schema evolution would only be necessary if adding a new physical column to the table, not for constraint operations.

---

---

## Question 5: Schema Changes to Aggregate Tables

To reduce storage and compute costs, the data engineering team has been tasked with curating a series of aggregate tables leveraged by business intelligence dashboards, customer-facing applications, production machine learning models, and ad hoc analytical queries.

The data engineering team has been made aware of new requirements from a customer-facing application, which is the only downstream workload they manage entirely. As a result, an aggregate table used by numerous teams across the organization will need to have a number of fields renamed, and additional fields will also be added.

Which of the solutions addresses the situation while minimally interrupting other teams in the organization without increasing the number of tables that need to be managed?

**A.** Send all users notice that the schema for the table will be changing; include in the communication the logic necessary to revert the new table schema to match historic queries.

**B.** Configure a new table with all the requisite fields and new names and use this as the source for the customer-facing application; create a view that maintains the original data schema and table name by aliasing select fields from the new table.

**C.** Create a new table with the required schema and new fields and use Delta Lake's deep clone functionality to sync up changes committed to one table to the corresponding table.

**D.** Replace the current table definition with a logical view defined with the query logic currently writing the aggregate table; create a new table to power the customer-facing application.

**E.** Add a table comment warning all users that the table schema and field names will be changing on a given date; overwrite the table in place to the specifications of the customer-facing application.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 8: Data Governance**
  - Sub-topic: Create and add descriptions/metadata about enterprise data to make it more discoverable.

- **Section 10: Data Modelling**
  - Sub-topic: Design and implement scalable data models using Delta Lake to manage large datasets.

---

### Important Keywords to Focus On

- **"only downstream workload they manage entirely"**: The customer-facing application is the only consumer whose requirements can be directly implemented; other teams' needs must be preserved.
- **"fields renamed"**: Other teams likely have existing queries using the old field names, which would break if changed directly.
- **"minimally interrupting other teams"**: Solution must maintain backward compatibility for existing consumers.
- **"without increasing the number of tables"**: Single source of truth is preferred; don't want to manage multiple versions of the same logical data.
- **"used by numerous teams across the organization"**: High-impact table; changes affect many dependencies.

---

### Correct Option: B

**Explanation:**

This solution elegantly separates the physical data layer from the logical interface layer, solving the conflicting requirements:

1. **Physical Layer (New Table)**:
   - Create a new physical table with the new schema required by the customer-facing application
   - Include all renamed fields with their new names
   - Include any additional fields needed by the customer-facing application
   - This table is used directly as the data source for the customer-facing application

2. **Logical Layer (View)**:
   - Create a view with the name and schema of the **original table**
   - The view uses column aliasing to map the new field names back to the old field names
   - The view selects only those fields that other teams are currently accessing
   - This view queries from the new physical table

3. **Benefits of This Approach**:
   - **Backward Compatibility**: Teams using the old table name and old field names experience no disruption; they query the view, which provides the exact same interface as before
   - **Single Source of Truth**: All data comes from one place (the new table), ensuring consistency and reducing maintenance burden
   - **Clean Schema Evolution**: The customer-facing application gets the new schema it needs
   - **No Duplication**: Data isn't duplicated between tables; the view is just a logical layer

4. **Example**:
   - Old table had: `customer_id`, `total_revenue`, `purchase_count`
   - New table has: `cust_id`, `total_revenue`, `purchase_count`, `churn_risk_score`
   - View query: `SELECT cust_id AS customer_id, total_revenue, purchase_count FROM new_table`
   - Old consumers query the view and see their expected schema
   - New application queries the new table directly and sees the new schema

---

### Why Other Options are Incorrect

- **A. Send notice with reversion logic**: This is operationally burdensome and maintains no backward compatibility. Every downstream consumer would need to update their queries, and providing "reversion logic" doesn't solve the problem of breaking existing queries.

- **C. Use deep clone with sync**: Deep clone is useful for creating independent copies, but it doesn't solve the schema mismatch problem. Additionally, manually syncing changes between two tables adds operational complexity and potential for data inconsistency. This also increases the number of tables to manage (violating the requirement).

- **D. Replace table with view, create new table**: This inverts the logic and creates problems:
  - The original table (now a view) would need to compute the aggregations on demand, which is inefficient
  - Other teams would lose direct access to the physical data
  - Creating a new separate table increases the number of tables to manage
  - This doesn't actually address the schema conflict; it just shuffles it around

- **E. Overwrite table in place with warning comment**: This is the worst option because:
  - Warning comments don't prevent breakage; they just notify users after the fact
  - All existing queries from other teams will fail when field names change
  - Overwriting the table means all downstream applications break simultaneously
  - No backward compatibility or grace period for migration

---

---

## Question 6: Change Data Feed vs. Streaming Reads

A nightly job ingests data into a Delta Lake table using the following code:

```python
spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .load("s3://input-path") \
  .writeStream \
  .mode("append") \
  .option("checkpointLocation", "s3://checkpoint") \
  .table("bronze")
```

The next step in the pipeline requires a function that returns an object that can be used to manipulate new records that have not yet been processed to the next table in the pipeline.

Which code snippet completes this function definition?

```python
def new_records():
```

**A.** `return spark.readStream.table("bronze")`

**B.** `return spark.readStream.load("bronze")`

**C.** `return spark.read.option("readChangeFeed", "true").table("bronze")`

**D.** `return spark.read.option("readChangeFeed", "true").table("bronze")`

**E.** (Empty/No valid option)

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 1: Developing Code for Data Processing using Python and SQL**
  - Sub-topic: Build and manage reliable, production-ready data pipelines for batch and streaming data using Lakeflow Spark Declarative Pipelines and Autoloader.
  - Sub-topic: Compare Spark Structured Streaming and Lakeflow Spark Declarative Pipelines to determine the optimal approach for building scalable ETL pipelines.

- **Section 6: Cost & Performance Optimisation**
  - Sub-topic: Apply Change Data Feed (CDF) to address specific limitations of streaming tables and enhance latency.

---

### Important Keywords to Focus On

- **"nightly job ingests data"**: Data is being written via Structured Streaming (writeStream).
- **"function that returns an object that can be used to manipulate new records"**: Need to read new/unprocessed records incrementally.
- **"records that have not yet been processed to the next table"**: Need to read only incremental/new changes, not all historical data.
- **"readStream vs. readChangeFeed"**: Two different mechanisms for reading Delta changes incrementally.

---

### Correct Option: A

**Explanation:**

The key to this question is understanding the requirements for reading incremental data from a Delta table:

1. **Understanding the Data Flow**:
   - The nightly job appends new records to the "bronze" table using Structured Streaming
   - The next stage requires a function that returns an object to process **only new records** that haven't been processed yet
   - This is an incremental read requirement, not a full table scan

2. **Why `spark.readStream.table("bronze")` is Correct**:
   - `readStream` initiates a Structured Streaming read operation
   - `.table("bronze")` specifies the Delta table as the source
   - When first executed with a checkpoint, it reads all data (historical)
   - On subsequent executions with the same checkpoint, it reads only the new data appended since the last checkpoint
   - The checkpoint maintains state, enabling incremental processing

3. **How This Works with the Nightly Job**:
   - The nightly job writes new records to "bronze"
   - The next pipeline stage uses a readStream to detect these new records
   - The checkpoint ensures the same records aren't processed multiple times
   - Each execution processes only the delta (new records) from the previous checkpoint

4. **Returning an Object for Manipulation**:
   - `readStream` returns a `StreamingDataFrame` object
   - This object has methods to manipulate the data (`.transform()`, `.select()`, `.filter()`, etc.)
   - The DataFrame can be written to another table, processed, or joined with other data

---

### Why Other Options are Incorrect

- **B. `spark.readStream.load("bronze")`**: While syntactically similar, `.load()` expects a file path, not a table name. For reading from a Delta table, you must use `.table()`.

- **C & D. `spark.read.option("readChangeFeed", "true").table("bronze")`**: These use the wrong read method:
  - `spark.read` is for batch reads, not streaming reads
  - It would read the entire table or a snapshot at a point in time, not incrementally
  - Change Data Feed (CDF) is designed to capture INSERT, UPDATE, DELETE operations
  - CDF is typically used when the bronze table itself has been updated/deleted, not just appended
  - For a simple append-only scenario, Structured Streaming with checkpoints is the correct pattern

---

---

## Question 7: Change Data Feed with Multiple Executions

A junior data engineer seeks to leverage Delta Lake's Change Data Feed functionality to create a Type 1 table representing all of the values that have ever been valid for all rows in a bronze table created with the property `delta.enableChangeDataFeed = true`. They plan to execute the following code as a daily job:

```python
spark.read \
  .option("readChangeData", "true") \
  .table("bronze") \
  .write \
  .mode("append") \
  .table("gold")
```

Which statement describes the execution and results of running the above query multiple times?

**A.** Each time the job is executed, newly updated records will be merged into the target table, overwriting previous values with the same primary keys.

**B.** Each time the job is executed, the entire available history of inserted or updated records will be appended to the target table, resulting in many duplicate entries.

**C.** Each time the job is executed, the target table will be overwritten using the entire history of inserted or updated records, giving the desired result.

**D.** Each time the job is executed, the differences between the original and current versions are calculated; this may result in duplicate entries for some records.

**E.** Each time the job is executed, only those records that have been inserted or updated since the last execution will be appended to the target table, giving the desired result.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 1: Developing Code for Data Processing using Python and SQL**
  - Sub-topic: Build and manage reliable, production-ready data pipelines for batch and streaming data.

- **Section 6: Cost & Performance Optimisation**
  - Sub-topic: Apply Change Data Feed (CDF) to address specific limitations of streaming tables and enhance latency.

---

### Important Keywords to Focus On

- **"execute the following code as a daily job"**: The job runs multiple times; state/checkpoint management is critical.
- **"readChangeData"** is not a standard option; it should be `readChangeFeed`
- **"entire available history of inserted or updated records"**: CDF by default (without checkpoint) returns all changes since table creation
- **"write.mode(append)"**: Adds data to the target table without deduplication
- **"running the above query multiple times"**: Each execution will read the full history again unless checkpointing is implemented

---

### Correct Option: B

**Explanation:**

This question tests understanding of Change Data Feed behavior without checkpoint management:

1. **What Change Data Feed Does**:
   - CDF tracks all changes (INSERT, UPDATE, DELETE) to a Delta table
   - When you read with `readChangeFeed`, it returns the entire history of changes since table creation
   - This is by default; it requires additional configuration to read incremental changes

2. **The Problem with the Code**:
   - The code uses `.mode("append")` to write to the gold table
   - There is no checkpoint configured to track which changes have already been processed
   - Without a checkpoint, each execution re-reads the ENTIRE change history from the bronze table
   - The gold table grows with duplicate entries on each execution

3. **What Happens on Each Execution**:
   - **First run**: Reads all changes from bronze table creation until now, appends them to gold
   - **Second run**: Reads ALL changes again from bronze table creation until now, appends them to gold (duplicates!)
   - **Third run**: Same as second run, more duplicates
   - Result: The gold table contains multiple copies of every record

4. **Why This Doesn't Create the Desired Type 1 Table**:
   - A Type 1 table should show only the most recent state for each record
   - Instead, this approach appends all historical changes without deduplication
   - The gold table becomes a full audit history with duplicates, not a Type 1 dimension

---

### Why Other Options are Incorrect

- **A. Newly updated records will be merged**: MERGE operations are not used in this code; the code uses `mode("append")`.

- **C. Overwriting with entire history**: The code uses `mode("append")`, not `mode("overwrite")`. Additionally, overwriting the entire table each time is inefficient.

- **D. Differences between original and current versions**: CDF doesn't calculate differences; it provides a feed of all changes. Additionally, the code doesn't have any logic to compare versions.

- **E. Only those records inserted or updated since last execution**: This would be the desired behavior, but it requires checkpoint management to track state between executions. The code as written has no checkpoint, so it re-reads all changes every time.

**To Fix This Code for Proper Incremental Processing**:
   - Add a checkpoint to track which changes have been processed
   - Use `readStream` with a checkpoint for streaming semantics
   - Or implement a watermark-based approach to track the last processed version

---

---

## Question 8: Detecting Suboptimal Cached Table Performance

The data engineer is using Spark's MEMORY_ONLY storage level.

Which indicators should the data engineer look for in the Spark UI's Storage tab to signal that a cached table is not performing optimally?

**A.** On Heap Memory Usage is within 75% of Off Heap Memory Usage

**B.** The RDD Block Name includes the "*" annotation signaling a failure to cache

**C.** Size on Disk is > 0

**D.** The number of Cached Partitions > the number of Spark Partitions

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 6: Cost & Performance Optimisation**
  - Sub-topic: Understand the optimization techniques used by Databricks to ensure the performance of queries on large datasets.

---

### Important Keywords to Focus On

- **"MEMORY_ONLY storage level"**: Data should be cached entirely in memory, not spilled to disk.
- **"Spark UI's Storage tab"**: Where to observe caching metrics and diagnostics.
- **"Size on Disk is > 0"**: Indicates data has been spilled from memory to disk (inefficient).
- **"MEMORY_ONLY"** means no disk spillover is expected; if data appears on disk, caching has failed.

---

### Correct Option: C

**Explanation:**

Understanding storage levels and their implications is critical for performance optimization:

1. **MEMORY_ONLY Storage Level**:
   - This storage level specifies that data should be cached entirely in memory
   - There is no failover; if data cannot fit in memory, it is NOT cached
   - Spilling to disk is explicitly NOT supported with MEMORY_ONLY

2. **What Size on Disk > 0 Indicates**:
   - If "Size on Disk" shows a value greater than 0, it means Spark has spilled data from memory to disk
   - This is a sign that the memory allocation is insufficient for the MEMORY_ONLY configuration
   - This occurs when:
     - The executor memory is too small for the dataset
     - The working memory fraction is too small relative to the dataset
     - Multiple datasets are competing for the same memory space

3. **Performance Implications**:
   - Disk I/O is orders of magnitude slower than memory access
   - Spilled data will cause significant slowdowns in query execution
   - The cache is not providing its intended performance benefit

4. **How to Detect in Spark UI**:
   - Navigate to the Storage tab in Spark UI
   - Look at each cached RDD/table entry
   - Check the "Size on Disk" column
   - Any non-zero value indicates suboptimal performance

---

### Why Other Options are Incorrect

- **A. On Heap Memory Usage is within 75% of Off Heap Memory Usage**: This ratio doesn't inherently indicate a problem. On-heap and off-heap memory serve different purposes. This option confuses memory management concepts.

- **B. RDD Block Name includes "*" annotation**: This is not a standard Spark UI indicator. RDD block names don't have a "*" annotation that signals caching failure. This is incorrect Spark UI interpretation.

- **D. Number of Cached Partitions > Number of Spark Partitions**: This is logically impossible. You cannot cache more partitions than the RDD has. This demonstrates a misunderstanding of partitioning concepts.

---

---

## Question 9: Git Workflow with Databricks Repos

A developer has successfully configured their credentials for Databricks Repos and cloned a remote Git repository. They do not have privileges to make changes to the main branch, which is the only branch currently visible in their workspace.

Which approach allows this user to share their code updates without the risk of overwriting the work of their teammates?

**A.** Use Repos to checkout all changes and send the git diff log to the team.

**B.** Use Repos to create a fork of the remote repository, commit all changes, and make a pull request on the source repository.

**C.** Use Repos to pull changes from the remote Git repository; commit and push changes to a branch that appeared as changes were pulled.

**D.** Use Repos to merge all differences and make a pull request back to the remote repository.

**E.** Use Repos to create a new branch, commit all changes, and push changes to the remote Git repository.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 9: Debugging and Deploying**
  - Sub-topic: Deploying CI/CD - Configure and integrate with Git-based CI/CD workflows using Databricks Git Folders for notebook and code deployment.

- **Section 1: Developing Code for Data Processing using Python and SQL**
  - Sub-topic: Using Python and Tools for development - Design and implement a scalable Python project structure optimized for Databricks Asset Bundles (DABs), enabling modular development, deployment automation, and CI/CD integration.

---

### Important Keywords to Focus On

- **"do not have privileges to make changes to the main branch"**: Cannot commit directly to main; need an alternative workflow.
- **"only branch currently visible in their workspace"**: Main branch is the only default branch shown; other branches must be explicitly created or checked out.
- **"without the risk of overwriting the work of their teammates"**: Need isolated workspace to prevent conflicts.
- **"share their code updates"**: Need a way to contribute work back to the team.

---

### Correct Option: E

**Explanation:**

This question tests understanding of Git workflow best practices and how Databricks Repos facilitates branching:

1. **The Constraint**:
   - The developer has no write permissions to the main branch
   - Only the main branch is currently visible in their workspace
   - They need to contribute code updates without the risk of conflicts

2. **The Solution - Create a Feature Branch**:
   - The developer should create a new branch in Databricks Repos using Git commands
   - The new branch is a personal feature branch (e.g., `feature/my-changes`)
   - All changes are committed to this feature branch, not to main
   - The feature branch is then pushed to the remote Git repository

3. **Why This Approach Works**:
   - **Isolation**: Changes are isolated in a feature branch, not on main
   - **No Conflicts**: Other teammates work on main or their own branches; no risk of overwriting their work
   - **Traceable History**: The feature branch maintains a clear history of changes
   - **Pull Request Ready**: Once changes are pushed to a feature branch on the remote repository, the developer can request the repository maintainer to review and merge via a pull request

4. **Git Workflow**:
   ```bash
   # Create and checkout new feature branch
   git checkout -b feature/my-changes
   
   # Make changes to files
   # Add and commit changes
   git add <files>
   git commit -m "My feature"
   
   # Push branch to remote repository
   git push origin feature/my-changes
   
   # Repository maintainer reviews and merges via pull request
   ```

5. **Integration with Databricks Repos**:
   - Databricks Repos handles Git authentication and operations
   - The developer can create branches using Git commands within Databricks
   - Push operations sync the branch to the remote Git repository
   - This enables standard Git workflows in Databricks notebooks

---

### Why Other Options are Incorrect

- **A. Send git diff log to the team**: This is not a proper version control workflow. Manually sharing diffs is error-prone and doesn't integrate with the Git repository. Changes would not be tracked in version control.

- **B. Create a fork of the remote repository**: While forking is a valid workflow for open-source contributions, it's not necessary in this scenario. The developer has access to the repository; they just need a branch. Forking adds unnecessary complexity for an internal team workflow.

- **C. Pull changes, then commit and push to a branch that appeared as changes were pulled**: This is confusing and not how Git workflows work. You don't automatically get a branch from pulling; you create branches explicitly. This option describes a non-standard process.

- **D. Merge all differences and make a pull request**: The developer cannot merge into main (no privileges). This option is not viable given the access constraints.

---

---

## Question 10: Reading New Records with Change Data Feed

A nightly job ingests data into a Delta Lake table using the following code:

```python
spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .load("s3://input-path") \
  .writeStream \
  .mode("append") \
  .option("checkpointLocation", "s3://checkpoint") \
  .table("bronze")
```

The next step in the pipeline requires a function that returns an object that can be used to manipulate new records that have not yet been processed to the next table in the pipeline.

Which code snippet completes this function definition?

```python
def new_records():
```

**A.** `return spark.readStream.table("bronze")`

**B.** `return spark.read.option("readChangeFeed", "true").table("bronze")`

**C.** (Empty/No valid option)

**D.** (Empty/No valid option)

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 1: Developing Code for Data Processing using Python and SQL**
  - Sub-topic: Build and manage reliable, production-ready data pipelines for batch and streaming data using Lakeflow Spark Declarative Pipelines and Autoloader.
  - Sub-topic: Create and Automate ETL workloads using Jobs via UI/APIs/CLI.

- **Section 6: Cost & Performance Optimisation**
  - Sub-topic: Apply Change Data Feed (CDF) to address specific limitations of streaming tables and enhance latency.

---

### Important Keywords to Focus On

- **"nightly job ingests data"**: Data arrives periodically in batches.
- **"new records that have not yet been processed"**: Need incremental/streaming read semantics.
- **"returns an object that can be used to manipulate"**: Should return a DataFrame or equivalent that can be transformed.

---

### Correct Option: B

**Explanation:**

This variant of the earlier question tests understanding of when to use Change Data Feed vs. Structured Streaming:

1. **Difference from Question 6**:
   - This question has different options, specifically highlighting `readChangeFeed` as an option
   - The context is the same (nightly ingestion, processing new records)
   - But the focus here is on which API is correct for this specific scenario

2. **Why `spark.read.option("readChangeFeed", "true").table("bronze")` is Correct**:
   - `readChangeFeed` reads the change data feed from the bronze table
   - It returns the set of changes (inserts, updates, deletes) since the last checkpoint/query
   - For tables with CDC enabled, this is the appropriate way to read incremental changes
   - Using a batch read (`.read` instead of `.readStream`) is acceptable here because the job is running nightly

3. **Batch vs. Stream Read in This Context**:
   - The nightly job writes data and then stops
   - A downstream job can use a batch read with CDF to process those changes once per night
   - The checkpoint from the writeStream ensures new data is written
   - The readChangeFeed picks up those changes at a batch interval

4. **Object Returned**:
   - `readChangeFeed` returns a DataFrame containing the change records
   - This DataFrame can be manipulated with `.select()`, `.filter()`, `.transform()`, etc.
   - It can be written to another table for further processing

---

### Why Other Options are Incorrect

- **A. `spark.readStream.table("bronze")`**: While this works for continuous streaming scenarios, using `.readStream` with a daily batch job is overkill:
  - It's designed for continuous ingestion, not batch processing
  - A batch read with CDF is more appropriate for nightly jobs
  - Note: This option might also be correct in some scenarios, but B is more specific to the use case

- **C & D. (Empty/No valid option)**: If these are truly empty, then B would be the only viable option.

---

---

## Question 11: CDC with Hourly Ingestion Requirements

An upstream system is emitting change data capture (CDC) logs that are being written to a cloud object storage directory. Each record in the log indicates the change type (insert, update, or delete) and the values for each field after the change. The source table has a primary key identified by the field pk_id.

For auditing purposes, the data governance team wishes to maintain a full record of all values that have ever been valid in the source system. For analytical purposes, only the most recent value for each record needs to be recorded. The Databricks job to ingest these records occurs once per hour, but each individual record may have changed multiple times over the course of an hour.

Which solution meets these requirements?

**A.** Iterate through an ordered set of changes to the table, applying each in turn to create the current state of the table, (insert, update, delete), timestamp of change, and the values.

**B.** Use merge into to insert, update, or delete the most recent entry for each pk_id into a table, then propagate all changes throughout the system.

**C.** Deduplicate records in each batch by pk_id and overwrite the target table.

**D.** Use Delta Lake's change data feed to automatically process CDC data from an external system, propagating all changes to all dependent tables in the Lakehouse.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 2: Data Ingestion & Acquisition**
  - Sub-topic: Design and implement data ingestion pipelines to efficiently ingest a variety of data formats from diverse sources.

- **Section 3: Data Transformation, Cleansing, and Quality**
  - Sub-topic: Write efficient Spark SQL and PySpark code to apply advanced data transformations.

- **Section 8: Data Governance**
  - Sub-topic: Create and add descriptions/metadata about enterprise data to make it more discoverable.

---

### Important Keywords to Focus On

- **"maintain a full record of all values that have ever been valid"**: Need a Type 2 or audit table that keeps historical records.
- **"only the most recent value for each record needs to be recorded"**: Also need a Type 1 table for analytical use.
- **"change type (insert, update, or delete)"**: CDC logs contain operation information.
- **"each individual record may have changed multiple times over the course of an hour"**: Deduplication by pk_id and selecting latest is necessary.
- **"Databricks job to ingest these records occurs once per hour"**: Batch ingestion frequency.

---

### Correct Option: D

**Explanation:**

This question tests a nuanced understanding of CDC processing and Delta Lake's capabilities:

1. **The Dual Requirement**:
   - Audit table: Full history of all changes
   - Analytical table: Most recent state for each record
   - These seem contradictory but are actually complementary

2. **Delta Lake's Change Data Feed as a Solution**:
   - When CDC logs are written to Delta Lake tables, CDF can be enabled on those tables
   - CDF captures all changes (INSERT, UPDATE, DELETE operations)
   - CDF maintains complete history while also supporting queries for current state

3. **How Delta Lake CDF Solves Both Requirements**:
   - **For auditing**: Querying the CDF with `readChangeFeed` gives the complete change history
   - **For analytics**: Using a Type 1 table built from CDF changes gives the most recent state
   - The table itself serves as the audit log; views can provide the analytical layer

4. **Why This is Superior to Other Approaches**:
   - CDF is designed specifically for CDC scenarios
   - It automatically handles the complexity of tracking operations (I/U/D)
   - It provides both historical and current-state access patterns
   - It integrates seamlessly with Delta Lake's ACID guarantees

5. **Propagating Changes**:
   - CDF automatically propagates all changes throughout the Lakehouse
   - Dependent tables can subscribe to these changes and update accordingly
   - This is more maintainable than manual CDC processing

---

### Why Other Options are Incorrect

- **A. Iterate through ordered changes**: While this describes a valid conceptual approach, it's not practical:
  - Requires manual implementation of complex logic
  - Difficult to maintain and debug
  - Doesn't leverage Delta Lake's built-in CDC capabilities
  - Error-prone for production systems

- **B. Use MERGE for most recent entry only**: This only solves the analytical requirement:
  - Doesn't maintain audit/historical records
  - MERGE on its own doesn't capture the full change history
  - Doesn't meet the "full record of all values that have ever been valid" requirement

- **C. Deduplicate by pk_id and overwrite**: This loses the historical record:
  - Only keeps the most recent state
  - Doesn't maintain the audit trail for compliance/governance
  - Doesn't capture the change operations (I/U/D)
  - Violates the auditing requirement

---

---

## Question 12: Time Travel for Comparing Table Versions

A Delta Lake table in the Lakehouse named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.

Immediately after each update succeeds, the data engineering team would like to determine the difference between the new version and the previous version of the table.

Given the current implementation, which method can be used?

**A.** Execute a query to calculate the difference between the new version and the previous version using Delta Lake's built-in versioning and time travel functionality.

**B.** Parse the Delta Lake transaction log to identify all newly written data files.

**C.** Parse the Spark event logs to identify those rows that were updated, inserted, or deleted.

**D.** Execute DESCRIBE HISTORY customer_churn_params to obtain the full operation metrics for the update, including a log of all records that have been added or modified.

**E.** Use Delta Lake's change data feed to identify those records that have been updated, inserted, or deleted.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 6: Cost & Performance Optimisation**
  - Sub-topic: Understand the optimization techniques used by Databricks to ensure the performance of queries on large datasets.

- **Section 8: Data Governance**
  - Sub-topic: Create and add descriptions/metadata about enterprise data to make it more discoverable.

---

### Important Keywords to Focus On

- **"immediately after each update succeeds"**: Need to compare versions right after an overwrite operation.
- **"overwriting the table"**: The operation replaces all data; CDF won't help since we're doing a full overwrite.
- **"difference between the new version and the previous version"**: Need to compare two points in time.
- **"time travel functionality"**: Delta Lake's ability to query previous versions.

---

### Correct Option: A

**Explanation:**

Delta Lake's time travel capability is designed specifically for this use case:

1. **Delta Lake Versioning**:
   - Every write operation to a Delta table increments the version number
   - Each version is immutable and permanently recorded in the transaction log
   - Versions are timestamped for easy reference

2. **Using Time Travel to Compare Versions**:
   - Query the current (new) version: `SELECT * FROM customer_churn_params`
   - Query the previous version: `SELECT * FROM customer_churn_params@v{current_version - 1}`
   - Or use timestamps: `SELECT * FROM customer_churn_params@{timestamp_before_update}`

3. **Finding the Differences**:
   ```sql
   -- Get records in new version but not in previous
   SELECT * FROM customer_churn_params
   EXCEPT
   SELECT * FROM customer_churn_params@v{v-1}
   
   -- Get records deleted in new version
   SELECT * FROM customer_churn_params@v{v-1}
   EXCEPT
   SELECT * FROM customer_churn_params
   ```

4. **Why This is Optimal**:
   - Built-in Delta Lake functionality, no parsing required
   - Time travel is designed for exactly this use case
   - Simple, readable SQL queries
   - Efficient; Delta Lake optimizes these comparisons
   - Works with the existing table structure

5. **Workflow**:
   - After the nightly overwrite completes, the version number increments
   - Query the new version and the previous version
   - Compare them using set operations (EXCEPT, UNION, etc.)
   - Obtain the difference without additional data structures

---

### Why Other Options are Incorrect

- **B. Parse the transaction log**: While possible, this is unnecessarily complex:
  - Requires manually parsing JSON/binary transaction log files
  - Doesn't provide data-level differences, only file information
  - Error-prone and difficult to maintain
  - Time travel queries are the intended approach

- **C. Parse Spark event logs**: Event logs track job execution, not data changes:
  - Don't contain information about which rows were modified
  - Primarily used for performance debugging, not data analysis
  - Not designed for determining data differences

- **D. DESCRIBE HISTORY**: This shows operation metadata, not data differences:
  - Returns operation summaries (operation type, rows affected, etc.)
  - Doesn't provide the actual row-level differences
  - Useful for understanding what operations occurred, not what changed in the data

- **E. Change Data Feed**: CDF won't work for full overwrites:
  - CDF tracks changes (I/U/D operations)
  - Full overwrites are considered writes of new files, not individual operations
  - CDF would show all records as new, not identifying what actually changed
  - Time travel is more appropriate for comparing table states

---

---

## Question 13: Data Retention and VACUUM Operations

The data engineering team has configured a job to process customer requests to be forgotten (have their data deleted). All user data that needs to be deleted is stored in Delta Lake tables using default table settings.

The team has decided to process all deletions from the previous week as a batch job at 1am each Sunday. The total duration of this job is less than one hour. Every Monday at 3am, a batch job executes a series of VACUUM commands on all Delta Lake tables throughout the organization.

The compliance officer has recently learned about Delta Lake's time travel functionality. They are concerned that this might allow continued access to deleted data.

Assuming all delete logic is correctly implemented, which statement correctly addresses this concern?

**A.** Because the VACUUM command permanently deletes all files containing deleted records, deleted records may be accessible with time travel for around 24 hours.

**B.** Because the default data retention threshold is 24 hours, data files containing deleted records will be retained until the VACUUM job is run the following day.

**C.** Because the default data retention threshold is 7 days, data files containing deleted records will be retained until the VACUUM job is run 8 days later.

**D.** Because Delta Lake's delete statements have ACID guarantees, deleted records will be permanently purged from all storage systems as soon as a delete job completes.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 7: Ensuring Data Security and Compliance**
  - Sub-topic: Ensuring Compliance - Develop a data purging solution ensuring compliance with data retention policies.

- **Section 8: Data Governance**
  - Sub-topic: Create and add descriptions/metadata about enterprise data to make it more discoverable.

---

### Important Keywords to Focus On

- **"delete statements have ACID guarantees"**: Deletion is atomic, but data files aren't immediately purged.
- **"default data retention threshold"**: Delta Lake has a default retention policy before VACUUM can delete files.
- **"VACUUM commands"**: Permanently removes data files; runs AFTER deletions.
- **"deleted records may be accessible with time travel"**: Concern about post-deletion access via time travel.
- **"default table settings"**: Important because retention policy can be customized.

---

### Correct Option: B

**Explanation:**

Understanding the relationship between Delta Lake's deletion mechanism, retention policies, and time travel is critical for compliance:

1. **How Delta Lake Deletion Works**:
   - When a DELETE statement is executed, the rows are marked as deleted in the transaction log
   - The underlying data files are NOT immediately deleted from storage
   - This maintains ACID semantics and allows time travel to previous versions
   - The deleted records are logically deleted but physically still exist

2. **Default Data Retention Threshold**:
   - Delta Lake has a default retention threshold of **7 days** (604800 seconds)
   - Actually, the default is **30 days** for most Databricks deployments
   - However, some configurations use **7 days**
   - The option states "default data retention threshold is 7 days" - this is the key fact for this question

   *Note: The actual default in production Databricks is 30 days, but this question specifies the context.*

3. **The Timeline**:
   - **Sunday 1am**: DELETE job runs and completes within 1 hour (by 2am)
   - **Sunday 2am - Monday 3am**: 25 hours have passed
   - **Monday 3am**: VACUUM job runs
   - **Data Retention**: 7 days from deletion = until the following Sunday (approximately)

4. **Time Travel Accessibility**:
   - Between deletion and VACUUM, deleted records are still accessible via time travel
   - The data files still exist in storage
   - Queries with `@v{version}` or `@{timestamp}` can retrieve the data
   - This is by design, allowing recovery from accidental deletions

5. **Why This Matters for Compliance**:
   - The 7-day retention window is a grace period
   - VACUUM can only run after the retention period expires
   - Running VACUUM the very next day (Monday) is TOO EARLY to actually delete files
   - Files from Sunday's deletion won't be purged until after the 7-day window (the following Sunday)

---

### Why Other Options are Incorrect

- **A. Deleted records may be accessible for around 24 hours**: This is too short:
  - The default retention is 7 days (or 30 days in most Databricks deployments)
  - 24 hours is less than the minimum retention window
  - Time travel would be blocked after 24 hours, but files would still exist

- **C. Default data retention threshold is 7 days, but files retained until 8 days later**: This has the math backwards:
  - If the retention threshold is 7 days, then files are retained for 7 days after deletion
  - They would be purged 7 days later, not 8 days later
  - Running VACUUM the next day has no effect; it must wait for the threshold to pass

- **D. Deleted records permanently purged as soon as delete job completes**: This is incorrect:
  - DELETE statements mark rows as deleted but don't purge files
  - Permanent purging only happens when VACUUM runs AND the retention period has elapsed
  - This confuses logical deletion with physical file deletion

---

---

## Question 14: Language Interoperability in Databricks Notebooks

A junior member of the data engineering team is exploring the language interoperability of Databricks notebooks. The intended outcome of the below code is to register a view of all sales that occurred in countries on the continent of Africa that appear in the geo_lookup table.

Before executing the code, running SHOW TABLES on the current database indicates the database contains only two tables: geo_lookup and sales.

```python
# Cmd 1
countries_af = spark.sql("""
    SELECT country FROM geo_lookup WHERE continent = 'Africa'
""")

# Cmd 2
spark.sql("""
    CREATE VIEW sales_af AS
    SELECT * FROM sales WHERE country IN (
        SELECT * FROM countries_af
    )
""")
```

What will be the outcome of executing these command cells in order in an interactive notebook?

**A.** Both commands will succeed. Executing SHOW TABLES will show that countries_af and sales_af have been registered as views.

**B.** Cmd 1 will succeed. Cmd 2 will search all accessible databases for a table or view named countries_af; if this entity exists, Cmd 2 will succeed.

**C.** Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable representing a PySpark DataFrame.

**D.** Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable containing a list of strings.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 1: Developing Code for Data Processing using Python and SQL**
  - Sub-topic: Using Python and Tools for development - Design and implement a scalable Python project structure.

---

### Important Keywords to Focus On

- **"language interoperability of Databricks notebooks"**: Mixing Python and SQL in the same notebook.
- **"countries_af = spark.sql(...)"**: Assigning SQL query result to a Python variable.
- **"SELECT * FROM countries_af"**: Attempting to use a Python variable in SQL.
- **"PySpark DataFrame"**: What spark.sql() returns.

---

### Correct Option: C

**Explanation:**

This question tests understanding of how Python and SQL contexts interact in Databricks notebooks:

1. **Cmd 1 Analysis**:
   - `spark.sql(...)` executes a SQL query and returns a **PySpark DataFrame** object
   - `countries_af` is a Python variable that holds this DataFrame
   - The query executes successfully, returning a DataFrame with a "country" column containing African countries

2. **Cmd 2 Analysis**:
   - `spark.sql(...)` attempts to execute SQL code
   - The SQL code tries to reference `countries_af` as if it were a SQL table or view
   - **This fails** because `countries_af` is a Python variable, not a SQL object
   - SQL doesn't have access to Python variables directly

3. **Why Cmd 2 Fails**:
   - SQL execution context and Python execution context are separate
   - SQL can only reference:
     - Existing SQL tables
     - Existing SQL views
     - Temporary views created with `createOrReplaceTempView()`
   - A Python variable doesn't exist in the SQL execution context

4. **How to Fix Cmd 2**:
   - **Option 1**: Register the DataFrame as a temporary view
     ```python
     countries_af.createOrReplaceTempView("countries_af_view")
     spark.sql("CREATE VIEW sales_af AS SELECT * FROM sales WHERE country IN (SELECT * FROM countries_af_view)")
     ```
   - **Option 2**: Inject the DataFrame into the query using string interpolation
     ```python
     countries_list = countries_af.select("country").rdd.flatMap(lambda x: x).collect()
     # Use the list in SQL...
     ```

---

### Why Other Options are Incorrect

- **A. Both commands will succeed**: Cmd 2 will fail because countries_af is not registered as a SQL object.

- **B. Cmd 2 will search all databases for countries_af**: SQL won't automatically search for Python variables. It only searches for registered SQL tables/views, which countries_af is not.

- **D. countries_af is a Python variable containing a list of strings**: `spark.sql()` returns a DataFrame, not a list. While the DataFrame could be converted to a list, that's not the default behavior.

---

---

## Question 15: Using MLflow Models in PySpark DataFrames

The data science team has created and logged a production model using MLflow. The model accepts a list of column names and returns a new column of type DOUBLE.

The following code correctly imports the production model, loads the customers table containing the customer_id key column into a DataFrame, and defines the feature columns needed for the model.

```python
model = mlflow.pyfunc.load_model("models:/my_model/production")
df = spark.table("customers")
columns = ["age", "income", "purchase_history"]
```

Which code block will output a DataFrame with the schema "customer_id LONG, predictions DOUBLE"?

**A.** `df.map(lambda x: model(x[columns])).select("customer_id", "predictions")`

**B.** 
```python
df.select("customer_id",
    model(*columns).alias("predictions"))
```

**C.** `model.predict(df, columns)`

**D.** `df.apply(model, columns).select("customer_id", "predictions")`

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 1: Developing Code for Data Processing using Python and SQL**
  - Sub-topic: Develop User-Defined Functions (UDFs) using Pandas/Python UDF.

- **Section 3: Data Transformation, Cleansing, and Quality**
  - Sub-topic: Write efficient Spark SQL and PySpark code to apply advanced data transformations.

---

### Important Keywords to Focus On

- **"MLflow model accepts a list of column names"**: Model takes feature columns as input.
- **"returns a new column of type DOUBLE"**: Output is a single numeric value per row.
- **"output a DataFrame with the schema"**: Need to return a properly structured DataFrame.
- **"customer_id LONG, predictions DOUBLE"**: Expected schema.

---

### Correct Option: B

**Explanation:**

Applying MLflow models to Spark DataFrames requires understanding Pandas UDF patterns and Spark SQL integration:

1. **Understanding Option B**:
   - `df.select(...)` selects columns from the DataFrame
   - `"customer_id"` is selected directly
   - `model(*columns)` calls the model with unpacked column names, which registers a Pandas UDF
   - `.alias("predictions")` names the output column
   - Returns a DataFrame with customer_id and predictions columns

2. **How MLflow Models Work with DataFrames**:
   - MLflow pyfunc models can be applied to DataFrames as Pandas UDFs
   - The `*columns` unpacking passes the feature columns to the model
   - The model processes the data and returns predictions

3. **Why This Returns the Correct Schema**:
   - customer_id column is LONG (from the original table)
   - model output is DOUBLE (as specified)
   - The select creates a DataFrame with exactly these two columns

---

### Why Other Options are Incorrect

- **A. Using map with lambda**: While map could theoretically work:
   - map() would need to be combined with createDataFrame to produce a proper DataFrame
   - This is more complex than the direct select approach
   - The syntax shown wouldn't produce the expected schema

- **C. model.predict(df, columns)**: This is incorrect syntax:
   - MLflow pyfunc models don't have a .predict() method that works on entire DataFrames
   - This isn't how MLflow models are applied in PySpark
   - The proper way is through Pandas UDFs or direct model calls

- **D. df.apply()**: DataFrames don't have an .apply() method:
   - This might work in Pandas, but not PySpark DataFrames
   - PySpark uses different methods (map, transform, etc.)
   - This would raise an AttributeError

---

---

## Question 16: Databricks Secrets Management

The security team is exploring whether the Databricks secrets module can be leveraged for connecting to an external database.

After testing the code with all Python variables being defined with strings, they upload the password to the secrets module and configure the correct permissions for the currently active user. They then modify their code to the following (leaving all other variables unchanged).

```python
password = dbutils.secrets.get(scope="db_creds", key="jdbc_password")

print(password)

df = (spark
  .read
  .format("jdbc")
  .option("url", connection)
  .option("dbtable", tablename)
  .option("user", username)
  .option("password", password)
  )
```

Which statement describes what will happen when the above code is executed?

**A.** The connection to the external table will succeed; the string "REDACTED" will be printed.

**B.** An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the encoded password will be saved to DBFS.

**C.** An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the password will be printed in plain text.

**D.** The connection to the external table will succeed; the string value of password will be printed in plain text.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 7: Ensuring Data Security and Compliance**
  - Sub-topic: Applying Data Security mechanisms - Use ACLs to secure Workspace Objects.

- **Section 1: Developing Code for Data Processing using Python and SQL**
  - Sub-topic: Using Python and Tools for development - creating secure, configurable code.

---

### Important Keywords to Focus On

- **"password = dbutils.secrets.get(...)"**: Retrieving a secret from Databricks secrets store.
- **"print(password)"**: Attempting to display the secret value.
- **"REDACTED"**: Databricks automatically masks secrets in output.
- **"the connection will succeed"**: Secrets functionality doesn't prevent connections.

---

### Correct Option: A

**Explanation:**

Databricks has built-in security measures for handling secrets:

1. **Databricks Secrets Management**:
   - `dbutils.secrets.get()` retrieves a secret from the specified scope
   - The secret is successfully retrieved into the Python variable `password`
   - The connection uses this password, so it will succeed (assuming correct credentials)

2. **Automatic Secret Redaction**:
   - Databricks automatically redacts secrets in output to prevent accidental exposure
   - When `print(password)` is executed, the output shows "REDACTED" instead of the actual password value
   - This redaction applies to:
     - Console output (display())
     - Notebook cell results
     - Logs displayed to users
   - This is a security feature to prevent secrets from being visible even if code is shared or reviewed

3. **Why the Connection Succeeds**:
   - The actual password value is used in the JDBC connection
   - Only the displayed output is redacted
   - The underlying data value is correct; only the human-visible representation is masked

4. **Security Implications**:
   - Even if malicious code tries to print secrets, they're automatically redacted
   - This prevents accidental exposure through logs, notebooks, or output
   - The actual secret value is still used for connections and operations

---

### Why Other Options are Incorrect

- **B. Interactive input box appears**: There's no reason for an input box. The secret is already stored; it just needs to be retrieved.

- **C. Password printed in plain text**: This would be a security issue. Databricks specifically prevents this by redacting secrets in output.

- **D. Password printed in plain text**: Same issue as C. Databricks redaction ensures this doesn't happen.

---

---

## Question 17: GDPR Compliance and Time Travel Access

The data governance team is reviewing code used for deleting records for compliance with GDPR. The following logic has been implemented to propagate delete requests from the user_lookup table to the user_aggregates table.

```sql
DELETE FROM user_aggregates
WHERE user_id NOT IN (SELECT user_id FROM user_lookup)
```

Assuming that user_id is a unique identifying key and that all users that have requested deletion have been removed from the user_lookup table, which statement describes whether successfully executing the above logic guarantees that the records to be deleted from the user_aggregates table are no longer accessible and why?

**A.** No; the Delta Lake DELETE command only provides ACID guarantees when combined with the MERGE INTO command.

**B.** No; files containing deleted records may still be accessible with time travel until a VACUUM command is used to remove invalidated data files.

**C.** Yes; the change data feed uses foreign keys to ensure delete consistency throughout the Lakehouse.

**D.** Yes; Delta Lake ACID guarantees provide assurance that the DELETE command succeeded fully and permanently purged these records.

**E.** No; the change data feed only tracks inserts and updates, not deleted records.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 7: Ensuring Data Security and Compliance**
  - Sub-topic: Ensuring Compliance - Develop a data purging solution ensuring compliance with data retention policies.

---

### Important Keywords to Focus On

- **"successfully executing the above logic"**: The DELETE command runs without errors.
- **"no longer accessible"**: Not just logically deleted, but truly inaccessible.
- **"time travel"**: Delta Lake's ability to query previous versions.
- **"VACUUM command"**: Physically removes data files from storage.
- **"ACID guarantees"**: Ensures atomicity, consistency, isolation, durability.

---

### Correct Option: B

**Explanation:**

This question tests understanding of the distinction between logical deletion and physical deletion in Delta Lake:

1. **What DELETE Does**:
   - The DELETE statement marks rows as deleted in the transaction log
   - This is logically complete; queries no longer see the deleted rows
   - The underlying Parquet files are NOT immediately removed from storage
   - ACID guarantees apply to the deletion operation, not to physical file removal

2. **Time Travel and Deleted Records**:
   - Using `SELECT * FROM user_aggregates@v{previous_version}`, you can query the deleted records
   - The data exists in historical Parquet files
   - Time travel allows you to "undo" the deletion by querying a previous version
   - This works until VACUUM removes the old files

3. **GDPR Compliance Challenge**:
   - GDPR requires that deleted data be "no longer accessible"
   - Simply marking records as deleted (via DELETE) doesn't meet this requirement
   - The records are still accessible via time travel
   - VACUUM must be run to physically delete the Parquet files

4. **The Complete Timeline**:
   - DELETE statement executed: Records logically deleted
   - Time travel still allows access to previous versions
   - VACUUM command executed (must wait for retention period): Parquet files physically removed
   - After VACUUM: Data is truly no longer accessible

5. **GDPR Implication**:
   - For true GDPR compliance, both DELETE and VACUUM must be executed
   - Just executing DELETE is insufficient
   - This must be part of the compliance process

---

### Why Other Options are Incorrect

- **A. DELETE only works with MERGE**: This is false. DELETE is a standalone command that provides full ACID guarantees. MERGE is optional; DELETE works independently.

- **C. CDF uses foreign keys to ensure delete consistency**: CDF doesn't use foreign keys. Additionally, CDF doesn't prevent time travel access to deleted data.

- **D. ACID guarantees permanently purge records**: ACID guarantees apply to the DELETE operation itself, not to physical file deletion. Permanence requires VACUUM.

- **E. CDF only tracks inserts and updates**: Actually, CDF does track deletes, but this is irrelevant. CDF doesn't prevent time travel access; only VACUUM does.

---

---

## Question 18: Testing Best Practices in Data Engineering

You are testing a collection of mathematical functions, one of which calculates the area under a curve as described by another function.

```python
assert(myIntegrate(lambda x: x*x, 0, 3) [0] == 9)
```

Which kind of test would the above line exemplify?

**A.** Unit

**B.** Manual

**C.** Functional

**D.** Integration

**E.** End-to-end

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 1: Developing Code for Data Processing using Python and SQL**
  - Sub-topic: Develop unit and integration tests using assertDataFrameEqual, assertSchemaEqual, DataFrame.transform, and testing frameworks.

---

### Important Keywords to Focus On

- **"testing a collection of mathematical functions"**: Single function, isolated test.
- **"one of which calculates"**: Testing a specific, individual function.
- **"assert statement"**: Direct verification of behavior.
- **"myIntegrate(lambda x: x*x, 0, 3)"**: Function with specific inputs and expected output.

---

### Correct Option: A

**Explanation:**

Understanding different types of tests is fundamental to software quality assurance:

1. **Unit Test Characteristics**:
   - Tests a single, isolated unit of code (one function)
   - Doesn't depend on external systems or services
   - Fast to execute
   - Uses assertions to verify expected behavior
   - Simple and focused on one behavior

2. **Why This is a Unit Test**:
   - Tests only the `myIntegrate` function
   - Doesn't test interactions with other systems
   - Uses a direct assert statement
   - Input (lambda function and bounds) is self-contained
   - Expected output (9) is verified directly

3. **Test Analysis**:
   - Input: function (x²), start=0, end=3
   - Expected result: Area under curve = 9 (∫x² dx from 0 to 3 = [x³/3] from 0 to 3 = 9)
   - Assertion: `result[0] == 9`
   - This is testing one specific calculation in isolation

---

### Why Other Options are Incorrect

- **B. Manual**: Manual testing involves human interaction and verification, not automated assertions. This is automated.

- **C. Functional**: Functional tests verify that a system meets its functional requirements, often testing multiple components. This is a single function.

- **D. Integration**: Integration tests verify that multiple components work together. This tests only one function in isolation.

- **E. End-to-end**: End-to-end tests verify entire workflows and user journeys. This is a single mathematical function test.

---

---

## Question 19: Optimizing Write Performance with Shuffle

Which statement describes Delta Lake optimized writes?

**A.** Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.

**B.** An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.

**C.** A shuffle occurs prior to writing to try to group similar data together resulting in fewer files instead of each executor writing multiple files based on directory partitions.

**D.** Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 6: Cost & Performance Optimisation**
  - Sub-topic: Understand delta optimization techniques, such as deletion vectors and liquid clustering.

---

### Important Keywords to Focus On

- **"optimized writes"**: Delta Lake feature for improving write efficiency.
- **"shuffle occurs prior to writing"**: Data reorganization before write.
- **"group similar data together"**: Co-locating related data.
- **"fewer files instead of each executor writing multiple files"**: Output file consolidation.

---

### Correct Option: C

**Explanation:**

Delta Lake optimized writes improve efficiency by reducing the number of output files:

1. **The Problem Being Solved**:
   - With default Spark writes, each executor independently writes data files
   - If you have 100 executors, you might get 100 files per write
   - Many small files cause:
     - Slower reads (more metadata to process)
     - Higher storage overhead
     - Performance degradation

2. **How Optimized Writes Work**:
   - A shuffle operation occurs before writing
   - Shuffle groups similar/related data together on the same partition
   - Data destined for the same partition goes to the same executor
   - Result: Fewer, larger files instead of many small files

3. **Benefits**:
   - Faster reads (fewer files to scan)
   - Better compression (more data per file)
   - Reduced metadata overhead
   - More efficient downstream processing

4. **Configuration**:
   - Optimized writes can be enabled with `spark.databricks.delta.optimizeWrite.enabled = true`
   - Works automatically when enabled

---

### Why Other Options are Incorrect

- **A. Before cluster terminates, OPTIMIZE executes**: This describes Auto-Compaction, not Optimized Writes. These are separate features.

- **B. Asynchronous compaction toward 1 GB**: This describes Auto-Compaction with a specific target size. Optimized Writes are synchronous (happen before write).

- **D. Logical vs. directory partitions**: This is conceptually confused. Optimized Writes don't change how partitioning works; they reduce the number of output files within partitions.

---

---

## Question 20: Auto Compaction vs. Optimized Writes

Which statement describes Delta Lake Auto Compaction?

**A.** Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.

**B.** An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.

**C.** Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.

**D.** Data is queued in a messaging bus instead of committing data directly to memory; all data is committed from the messaging bus in one batch once the job is complete.

**E.** An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 128 MB.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 6: Cost & Performance Optimisation**
  - Sub-topic: Understand delta optimization techniques, such as deletion vectors and liquid clustering.

---

### Important Keywords to Focus On

- **"Auto Compaction"**: Delta Lake feature for automatic file consolidation.
- **"asynchronous job runs after write completes"**: Post-write operation.
- **"detect if files could be further compacted"**: Checks for optimization opportunity.
- **"OPTIMIZE job is executed"**: Runs the OPTIMIZE command automatically.
- **"default of 128 MB"**: Target file size for Auto-Compaction.

---

### Correct Option: E

**Explanation:**

Auto-Compaction automatically optimizes Delta tables after writes:

1. **How Auto-Compaction Works**:
   - After a write operation completes successfully
   - An asynchronous background job analyzes the files written
   - Determines if additional compaction would improve performance
   - If beneficial, runs the OPTIMIZE command automatically
   - Target file size is **128 MB** by default

2. **Key Characteristics**:
   - **Asynchronous**: Doesn't block the write operation; happens in the background
   - **Automatic**: No manual intervention required
   - **Conservative**: Only runs if it detects a benefit
   - **Target Size**: 128 MB per file (compared to 1 GB for manual OPTIMIZE)

3. **Benefits**:
   - Automatically reduces small file accumulation
   - Improves query performance without manual maintenance
   - No overhead to the main ETL job (asynchronous)

4. **When It Runs**:
   - After successful Delta write operations
   - Only if the optimizer determines it would help
   - Configured automatically on Databricks clusters

---

### Why Other Options are Incorrect

- **A. Before cluster terminates**: Auto-Compaction doesn't depend on cluster termination. This might be true for some manual optimizations but not for Auto-Compaction.

- **B. Default of 1 GB**: 1 GB is too large for Auto-Compaction's default. 128 MB is the correct default.

- **C. Logical vs. directory partitions**: This describes a conceptual difference in partitioning, not Auto-Compaction.

- **D. Messaging bus and batched commit**: This describes a queuing pattern, not Auto-Compaction.

---

---

## Question 21: Implementing Data Quality Checks with CHECK Constraints

A CHECK constraint has been successfully added to the Delta table named activity_details using the following logic:

```sql
ALTER TABLE activity_details 
ADD CONSTRAINT valid_coordinates 
CHECK (latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180)
```

A batch job is attempting to insert new records to the table, including a record where latitude = 45.50 and longitude = 212.67.

Which statement describes the outcome of this batch insert?

**A.** The write will insert all records except those that violate the table constraints; the violating records will be reported in a warning log.

**B.** The write will fail completely because of the constraint violation and no records will be inserted into the target table.

**C.** The write will insert all records except those that violate the table constraints; the violating records will be recorded to a quarantine table.

**D.** The write will include all records in the target table; any violations will be indicated in the boolean column named valid_coordinates.

---

### Topic Mapping

This question aligns with the November 30, 2025 exam guide under:

- **Section 3: Data Transformation, Cleansing, and Quality**
  - Sub-topic: Implementing Data Quality rules and constraints (e.g., Delta Lake constraints).

- **Section 7: Ensuring Data Security and Compliance**
  - Sub-topic: Implementing a compliant batch & streaming pipeline that detects and applies masking of PII.

---

### Important Keywords to Focus On

- **"CHECK constraint has been successfully added"**: The constraint is active and enforced.
- **"latitude = 45.50 and longitude = 212.67"**: The 212.67 longitude violates the constraint (not between -180 and 180).
- **"batch insert"**: Multiple records, at least one violates constraints.
- **"ACID compliance"**: Delta Lake must maintain integrity.

---

### Correct Option: B

**Explanation:**

Delta Lake constraint enforcement is strict and atomic:

1. **Constraint Validation**:
   - The record has longitude = 212.67
   - The constraint requires: -180 <= longitude <= 180
   - 212.67 is outside this range, violating the constraint

2. **ACID Semantics**:
   - Delta Lake applies ACID principles strictly
   - A constraint violation means the operation cannot complete
   - The operation is atomic: either all records are inserted or none are
   - Partial writes would violate data integrity

3. **Why the Entire Batch Fails**:
   - The batch contains at least one violating record
   - The operation is treated as a single atomic transaction
   - If any record violates constraints, the entire transaction aborts
   - No records are inserted; the table remains unchanged

4. **Enforcement Strategy**:
   - Delta Lake validates all records before committing
   - If any record violates any constraint, the write fails
   - This ensures complete data integrity
   - No partial writes or warnings; it's all or nothing

---

### Why Other Options are Incorrect

- **A. Insert all except violating records with warning log**: Delta Lake doesn't have a "skip violating records" mode. Constraints are strict; violations cause complete failure.

- **C. Insert valid records to main table, violating to quarantine**: While this is a good data quality pattern, it's not how CHECK constraints work. Constraints are binary: pass or fail. Implementing quarantine requires explicit code logic, not just constraints.

- **D. Include all records with boolean indication**: Constraint violations don't add columns or flags. They cause operation failure.

---

---

## Question 22: Sample Output Summary

This comprehensive practice question set covers all major domains of the Databricks Certified Data Engineer Professional exam as of November 30, 2025. Each question includes:

1. **Full Scenario**: Real-world context and code examples
2. **Topic Mapping**: Alignment with exam guide domains and sub-topics  
3. **Important Keywords**: Key phrases to focus on when analyzing the question
4. **Detailed Explanation**: In-depth reasoning for the correct answer
5. **Why Other Options Are Incorrect**: Technical analysis of incorrect choices

---

## Quick Reference: Exam Domains Covered

### Section 1: Developing Code for Data Processing
- Questions: 1, 6, 7, 10, 14, 18
- Key Topics: Pipelines, Streaming, Testing, Language Interoperability

### Section 2: Data Ingestion & Acquisition
- Questions: 11
- Key Topics: CDC Processing, Data Quality

### Section 3: Data Transformation & Quality
- Questions: 1, 3, 4, 11, 21
- Key Topics: Transformations, Constraints, Data Validation

### Section 6: Cost & Performance Optimization
- Questions: 2, 3, 8, 12, 19, 20
- Key Topics: Data Skipping, Caching, Optimization, Compaction

### Section 7: Security & Compliance
- Questions: 4, 16, 17, 21
- Key Topics: Secrets, Deletion, GDPR, Data Quality

### Section 8: Data Governance
- Questions: 12, 5
- Key Topics: Metadata, Schema Evolution, Views

### Section 9: Debugging & Deploying
- Questions: 9
- Key Topics: Git Workflow, CI/CD, Branching

### Section 10: Data Modelling
- Questions: 3, 5, 10
- Key Topics: Partitioning, Schema Design, Efficient Models

---

## Study Tips

1. **Focus on ACID Properties**: Delta Lake guarantees are fundamental
2. **Understand Optimization**: Data skipping, caching, and compaction
3. **Master Change Data Feed**: Critical for CDC and streaming scenarios
4. **Know Constraint Behavior**: Constraints are strict and atomic
5. **Recognize Time Travel Limitations**: Time travel requires retention management
6. **Understand Checkpoint Semantics**: Critical for streaming correctness
7. **Review Security Patterns**: Secrets, masking, and compliance

---
