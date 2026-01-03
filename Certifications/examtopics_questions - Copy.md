### Question 1

An upstream system has been configured to pass the date for a given batch of data to the Databricks Jobs API as a parameter. The notebook to be scheduled will use this parameter to load data with the following code: df = spark.read.format("parquet").load(f"/mnt/source/(date)")Which code block should be used to create the date Python variable used in the above code block?

- A. date = spark.conf.get("date")
- B. input_dict = input()date= input_dict["date"]
- C. import sysdate = sys.argv[1]
- D. date = dbutils.notebooks.getParam("date")
- E. dbutils.widgets.text("date", "null")date = dbutils.widgets.get("date")

**Topic Mapping:** Section 1: Developing Code for Data Processing using Python and SQL → Create and Automate ETL workloads using Jobs via UI/APIs/CLI

**Important Keywords to Focus On:**
- "**Databricks Jobs API as a parameter**" - indicates using Jobs API to pass parameters
- "**notebook to be scheduled**" - refers to job scheduling context
- "**pass the date for a given batch**" - parameter passing mechanism

**Correct Answer:** E

**Explanation:**
When passing parameters to a Databricks notebook through the Jobs API, you need to use **dbutils.widgets** to receive those parameters. The correct approach involves two steps:
1. First, create a widget using `dbutils.widgets.text("date", "null")` - this declares a widget named "date" with a default value of "null"
2. Then, retrieve the parameter value using `dbutils.widgets.get("date")` - this fetches the actual value passed by the Jobs API

When a job is triggered via the Databricks Jobs API with parameters, the API automatically populates the widget values. This is the standard mechanism for parameterizing notebooks in job contexts.

**Why Other Options are Incorrect:**
- **A. date = spark.conf.get("date")** - `spark.conf` is used for Spark configuration properties, not for job parameters. This is for internal Spark settings like spark.sql.shuffle.partitions, not external parameter passing.
- **B. input_dict = input()date= input_dict["date"]** - The `input()` function is for interactive user input in standard Python, not applicable in automated job execution contexts. Jobs run non-interactively.
- **C. import sysdate = sys.argv[1]** - `sys.argv` is used for command-line arguments in standalone Python scripts, but Databricks notebooks don't receive job parameters through sys.argv.
- **D. date = dbutils.notebooks.getParam("date")** - While `dbutils.notebooks` exists, the correct method for retrieving job parameters is through widgets, not `getParam()`. The `dbutils.notebooks` utilities are primarily for notebook workflow orchestration (run, exit), not parameter retrieval.

---

### Question 2

The Databricks workspace administrator has configured interactive clusters for each of the data engineering groups. To control costs, clusters are set to terminate after 30 minutes of inactivity. Each user should be able to execute workloads against their assigned clusters at any time of the day.Assuming users have been added to a workspace but not granted any permissions, which of the following describes the minimal permissions a user would need to start and attach to an already configured cluster.

- A. "Can Manage" privileges on the required cluster
- B. Workspace Admin privileges, cluster creation allowed, "Can Attach To" privileges on the required cluster
- C. Cluster creation allowed, "Can Attach To" privileges on the required cluster
- D. "Can Restart" privileges on the required cluster
- E. Cluster creation allowed, "Can Restart" privileges on the required cluster

**Topic Mapping:** Section 7: Ensuring Data Security and Compliance → Use ACLs to secure Workspace Objects, enforcing the principle of least privilege

**Important Keywords to Focus On:**
- "**minimal permissions**" - indicates principle of least privilege
- "**already configured cluster**" - cluster exists, no need to create
- "**start and attach**" - requires restart capability and attachment rights
- "**terminate after 30 minutes of inactivity**" - cluster will be terminated, needs restart

**Correct Answer:** D

**Explanation:**
The "**Can Restart**" privilege is the minimal permission required for this scenario. Here's why:
- The cluster is already configured (pre-existing), so users don't need cluster creation permissions
- When a cluster terminates due to inactivity (30 minutes), it needs to be restarted before use
- "Can Restart" privilege includes the ability to both **start a terminated cluster** and **attach to it** for executing workloads
- This follows the principle of least privilege - giving only what's necessary to accomplish the task

**Why Other Options are Incorrect:**
- **A. "Can Manage" privileges** - This is excessive. "Can Manage" provides full control including modifying, deleting, and changing permissions on the cluster. It's more than what's needed for just starting and using the cluster.
- **B. Workspace Admin privileges, cluster creation allowed, "Can Attach To" privileges** - Highly excessive. Workspace Admin gives blanket permissions across the entire workspace. Users don't need admin rights or cluster creation capabilities for pre-configured clusters. "Can Attach To" alone cannot restart a terminated cluster.
- **C. Cluster creation allowed, "Can Attach To" privileges** - Users don't need cluster creation permissions since the cluster already exists. Additionally, "Can Attach To" alone doesn't grant the ability to restart a terminated cluster.
- **E. Cluster creation allowed, "Can Restart" privileges** - While "Can Restart" is correct, cluster creation permission is unnecessary since the cluster is already configured. This violates the principle of least privilege.

---

### Question 3

When scheduling Structured Streaming jobs for production, which configuration automatically recovers from query failures and keeps costs low?

- A. Cluster: New Job Cluster; Retries: Unlimited; Maximum Concurrent Runs: Unlimited
- B. Cluster: New Job Cluster; Retries: None; Maximum Concurrent Runs: 1
- C. Cluster: Existing All-Purpose Cluster; Retries: Unlimited; Maximum Concurrent Runs: 1
- D. Cluster: New Job Cluster; Retries: Unlimited; Maximum Concurrent Runs: 1
- E. Cluster: Existing All-Purpose Cluster; Retries: None; Maximum Concurrent Runs: 1

**Topic Mapping:** Section 1: Developing Code for Data Processing using Python and SQL → Create and Automate ETL workloads using Jobs via UI/APIs/CLI & Section 6: Cost & Performance Optimisation

**Important Keywords to Focus On:**
- "**Structured Streaming jobs**" - continuous processing, should not run concurrently
- "**automatically recovers from query failures**" - requires retry mechanism
- "**keeps costs low**" - cost optimization is key
- "**production**" - requires reliability and efficiency

**Correct Answer:** D

**Explanation:**
Option D is the optimal configuration for production Structured Streaming jobs:
- **New Job Cluster**: Job clusters are ephemeral and created when the job starts, terminated when finished. They cost significantly less than all-purpose clusters (approximately 50% cheaper) and are designed for automated production workloads.
- **Retries: Unlimited**: Structured Streaming jobs are designed to be long-running and continuous. If a job fails due to transient issues (network glitches, temporary resource unavailability), unlimited retries ensure automatic recovery without manual intervention. Checkpointing in Structured Streaming ensures processing continues from where it left off.
- **Maximum Concurrent Runs: 1**: Streaming jobs should never run multiple instances concurrently. Each streaming job maintains state in checkpoint locations, and concurrent runs would cause conflicts, data duplication, and state corruption. Only one instance should process the stream at any time.

**Why Other Options are Incorrect:**
- **A. Maximum Concurrent Runs: Unlimited** - This is dangerous for streaming jobs. Multiple concurrent instances would attempt to process the same stream simultaneously, leading to duplicate processing, checkpoint conflicts, and data inconsistency.
- **B. Retries: None** - Without retries, any transient failure would permanently stop the streaming job, requiring manual intervention. This doesn't meet the "automatically recovers" requirement.
- **C. Existing All-Purpose Cluster; Retries: Unlimited; Maximum Concurrent Runs: 1** - All-purpose clusters are significantly more expensive (about 2x the cost of job clusters) and designed for interactive use, not automated jobs. This violates the "keeps costs low" requirement.
- **E. Existing All-Purpose Cluster; Retries: None** - Combines two problems: expensive all-purpose cluster and no retry mechanism. Doesn't meet either the cost or automatic recovery requirements.

---

### Question 4

![Question 4 Image 1](examtopics_images/question_4_img_1.png)

The data engineering team has configured a Databricks SQL query and alert to monitor the values in a Delta Lake table. The recent_sensor_recordings table contains an identifying sensor_id alongside the timestamp and temperature for the most recent 5 minutes of recordings.The below query is used to create the alert:The query is set to refresh each minute and always completes in less than 10 seconds. The alert is set to trigger when mean (temperature) > 120. Notifications are triggered to be sent at most every 1 minute.If this alert raises notifications for 3 consecutive minutes and then stops, which statement must be true?

- A. The total average temperature across all sensors exceeded 120 on three consecutive executions of the query
- B. The recent_sensor_recordings table was unresponsive for three consecutive runs of the query
- C. The source query failed to update properly for three consecutive minutes and then restarted
- D. The maximum temperature recording for at least one sensor exceeded 120 on three consecutive executions of the query
- E. The average temperature recordings for at least one sensor exceeded 120 on three consecutive executions of the query

**Topic Mapping:** Section 5: Monitoring and Alerting → Use SQL Alerts to monitor data quality

**Important Keywords to Focus On:**
- "**mean(temperature) > 120**" - alert trigger condition on average temperature
- "**GROUP BY sensor_id**" - aggregation per sensor (from image)
- "**at least one sensor**" - the grouping means per-sensor averages
- "**3 consecutive minutes and then stops**" - condition was met 3 times, then resolved

**Correct Answer:** E

**Explanation:**
Based on the query structure shown in the image, the query groups data by `sensor_id` and calculates `mean(temperature)` for each sensor. The alert is configured to trigger when `mean(temperature) > 120`. This means:
- The query returns one row per sensor with its average temperature
- The alert evaluates each row independently
- If **any sensor** has an average temperature > 120, the alert triggers
- For the alert to fire for 3 consecutive minutes, at least one sensor must have maintained an average temperature > 120 during those 3 query executions
- After 3 minutes, the alert stopped, meaning no sensor's average temperature exceeded 120 anymore

The key insight is that with `GROUP BY sensor_id`, the alert checks per-sensor averages, not a global average across all sensors.

**Why Other Options are Incorrect:**
- **A. The total average temperature across all sensors** - The query uses `GROUP BY sensor_id`, so it calculates per-sensor averages, not a single total average across all sensors. The alert evaluates each sensor's average independently.
- **B. The table was unresponsive** - If the table were unresponsive, the query would fail or return no results, likely not triggering the alert in the expected manner. The question states the query "always completes in less than 10 seconds."
- **C. The source query failed to update** - Query failures wouldn't consistently trigger the alert based on the temperature threshold. Failed queries typically don't produce results that would meet the `mean(temperature) > 120` condition.
- **D. The maximum temperature recording** - The alert condition is on `mean(temperature)`, not `max(temperature)`. Maximum and average are different aggregations. A maximum could exceed 120 while the average doesn't, or vice versa.

---

### Question 5

A junior developer complains that the code in their notebook isn't producing the correct results in the development environment. A shared screenshot reveals that while they're using a notebook versioned with Databricks Repos, they're using a personal branch that contains old logic. The desired branch named dev-2.3.9 is not available from the branch selection dropdown.Which approach will allow this developer to review the current logic for this notebook?

- A. Use Repos to make a pull request use the Databricks REST API to update the current branch to dev-2.3.9
- B. Use Repos to pull changes from the remote Git repository and select the dev-2.3.9 branch.
- C. Use Repos to checkout the dev-2.3.9 branch and auto-resolve conflicts with the current branch
- D. Merge all changes back to the main branch in the remote Git repository and clone the repo again
- E. Use Repos to merge the current branch and the dev-2.3.9 branch, then make a pull request to sync with the remote repository

**Topic Mapping:** Section 9: Debugging and Deploying → Configure and integrate with Git-based CI/CD workflows using Databricks Git Folders for notebook and code deployment

**Important Keywords to Focus On:**
- "**dev-2.3.9 is not available from the branch selection dropdown**" - branch doesn't exist locally
- "**personal branch that contains old logic**" - currently on wrong/outdated branch
- "**review the current logic**" - need to access the correct branch
- "**Databricks Repos**" - Git integration in Databricks

**Correct Answer:** B

**Explanation:**
When a branch exists in the remote Git repository but doesn't appear in the local Databricks Repos dropdown, it means the local repository hasn't synced with the remote. The solution is to:
1. **Pull changes from the remote Git repository** - This fetches all new branches and updates from the remote repository
2. **Select the dev-2.3.9 branch** - Once pulled, the branch will appear in the dropdown and can be selected

This is the standard Git workflow: `git fetch` (or pull) to get remote branches, then `git checkout` to switch to the desired branch. In Databricks Repos, the "Pull" operation synchronizes with the remote and makes all remote branches available for checkout.

**Why Other Options are Incorrect:**
- **A. Make a pull request and use REST API to update branch** - Pull requests are for merging code between branches, not for accessing existing branches. You don't need the REST API to simply switch branches. This is overly complex for the task.
- **C. Checkout dev-2.3.9 and auto-resolve conflicts** - You cannot checkout a branch that doesn't exist in your local repository. The branch needs to be fetched first. Also, simply switching branches doesn't cause conflicts; conflicts occur during merges, not checkouts.
- **D. Merge to main and clone again** - This is unnecessarily destructive and time-consuming. There's no reason to merge the old logic to main or clone the entire repository again just to access a different branch.
- **E. Merge current branch with dev-2.3.9** - You cannot merge with a branch that doesn't exist in your local repository. Additionally, merging would combine the old logic with new logic, which isn't the goal. The developer just wants to view the current logic on dev-2.3.9, not merge branches.

---

### Question 6

![Question 6 Image 1](examtopics_images/question_6_img_1.png)

The security team is exploring whether or not the Databricks secrets module can be leveraged for connecting to an external database.After testing the code with all Python variables being defined with strings, they upload the password to the secrets module and configure the correct permissions for the currently active user. They then modify their code to the following (leaving all other variables unchanged).Which statement describes what will happen when the above code is executed?

- A. The connection to the external table will fail; the string "REDACTED" will be printed.
- B. An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the encoded password will be saved to DBFS.
- C. An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the password will be printed in plain text.
- D. The connection to the external table will succeed; the string value of password will be printed in plain text.
- E. The connection to the external table will succeed; the string "REDACTED" will be printed.

**Topic Mapping:** Section 7: Ensuring Data Security and Compliance → Applying Data Security mechanisms

**Important Keywords to Focus On:**
- "**Databricks secrets module**" - dbutils.secrets for secure credential management
- "**upload the password to secrets module**" - password stored securely
- "**configure the correct permissions**" - user has access to retrieve secret
- "**dbutils.secrets.get()**" - method to retrieve secrets (from image)

**Correct Answer:** E

**Explanation:**
Databricks secrets module (`dbutils.secrets`) is specifically designed for secure credential management:
- When you call `dbutils.secrets.get(scope, key)`, it retrieves the actual secret value and returns it as a string
- The **connection will succeed** because the actual password value is retrieved and passed to the database connection
- For **security purposes**, when secrets are printed or displayed in notebooks, Databricks automatically **redacts them** and shows "**REDACTED**" instead of the actual value
- This redaction happens at the notebook display level, not in the actual variable value, so the connection libraries can still use the real password

This design allows secure use of credentials in code while preventing accidental exposure in notebook outputs, logs, or error messages.

**Why Other Options are Incorrect:**
- **A. Connection will fail** - The connection will succeed because `dbutils.secrets.get()` retrieves the actual password value (not the string "REDACTED"). The actual password is used for authentication, and redaction only applies to display.
- **B. Interactive input box will appear** - `dbutils.secrets.get()` doesn't create interactive prompts. It directly retrieves stored secrets. Interactive input would be `dbutils.widgets` or Python's `input()` function.
- **C. Interactive input box and plain text password** - Double incorrect: no input box appears, and passwords are never printed in plain text when using secrets.
- **D. Password printed in plain text** - Databricks automatically redacts secret values when they're displayed in notebook outputs. This security feature prevents credential exposure in logs, notebook outputs, or shared notebooks.

---

### Question 7

![Question 7 Image 1](examtopics_images/question_7_img_1.png)

The data science team has created and logged a production model using MLflow. The following code correctly imports and applies the production model to output the predictions as a new DataFrame named preds with the schema "customer_id LONG, predictions DOUBLE, date DATE".The data science team would like predictions saved to a Delta Lake table with the ability to compare all predictions across time. Churn predictions will be made at most once per day.Which code block accomplishes this task while minimizing potential compute costs?

- A. preds.write.mode("append").saveAsTable("churn_preds")
- B. preds.write.format("delta").save("/preds/churn_preds")
- C. (Option C from image)
- D. (Option D from image)
- E. (Option E from image)

**Topic Mapping:** Section 2: Data Ingestion & Acquisition → Create an append-only data pipeline & Section 6: Cost & Performance Optimisation

**Important Keywords to Focus On:**
- "**compare all predictions across time**" - need historical tracking, append mode
- "**at most once per day**" - batch operation, not frequent updates
- "**minimizing potential compute costs**" - avoid expensive operations like full table scans
- "**date DATE**" - schema includes date field for time-based queries

**Correct Answer:** A

**Explanation:**
Option A (`preds.write.mode("append").saveAsTable("churn_preds")`) is the most cost-effective solution:
- **Append mode** adds new predictions without scanning existing data, which minimizes compute costs. Each day's predictions are simply appended to the table.
- **saveAsTable()** creates a managed Delta table with proper metadata registration, enabling easy querying and time-based comparisons using the `date` field
- Since predictions are made **once per day**, there's no risk of duplicates within a single execution
- The schema includes a `date` field, allowing queries like `WHERE date = '2024-01-01'` to compare predictions across different days
- No deduplication or merge logic is needed since each day produces unique records

This approach is simple, performant, and cost-effective for this use case.

**Why Other Options are Incorrect:**
- **B. preds.write.format("delta").save("/preds/churn_preds")** - While this creates a Delta table, using `save()` with a path creates an unmanaged table. This is less optimal than `saveAsTable()` because:
  - Metadata isn't registered in the metastore by default
  - Harder to query and manage
  - No automatic table name reference
  Option A is simpler and provides better metadata management.

- **C, D, E**: Without seeing the image, based on typical Delta Lake patterns, these options likely involve:
  - **MERGE operations**: Unnecessarily expensive for this use case. MERGE requires scanning existing data to check for matches, which is costly when you're just appending daily predictions with no duplicates.
  - **Overwrite mode**: Would lose historical predictions, violating the "compare all predictions across time" requirement.
  - **Complex deduplication logic**: Unnecessary overhead when predictions are made only once per day, adding compute costs without benefit.

---

### Question 8

![Question 8 Image 1](examtopics_images/question_8_img_1.png)

An upstream source writes Parquet data as hourly batches to directories named with the current date. A nightly batch job runs the following code to ingest all data from the previous day as indicated by the date variable:Assume that the fields customer_id and order_id serve as a composite key to uniquely identify each order.If the upstream system is known to occasionally produce duplicate entries for a single order hours apart, which statement is correct?

- A. Each write to the orders table will only contain unique records, and only those records without duplicates in the target table will be written.
- B. Each write to the orders table will only contain unique records, but newly written records may have duplicates already present in the target table.
- C. Each write to the orders table will only contain unique records; if existing records with the same key are present in the target table, these records will be overwritten.
- D. Each write to the orders table will only contain unique records; if existing records with the same key are present in the target table, the operation will fail.
- E. Each write to the orders table will run deduplication over the union of new and existing records, ensuring no duplicate records are present.

**Topic Mapping:** Section 3: Data Transformation, Cleansing, and Quality → Write efficient Spark SQL and PySpark code to apply advanced data transformations

**Important Keywords to Focus On:**
- "**dropDuplicates()**" (from image) - deduplication within the batch
- "**customer_id and order_id serve as a composite key**" - uniqueness criteria
- "**duplicate entries for a single order hours apart**" - duplicates can exist across time/batches
- "**nightly batch job**" - processes one day at a time

**Correct Answer:** B

**Explanation:**
Based on the code shown in the image using `dropDuplicates()` before writing:
- **dropDuplicates()** removes duplicate records **within the current DataFrame/batch** being processed (same day's data)
- Each write operation will only contain unique records for that day
- However, `dropDuplicates()` only operates on the current batch of data being written, **not on the existing data in the target table**
- If the same order was written yesterday and appears again today (after deduplication), **both records will exist in the table** because:
  - Yesterday's write created a record
  - Today's deduplication only checks today's batch
  - Today's write (append mode) adds the record again without checking existing table data

This is the limitation of using `dropDuplicates()` with append mode - it prevents duplicates within each batch but doesn't prevent duplicates across batches.

**Why Other Options are Incorrect:**
- **A. Only records without duplicates in target table will be written** - `dropDuplicates()` doesn't check the target table; it only deduplicates within the current DataFrame. It has no awareness of existing table data.
- **C. Existing records will be overwritten** - Append mode doesn't overwrite anything. It simply adds new records. Overwriting would require merge/upsert logic or overwrite mode.
- **D. Operation will fail if duplicates exist** - Append operations don't fail due to duplicate keys. Delta Lake allows duplicate records unless you explicitly use MERGE with unique key constraints or apply additional deduplication logic.
- **E. Deduplication over union of new and existing records** - This would require reading the existing table data, unioning with new data, and then deduplicating. The code shown only uses `dropDuplicates()` on the incoming batch, not on a union with existing table data. This would be achieved with a MERGE statement or explicit union + deduplication logic.

---
### Question 9

![Question 9 Image 1](examtopics_images/question_9_img_1.png)

A junior member of the data engineering team is exploring the language interoperability of Databricks notebooks. The intended outcome of the below code is to register a view of all sales that occurred in countries on the continent of Africa that appear in the geo_lookup table.Before executing the code, running SHOW TABLES on the current database indicates the database contains only two tables: geo_lookup and sales.Which statement correctly describes the outcome of executing these command cells in order in an interactive notebook?

- A. Both commands will succeed. Executing show tables will show that countries_af and sales_af have been registered as views.
- B. Cmd 1 will succeed. Cmd 2 will search all accessible databases for a table or view named countries_af: if this entity exists, Cmd 2 will succeed.
- C. Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable representing a PySpark DataFrame.
- D. Both commands will fail. No new variables, tables, or views will be created.
- E. Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable containing a list of strings.

**Topic Mapping:** Section 1: Developing Code for Data Processing using Python and SQL → Building and Testing an ETL pipeline with SQL and Apache Spark on the Databricks Platform

**Important Keywords to Focus On:**
- "**language interoperability**" - mixing Python and SQL in notebook
- "**Cmd 1**" (Python) creates countries_af
- "**Cmd 2**" (SQL) tries to use countries_af
- "**register a view**" - SQL view creation intent

**Correct Answer:** C

**Explanation:**
This question tests understanding of language interoperability in Databricks notebooks:

**Cmd 1** (Python cell):
- Creates a PySpark DataFrame filtered from the geo_lookup table
- `countries_af` exists only as a **Python variable** in the Python namespace
- It's a DataFrame object, not a registered SQL view or temp table

**Cmd 2** (SQL cell):
- SQL tries to reference `countries_af` as if it were a table or view
- **SQL cells cannot directly access Python variables** unless they're registered as temp views using `createOrReplaceTempView()` or `createOrReplaceGlobalTempView()`
- Since `countries_af` is just a Python variable (not registered), SQL cannot find it
- **Cmd 2 will fail** with an error like "Table or view not found: countries_af"

To make this work, Cmd 1 would need to include: `countries_af.createOrReplaceTempView("countries_af")`

**Why Other Options are Incorrect:**
- **A. Both commands succeed and views are registered** - Python variable assignment doesn't automatically register SQL views. Explicit registration using `createOrReplaceTempView()` is required.
- **B. Cmd 2 searches all databases** - SQL will search accessible databases, but `countries_af` doesn't exist as a table or view in any database - it's only a Python variable. The search will fail.
- **D. Both commands fail** - Cmd 1 will succeed because it's valid PySpark code that creates a DataFrame from an existing table. Only Cmd 2 fails.
- **E. countries_af is a list of strings** - The code uses `spark.table("geo_lookup").filter(...)`, which returns a PySpark DataFrame, not a list of strings. It's a DataFrame object with rows and columns.

---

### Question 10

A Delta table of weather records is partitioned by date and has the below schema: date DATE, device_id INT, temp FLOAT, latitude FLOAT, longitude FLOATTo find all the records from within the Arctic Circle, you execute a query with the below filter: latitude > 66.3Which statement describes how the Delta engine identifies which files to load?

- A. All records are cached to an operational database and then the filter is applied
- B. The Parquet file footers are scanned for min and max statistics for the latitude column
- C. All records are cached to attached storage and then the filter is applied
- D. The Delta log is scanned for min and max statistics for the latitude column
- E. The Hive metastore is scanned for min and max statistics for the latitude column

**Topic Mapping:** Section 6: Cost & Performance Optimisation → Understand the optimization techniques used by Databricks to ensure the performance of queries on large datasets (data skipping, file pruning, etc.)

**Important Keywords to Focus On:**
- "**Delta table**" - Delta Lake format with transaction log
- "**latitude > 66.3**" - filter on non-partition column
- "**identifies which files to load**" - file pruning/data skipping mechanism
- "**min and max statistics**" - data skipping uses statistics

**Correct Answer:** D

**Explanation:**
Delta Lake uses **data skipping** as a key optimization technique:
- The **Delta transaction log** (_delta_log directory) maintains metadata about all data files in the table
- For each data file, Delta Lake automatically collects and stores **min/max statistics** for each column (first 32 columns)
- When executing the query `latitude > 66.3`, the Delta engine:
  1. Reads the Delta log (very fast, just JSON files)
  2. Checks the min/max statistics for the latitude column in each file
  3. **Skips entire files** where max(latitude) <= 66.3 (no records could match)
  4. Only loads files where max(latitude) > 66.3 (potential matches exist)
- This dramatically reduces I/O by avoiding reading irrelevant files

This is called "data skipping" or "file pruning" and is one of Delta Lake's core performance optimizations.

**Why Other Options are Incorrect:**
- **A. Cached to operational database** - Delta Lake doesn't use external operational databases. It reads directly from cloud storage (S3, ADLS, etc.) using the Delta protocol.
- **B. Parquet file footers are scanned** - While Delta Lake uses Parquet format internally, it doesn't scan individual Parquet footers for file selection. This would be too slow. Delta Lake maintains centralized statistics in the transaction log for faster lookup.
- **C. Cached to attached storage** - Delta Lake doesn't cache all records before filtering. That would defeat the purpose of optimization. Data skipping happens before loading data, preventing unnecessary I/O.
- **E. Hive metastore scanned for statistics** - The Hive metastore stores table metadata (schema, location) but doesn't maintain per-file min/max statistics. Delta Lake manages its own statistics in the transaction log, independent of the metastore.

---

### Question 11

The data engineering team has configured a job to process customer requests to be forgotten (have their data deleted). All user data that needs to be deleted is stored in Delta Lake tables using default table settings.The team has decided to process all deletions from the previous week as a batch job at 1am each Sunday. The total duration of this job is less than one hour. Every Monday at 3am, a batch job executes a series of VACUUM commands on all Delta Lake tables throughout the organization.The compliance officer has recently learned about Delta Lake's time travel functionality. They are concerned that this might allow continued access to deleted data.Assuming all delete logic is correctly implemented, which statement correctly addresses this concern?

- A. Because the VACUUM command permanently deletes all files containing deleted records, deleted records may be accessible with time travel for around 24 hours.
- B. Because the default data retention threshold is 24 hours, data files containing deleted records will be retained until the VACUUM job is run the following day.
- C. Because Delta Lake time travel provides full access to the entire history of a table, deleted records can always be recreated by users with full admin privileges.
- D. Because Delta Lake's delete statements have ACID guarantees, deleted records will be permanently purged from all storage systems as soon as a delete job completes.
- E. Because the default data retention threshold is 7 days, data files containing deleted records will be retained until the VACUUM job is run 8 days later.

**Topic Mapping:** Section 7: Ensuring Data Security and Compliance → Develop a data purging solution ensuring compliance with data retention policies & Section 6: Cost & Performance Optimisation → Understand delta optimization techniques

**Important Keywords to Focus On:**
- "**VACUUM commands**" - physically removes old data files
- "**default table settings**" - default retention is 7 days
- "**deletions at 1am Sunday, VACUUM at 3am Monday**" - ~26 hour gap
- "**time travel functionality**" - concern about accessing deleted data

**Correct Answer:** E

**Explanation:**
Understanding Delta Lake's data retention and VACUUM behavior:
- **Default retention threshold: 7 days** - By default, Delta Lake retains data files for 7 days before VACUUM can delete them
- When DELETE operations run (Sunday 1am), they mark records as deleted in the transaction log, but **physical data files remain on storage**
- VACUUM command (Monday 3am) can only remove files that are older than the retention threshold
- Files containing deleted records from Sunday will be retained for **7 days** after they become obsolete
- The VACUUM job running Monday cannot delete Sunday's files - they must wait 7 days
- Therefore, deleted records remain accessible via time travel for **7 days**, until the VACUUM job runs **8 days later** (7-day retention + next VACUUM run)

This is by design to support time travel and data recovery scenarios.

**Why Other Options are Incorrect:**
- **A. 24 hours retention** - The default retention threshold is 7 days, not 24 hours. Files are accessible for approximately 7 days (plus the time until the next VACUUM run).
- **B. Default is 24 hours** - Incorrect. The default `delta.deletedFileRetentionDuration` is 7 days (168 hours), not 24 hours.
- **C. Deleted records can always be recreated** - VACUUM permanently removes old data files from storage after the retention period. Once VACUUMed, time travel cannot access those versions. Time travel only works within the retention window.
- **D. Permanently purged as soon as delete completes** - DELETE operations only update the transaction log; they don't immediately remove physical files. Files remain until VACUUM runs AND the retention threshold has passed. This allows time travel and recovery within the retention period.

---

### Question 12

![Question 12 Image 1](examtopics_images/question_12_img_1.png)

A junior data engineer has configured a workload that posts the following JSON to the Databricks REST API endpoint 2.0/jobs/create.Assuming that all configurations and referenced resources are available, which statement describes the result of executing this workload three times?

- A. Three new jobs named "Ingest new data" will be defined in the workspace, and they will each run once daily.
- B. The logic defined in the referenced notebook will be executed three times on new clusters with the configurations of the provided cluster ID.
- C. Three new jobs named "Ingest new data" will be defined in the workspace, but no jobs will be executed.
- D. One new job named "Ingest new data" will be defined in the workspace, but it will not be executed.
- E. The logic defined in the referenced notebook will be executed three times on the referenced existing all purpose cluster.

**Topic Mapping:** Section 1: Developing Code for Data Processing using Python and SQL → Create and Automate ETL workloads using Jobs via UI/APIs/CLI

**Important Keywords to Focus On:**
- "**2.0/jobs/create endpoint**" - creates job definition, doesn't run it
- "**executing this workload three times**" - API called 3 times
- "**schedule configured**" (from image) - likely has schedule defined
- No "**run_now**" or immediate execution trigger

**Correct Answer:** C

**Explanation:**
The `2.0/jobs/create` API endpoint is used to **define job configurations**, not execute them:
- Each call to `/jobs/create` creates a **new job definition** in the workspace
- Executing the API **three times** will create **three separate jobs**, each named "Ingest new data"
- The API only **registers** the job configuration (notebook path, cluster settings, schedule, etc.)
- **No immediate execution occurs** when calling `/jobs/create` - it only creates the job definition
- Jobs will only run when:
  - Their configured schedule triggers them
  - Someone manually clicks "Run Now" in the UI
  - The `/jobs/run-now` API is called

Since only the job definition is being created (three times), three jobs are defined but none are executed.

**Why Other Options are Incorrect:**
- **A. Jobs will run once daily** - While the jobs might have a daily schedule configured, calling `/jobs/create` doesn't trigger execution. The jobs are only defined, not started. They would only run according to their schedule after creation.
- **B. Notebook executed three times on new clusters** - The `/jobs/create` endpoint doesn't execute anything; it only creates job definitions. To execute, you'd need `/jobs/run-now` or wait for the schedule.
- **D. One job created** - Calling the API three times creates three separate jobs. Each API call creates a new job entity. There's no deduplication based on job name.
- **E. Executed on existing all-purpose cluster** - Two issues: First, `/jobs/create` doesn't execute jobs. Second, based on typical job configurations, it likely specifies a job cluster (new cluster for each run), not an existing all-purpose cluster.

---

### Question 13

An upstream system is emitting change data capture (CDC) logs that are being written to a cloud object storage directory. Each record in the log indicates the change type (insert, update, or delete) and the values for each field after the change. The source table has a primary key identified by the field pk_id.For auditing purposes, the data governance team wishes to maintain a full record of all values that have ever been valid in the source system. For analytical purposes, only the most recent value for each record needs to be recorded. The Databricks job to ingest these records occurs once per hour, but each individual record may have changed multiple times over the course of an hour.Which solution meets these requirements?

- A. Create a separate history table for each pk_id resolve the current state of the table by running a union all filtering the history tables for the most recent state.
- B. Use MERGE INTO to insert, update, or delete the most recent entry for each pk_id into a bronze table, then propagate all changes throughout the system.
- C. Iterate through an ordered set of changes to the table, applying each in turn; rely on Delta Lake's versioning ability to create an audit log.
- D. Use Delta Lake's change data feed to automatically process CDC data from an external system, propagating all changes to all dependent tables in the Lakehouse.
- E. Ingest all log information into a bronze table; use MERGE INTO to insert, update, or delete the most recent entry for each pk_id into a silver table to recreate the current table state.

**Topic Mapping:** Section 1: Developing Code for Data Processing using Python and SQL → Use APPLY CHANGES APIs to simplify CDC in Lakeflow Spark Declarative Pipelines & Section 2: Data Ingestion & Acquisition → Create an append-only data pipeline

**Important Keywords to Focus On:**
- "**maintain a full record of all values that have ever been valid**" - complete audit trail needed
- "**only the most recent value for each record**" - current state for analytics
- "**auditing purposes**" vs "**analytical purposes**" - two different requirements
- "**changed multiple times over the course of an hour**" - multiple changes per batch

**Correct Answer:** E

**Explanation:**
This solution implements a **medallion architecture** that satisfies both requirements:

**Bronze Table (Audit Trail)**:
- Ingest **all CDC log records** in append-only mode
- Maintains **complete history** of every change ever made
- Satisfies the auditing requirement: "full record of all values that have ever been valid"
- Each CDC record (insert/update/delete with timestamp) is preserved forever

**Silver Table (Current State)**:
- Use **MERGE INTO** to apply CDC logic based on pk_id
- Handles inserts, updates, and deletes to maintain current state
- Only stores the **most recent value** for each pk_id
- Satisfies analytical requirement: "only the most recent value for each record"
- If a record changed 5 times in an hour, only the final state is in silver

This is the standard pattern for CDC processing in medallion architecture.

**Why Other Options are Incorrect:**
- **A. Separate history table for each pk_id** - This is extremely inefficient and doesn't scale. With millions of pk_ids, you'd have millions of tables. Impractical for management, queries, and permissions.
- **B. MERGE INTO bronze table** - Using MERGE on bronze loses the complete audit trail. MERGE would update/delete existing records, destroying historical values. Bronze should be append-only to preserve all changes.
- **C. Iterate through ordered changes, rely on versioning** - Delta Lake versioning is for disaster recovery, not auditing. Versions expire after retention periods (VACUUM). Additionally, applying changes one-by-one is inefficient compared to batch processing.
- **D. Use Delta Lake's change data feed** - Change Data Feed (CDF) is for tracking changes TO a Delta table, not FOR processing CDC data FROM an external system. CDF is the output of Delta changes, not the input mechanism for external CDC logs.

---

### Question 14

An hourly batch job is configured to ingest data files from a cloud object storage container where each batch represent all records produced by the source system in a given hour. The batch job to process these records into the Lakehouse is sufficiently delayed to ensure no late-arriving data is missed. The user_id field represents a unique key for the data, which has the following schema: user_id BIGINT, username STRING, user_utc STRING, user_region STRING, last_login BIGINT, auto_pay BOOLEAN, last_updated BIGINTNew records are all ingested into a table named account_history which maintains a full record of all data in the same schema as the source. The next table in the system is named account_current and is implemented as a Type 1 table representing the most recent value for each unique user_id.Assuming there are millions of user accounts and tens of thousands of records processed hourly, which implementation can be used to efficiently update the described account_current table as part of each hourly batch job?

- A. Use Auto Loader to subscribe to new files in the account_history directory; configure a Structured Streaming trigger once job to batch update newly detected files into the account_current table.
- B. Overwrite the account_current table with each batch using the results of a query against the account_history table grouping by user_id and filtering for the max value of last_updated.
- C. Filter records in account_history using the last_updated field and the most recent hour processed, as well as the max last_iogin by user_id write a merge statement to update or insert the most recent value for each user_id.
- D. Use Delta Lake version history to get the difference between the latest version of account_history and one version prior, then write these records to account_current.
- E. Filter records in account_history using the last_updated field and the most recent hour processed, making sure to deduplicate on username; write a merge statement to update or insert the most recent value for each username.

**Topic Mapping:** Section 10: Data Modelling → Design and implement scalable data models using Delta Lake & Section 3: Data Transformation, Cleansing, and Quality → Write efficient Spark SQL and PySpark code

**Important Keywords to Focus On:**
- "**Type 1 table**" - SCD Type 1, overwrites with latest value
- "**tens of thousands of records processed hourly**" - only process new records, not all millions
- "**user_id field represents a unique key**" - merge key
- "**efficiently update**" - avoid full table scans

**Correct Answer:** C

**Explanation:**
This is the most efficient approach for Type 1 SCD updates:
- **Filter by last_updated for most recent hour**: Only processes the tens of thousands of new/changed records, not millions of existing records
- **Group by user_id with max last_login**: Handles cases where a user had multiple updates within the hour, keeping only the most recent
- **MERGE statement on user_id**: Efficiently updates existing records or inserts new ones
  - MERGE only scans target table for matching keys
  - Much more efficient than full table overwrites
  - Leverages Delta Lake's optimizations

This incremental approach scales well: processing time is proportional to changed records, not total table size.

**Why Other Options are Incorrect:**
- **A. Auto Loader on account_history directory** - Auto Loader is designed for ingesting files from cloud storage, not for reading from Delta tables. account_history is a Delta table, not a cloud storage directory with files. You'd use Structured Streaming with Delta source, but this adds unnecessary complexity for a simple hourly batch pattern.
- **B. Overwrite entire table each time** - Extremely inefficient. This requires:
  - Reading all historical records (millions)
  - Grouping and aggregating millions of records every hour
  - Writing millions of records even when only thousands changed
  - Wastes compute on unchanged data
- **D. Delta version history difference** - While technically possible, this approach:
  - Requires reading and comparing two full table versions
  - More complex than necessary
  - Version comparison isn't optimized for this use case
  - Doesn't leverage MERGE optimizations
- **E. Deduplicate on username, merge on username** - **Critical error**: The unique key is **user_id**, not username. Usernames might not be unique (could be null, could change, might have duplicates). Using the wrong key would cause data corruption.

---

### Question 15

A table in the Lakehouse named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.The churn prediction model used by the ML team is fairly stable in production. The team is only interested in making predictions on records that have changed in the past 24 hours.Which approach would simplify the identification of these changed records?

- A. Apply the churn model to all rows in the customer_churn_params table, but implement logic to perform an upsert into the predictions table that ignores rows where predictions have not changed.
- B. Convert the batch job to a Structured Streaming job using the complete output mode; configure a Structured Streaming job to read from the customer_churn_params table and incrementally predict against the churn model.
- C. Calculate the difference between the previous model predictions and the current customer_churn_params on a key identifying unique customers before making new predictions; only make predictions on those customers not in the previous predictions.
- D. Modify the overwrite logic to include a field populated by calling spark.sql.functions.current_timestamp() as data are being written; use this field to identify records written on a particular date.
- E. Replace the current overwrite logic with a merge statement to modify only those records that have changed; write logic to make predictions on the changed records identified by the change data feed.

**Topic Mapping:** Section 6: Cost & Performance Optimisation → Apply Change Data Feed (CDF) to address specific limitations & Section 3: Data Transformation, Cleansing, and Quality

**Important Keywords to Focus On:**
- "**only interested in making predictions on records that have changed**" - need to identify changed rows
- "**currently...overwriting the table**" - no change tracking currently
- "**simplify the identification**" - looking for easiest solution
- "**past 24 hours**" - daily change detection

**Correct Answer:** E

**Explanation:**
This solution addresses the core problem optimally:
- **Replace overwrite with MERGE**: MERGE statements update only changed records and insert new ones, unlike overwrite which replaces everything
- **Change Data Feed (CDF)**: When enabled on a Delta table (`ALTER TABLE SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`), CDF automatically tracks all changes (inserts, updates, deletes)
- CDF provides a simple API to query changes: `table_changes('table_name', start_version)` or between timestamps
- **Workflow**:
  1. MERGE updates customer_churn_params with latest data
  2. CDF automatically records which rows changed
  3. Query CDF to get only changed records from past 24 hours
  4. Make predictions only on those changed records
- This is the **simplest** and most efficient approach - CDF handles all change tracking automatically

**Why Other Options are Incorrect:**
- **A. Apply model to all rows, then ignore unchanged predictions** - Inefficient. You're running expensive ML inference on millions of records, then throwing away most predictions. Wastes significant compute resources. Doesn't simplify identification - you still process everything.
- **B. Structured Streaming with complete output mode** - "Complete mode" outputs the entire result table every trigger, which defeats the purpose of incremental processing. Also, converting to streaming adds complexity without solving the core problem of identifying changed records. Overwrite pattern isn't compatible with streaming semantics.
- **C. Calculate difference between previous predictions and current data** - This is complex and error-prone:
  - Requires joining two large datasets
  - Doesn't truly identify data changes (only differences in final state)
  - What if a customer wasn't in previous predictions but exists in current data?
  - Adds significant engineering overhead
- **D. Add timestamp field during write** - With overwrite mode, every record gets the same timestamp each night. You can't distinguish which records actually changed vs which records happened to be written. All records would have yesterday's timestamp, providing no useful information about changes.

---

### Question 16

![Question 16 Image 1](examtopics_images/question_16_img_1.png)

A table is registered with the following code:Both users and orders are Delta Lake tables. Which statement describes the results of querying recent_orders?

- A. All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query finishes.
- B. All logic will execute when the table is defined and store the result of joining tables to the DBFS; this stored data will be returned when the table is queried.
- C. Results will be computed and cached when the table is defined; these cached results will incrementally update as new records are inserted into source tables.
- D. All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query began.
- E. The versions of each source table will be stored in the table transaction log; query results will be saved to DBFS with each query.

**Topic Mapping:** Section 1: Developing Code for Data Processing using Python and SQL → Building ETL pipelines with SQL and Apache Spark on Databricks Platform

**Important Keywords to Focus On:**
- "**CREATE OR REPLACE VIEW**" (from image) - creating a view, not a table
- "**VIEW**" - virtual table, no data storage
- "**Delta Lake tables**" - source tables are Delta
- "**query time**" - when view logic executes

**Correct Answer:** D

**Explanation:**
Views in SQL are **virtual tables** that store query definitions, not data:
- **CREATE VIEW** stores only the SQL query definition, not the results
- When you query `recent_orders`, the view's SQL is executed **at query time**
- The view reads from the **current state** of the source tables (`users` and `orders`) as they exist **when the query begins**
- Each query against the view executes the full SELECT statement with the JOIN
- Delta Lake's ACID guarantees ensure **snapshot isolation**: the query sees a consistent snapshot from when it started, not during execution

This is different from materialized views (which cache results) or tables (which store data).

**Why Other Options are Incorrect:**
- **A. Valid versions at time query finishes** - Due to snapshot isolation, queries read from a consistent snapshot taken at query start time, not finish time. This prevents dirty reads and ensures consistency even if data changes during a long-running query.
- **B. Logic executes when table is defined, stored to DBFS** - This describes a table or materialized view, not a regular view. Views don't store data; they only store the query definition. No computation happens at view creation.
- **C. Results computed and cached, incrementally update** - This describes a streaming table or incrementally updated materialized view. Regular views don't cache results or incrementally update. They recompute from scratch every time.
- **E. Versions stored in transaction log, results saved to DBFS** - Views don't have their own transaction logs or store results to DBFS. They're just stored SQL query definitions in the metastore. Each query execution reads from the source tables' current state.

---

### Question 17

A production workload incrementally applies updates from an external Change Data Capture feed to a Delta Lake table as an always-on Structured Stream job. When data was initially migrated for this table, OPTIMIZE was executed and most data files were resized to 1 GB. Auto Optimize and Auto Compaction were both turned on for the streaming production job. Recent review of data files shows that most data files are under 64 MB, although each partition in the table contains at least 1 GB of data and the total table size is over 10 TB.Which of the following likely explains these smaller file sizes?

- A. Databricks has autotuned to a smaller target file size to reduce duration of MERGE operations
- B. Z-order indices calculated on the table are preventing file compaction
- C. Bloom filter indices calculated on the table are preventing file compaction
- D. Databricks has autotuned to a smaller target file size based on the overall size of data in the table
- E. Databricks has autotuned to a smaller target file size based on the amount of data in each partition

**Topic Mapping:** Section 6: Cost & Performance Optimisation → Understand delta optimization techniques & Section 1: Developing Code for Data Processing → Build and manage reliable streaming data pipelines

**Important Keywords to Focus On:**
- "**Structured Stream job**" with "**CDC feed**" - frequent small updates
- "**Auto Optimize and Auto Compaction turned on**" - automatic optimization enabled
- "**64 MB files**" - smaller than the 128 MB typical target
- "**each partition has at least 1 GB**" - sufficient data exists

**Correct Answer:** A

**Explanation:**
When using streaming with CDC operations (particularly MERGE operations):
- **Streaming CDC workloads** frequently update existing records, causing **many small writes**
- **MERGE operations** can be slow on large files because they must:
  - Read the entire target file
  - Apply updates/deletes
  - Rewrite the entire file
- Databricks **Adaptive Optimization** automatically tunes file sizes for workload patterns
- For **high-frequency update workloads**, smaller files (64 MB) are more efficient because:
  - Less data to read/rewrite per update
  - Faster MERGE operations
  - Better balance between query performance and update performance
- The system detected the frequent MERGE pattern and **autotuned down from the default** ~128 MB to ~64 MB to optimize for update performance

This is expected behavior for CDC streaming workloads with frequent updates.

**Why Other Options are Incorrect:**
- **B. Z-order indices preventing compaction** - Z-ordering is a one-time optimization technique that reorganizes data layout; it doesn't prevent Auto Compaction from running. Z-ordering actually works with compaction, not against it.
- **C. Bloom filter indices preventing compaction** - Bloom filters are probabilistic data structures for membership testing; they don't affect file compaction operations. They're stored separately in the transaction log and don't interfere with Auto Compaction.
- **D. Autotuned based on overall table size (10 TB)** - Table size alone doesn't determine file size. A 10 TB table could have 1 GB files (10,000 files) or 64 MB files (160,000 files). The system tunes based on workload patterns (read vs. write vs. update), not just total size.
- **E. Autotuned based on partition data size (1 GB per partition)** - Each partition has sufficient data (1 GB) to support larger files. If partition size were the constraint, we'd see files sized proportionally to partition size. The 64 MB file size is driven by the CDC update pattern, not partition boundaries.

---

### Question 18

Which statement regarding stream-static joins and static Delta tables is correct?

- A. Each microbatch of a stream-static join will use the most recent version of the static Delta table as of each microbatch.
- B. Each microbatch of a stream-static join will use the most recent version of the static Delta table as of the job's initialization.
- C. The checkpoint directory will be used to track state information for the unique keys present in the join.
- D. Stream-static joins cannot use static Delta tables because of consistency issues.
- E. The checkpoint directory will be used to track updates to the static Delta table.

**Topic Mapping:** Section 1: Developing Code for Data Processing using Python and SQL → Build and manage reliable streaming data pipelines & Section 3: Data Transformation, Cleansing, and Quality → Write efficient Spark SQL and PySpark code for joins

**Important Keywords to Focus On:**
- "**stream-static joins**" - joining streaming data with static reference data
- "**most recent version**" - when static table is read
- "**each microbatch**" - streaming processing unit
- "**checkpoint directory**" - state management

**Correct Answer:** A

**Explanation:**
In Spark Structured Streaming with stream-static joins:
- The **static Delta table is read fresh at the beginning of each microbatch**
- This ensures the streaming data is joined with the **most current version** of the lookup/reference data
- **Workflow per microbatch**:
  1. Read new streaming records
  2. Read the current state of the static Delta table
  3. Perform the join
  4. Write results
- This design allows the static table to be updated independently, and streaming queries will automatically use the latest data in subsequent microbatches
- Example: If you update a `product_catalog` table between microbatches, the next microbatch will use the updated catalog for enrichment

This is a key feature that enables fresh lookups without restarting the stream.

**Why Other Options are Incorrect:**
- **B. Uses version from job's initialization** - This would be problematic because:
  - Long-running streams would use stale reference data
  - Updates to static tables wouldn't be reflected
  - You'd need to restart streams to pick up static table changes
  - Not the actual behavior of Spark Structured Streaming
- **C. Checkpoint tracks state for unique keys in join** - Checkpoints track **streaming state** (offsets, watermarks, aggregation state), not join keys. Stream-static joins are stateless from the streaming perspective - no join state needs to be maintained across microbatches.
- **D. Cannot use Delta tables due to consistency issues** - False. Delta tables work perfectly with stream-static joins. Delta Lake's ACID properties actually make it ideal for this pattern - each read gets a consistent snapshot.
- **E. Checkpoint tracks updates to static table** - Checkpoints don't track the static table's changes or versions. They only track the streaming side's progress (offsets). The static table is re-read independently each microbatch without checkpoint involvement.

---

### Question 19

![Question 19 Image 1](examtopics_images/question_19_img_1.png)

A junior data engineer has been asked to develop a streaming data pipeline with a grouped aggregation using DataFrame df. The pipeline needs to calculate the average humidity and average temperature for each non-overlapping five-minute interval. Events are recorded once per minute per device.Streaming DataFrame df has the following schema:"device_id INT, event_time TIMESTAMP, temp FLOAT, humidity FLOAT"Code block:Choose the response that correctly fills in the blank within the code block to complete this task.

- A. to_interval("event_time", "5 minutes").alias("time")
- B. window("event_time", "5 minutes").alias("time")
- C. "event_time"
- D. window("event_time", "10 minutes").alias("time")
- E. lag("event_time", "10 minutes").alias("time")

**Topic Mapping:** Section 3: Data Transformation, Cleansing, and Quality → Write efficient Spark SQL and PySpark code to apply advanced data transformations, including window functions

**Important Keywords to Focus On:**
- "**non-overlapping five-minute interval**" - tumbling window of 5 minutes
- "**grouped aggregation**" - groupBy with aggregation
- "**event_time TIMESTAMP**" - time column for windowing
- "**calculate average humidity and temperature**" - aggregation per window

**Correct Answer:** B

**Explanation:**
For time-based grouping in Spark Structured Streaming, use the `window()` function:
- **window("event_time", "5 minutes")** creates **tumbling windows** (non-overlapping) of 5-minute duration
- Returns a struct containing window start and end times
- When used in `groupBy()`, it groups events that fall within the same 5-minute window
- **Complete code would be**:
  ```python
  df.groupBy(window("event_time", "5 minutes").alias("time"), "device_id")
    .agg(avg("temp"), avg("humidity"))
  ```
- Each device's data is grouped into 5-minute buckets (e.g., 10:00-10:05, 10:05-10:10, etc.)
- Aggregations compute average temp and humidity per device per window

This is the standard pattern for time-based aggregations in streaming workloads.

**Why Other Options are Incorrect:**
- **A. to_interval("event_time", "5 minutes")** - `to_interval()` is not a valid Spark function for windowing. There's `to_timestamp()`, `to_date()`, but not `to_interval()` for creating time windows. This would result in a function not found error.
- **C. "event_time"** - Grouping by raw timestamp would create a separate group for each unique timestamp. Since events occur every minute, you'd get 1-minute groups, not 5-minute intervals. Also wouldn't aggregate across multiple events properly.
- **D. window("event_time", "10 minutes")** - Creates 10-minute windows, not the required 5-minute intervals. While syntactically correct, it doesn't meet the specification of "five-minute interval."
- **E. lag("event_time", "10 minutes")** - `lag()` is a window function for accessing previous rows within a partition, not for time-based grouping. It's used in SQL window functions like `ROW_NUMBER()` and `RANK()`, not for grouped aggregations by time intervals.

---

### Question 20

![Question 20 Image 1](examtopics_images/question_20_img_1.png)

A data architect has designed a system in which two Structured Streaming jobs will concurrently write to a single bronze Delta table. Each job is subscribing to a different topic from an Apache Kafka source, but they will write data with the same schema. To keep the directory structure simple, a data engineer has decided to nest a checkpoint directory to be shared by both streams.The proposed directory structure is displayed below:Which statement describes whether this checkpoint directory structure is valid for the given scenario and why?

- A. No; Delta Lake manages streaming checkpoints in the transaction log.
- B. Yes; both of the streams can share a single checkpoint directory.
- C. No; only one stream can write to a Delta Lake table.
- D. Yes; Delta Lake supports infinite concurrent writers.
- E. No; each of the streams needs to have its own checkpoint directory.

**Topic Mapping:** Section 1: Developing Code for Data Processing using Python and SQL → Build and manage reliable streaming data pipelines & Section 6: Cost & Performance Optimisation

**Important Keywords to Focus On:**
- "**two Structured Streaming jobs**" - two separate streams
- "**concurrently write to a single bronze Delta table**" - multiple writers
- "**nest a checkpoint directory to be shared**" - one checkpoint for both streams
- "**checkpoint directory**" - maintains streaming state

**Correct Answer:** E

**Explanation:**
In Spark Structured Streaming, **each stream must have its own dedicated checkpoint directory**:
- **Checkpoint directories store**:
  - Stream offsets (what data has been processed)
  - State information for stateful operations
  - Metadata about the streaming query
- **Why separate checkpoints are required**:
  - Each stream tracks different Kafka offsets (different topics)
  - Checkpoint data is stream-specific and cannot be shared
  - Sharing would cause checkpoint corruption and data inconsistency
  - Streams would overwrite each other's progress information
- **Delta Lake supports concurrent writes** from multiple streams to the same table (using optimistic concurrency)
- But **checkpoints must be separate** per stream

**Correct structure**:
```
/bronze_table/
  _delta_log/
  data_files/
/checkpoint_stream1/
/checkpoint_stream2/
```

**Why Other Options are Incorrect:**
- **A. Delta Lake manages checkpoints in transaction log** - False. Delta Lake's transaction log (`_delta_log`) tracks table-level transactions (commits, schema, etc.), not streaming checkpoints. Streaming checkpoints are managed separately by Spark Structured Streaming in the specified checkpoint directory.
- **B. Streams can share a checkpoint directory** - False. Sharing checkpoints would cause corruption. Each stream needs independent tracking of its position in the source (Kafka offsets) and its state. Concurrent access would result in conflicts and lost data.
- **C. Only one stream can write to Delta table** - False. Delta Lake explicitly supports multiple concurrent writers using optimistic concurrency control. Multiple streams can safely write to the same Delta table simultaneously - they just need separate checkpoints.
- **D. Delta Lake supports infinite concurrent writers** - While Delta Lake supports multiple concurrent writers, this answer doesn't address the actual question about checkpoint directories. Also, "infinite" is an exaggeration - there are practical limits based on conflict resolution and performance.

---

### Question 21

A Structured Streaming job deployed to production has been experiencing delays during peak hours of the day. At present, during normal execution, each microbatch of data is processed in less than 3 seconds. During peak hours of the day, execution time for each microbatch becomes very inconsistent, sometimes exceeding 30 seconds. The streaming write is currently configured with a trigger interval of 10 seconds.Holding all other variables constant and assuming records need to be processed in less than 10 seconds, which adjustment will meet the requirement?

- A. Decrease the trigger interval to 5 seconds; triggering batches more frequently allows idle executors to begin processing the next batch while longer running tasks from previous batches finish.
- B. Increase the trigger interval to 30 seconds; setting the trigger interval near the maximum execution time observed for each batch is always best practice to ensure no records are dropped.
- C. The trigger interval cannot be modified without modifying the checkpoint directory; to maintain the current stream state, increase the number of shuffle partitions to maximize parallelism.
- D. Use the trigger once option and configure a Databricks job to execute the query every 10 seconds; this ensures all backlogged records are processed with each batch.
- E. Decrease the trigger interval to 5 seconds; triggering batches more frequently may prevent records from backing up and large batches from causing spill.

**Topic Mapping:** Section 6: Cost & Performance Optimisation → Query profiling and bottleneck identification & Section 1: Developing Code for Data Processing → Build and manage reliable streaming data pipelines

**Important Keywords to Focus On:**
- "**microbatch processed in less than 3 seconds**" (normal) vs "**sometimes exceeding 30 seconds**" (peak)
- "**trigger interval of 10 seconds**" - batch frequency
- "**peak hours become inconsistent**" - volume-related issue
- "**records need to be processed in less than 10 seconds**" - requirement

**Correct Answer:** E

**Explanation:**
The issue is that during peak hours, **large batches accumulate** causing performance problems:
- **Current situation**: 10-second trigger means waiting 10 seconds before processing next batch
- During peak hours, **more data arrives** per 10-second interval
- Larger batches cause:
  - Memory pressure and potential spill to disk
  - Longer processing times (30+ seconds)
  - Backlog accumulation

**Solution - Decrease trigger interval to 5 seconds**:
- **More frequent processing** = smaller, more manageable batches
- Instead of 10 seconds of data accumulating, only 5 seconds accumulates
- **Smaller batches** process faster and more consistently
- Reduces memory pressure and prevents spill
- Better resource utilization with steady processing vs. spiky workload
- 5-second batches should still complete well under 10-second requirement

This is a common pattern: more frequent, smaller batches often perform better than less frequent, larger batches.

**Why Other Options are Incorrect:**
- **A. Idle executors begin processing next batch** - This misunderstands Spark Structured Streaming. A streaming query processes one microbatch at a time sequentially. Executors don't start the next microbatch while the previous one is still running. The trigger controls when the next microbatch starts after the previous completes (or after the trigger interval, whichever is longer).
- **B. Increase interval to 30 seconds** - This makes the problem worse. Larger trigger intervals = more data per batch = larger processing times. You'd end up with even bigger batches during peak hours, potentially taking minutes to process. Completely opposite of what's needed.
- **C. Cannot modify trigger interval without modifying checkpoint** - False. Trigger interval is a runtime parameter that can be changed without affecting the checkpoint directory. Checkpoints store offsets and state, not trigger configuration. You can change triggers freely.
- **D. Trigger once every 10 seconds via job scheduler** - Trigger-once is for batch-like processing of all available data, not for consistent streaming. It would process ALL accumulated data in one large batch, making the problem worse. Also, scheduling trigger-once every 10 seconds via jobs is an anti-pattern - use regular triggers instead.

---

### Question 22

Which statement describes Delta Lake Auto Compaction?

- A. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.
- B. Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.
- C. Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.
- D. Data is queued in a messaging bus instead of committing data directly to memory; all data is committed from the messaging bus in one batch once the job is complete.
- E. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 128 MB.

**Topic Mapping:** Section 6: Cost & Performance Optimisation → Understand delta optimization techniques

**Important Keywords to Focus On:**
- "**Auto Compaction**" - automatic file compaction feature
- "**after the write completes**" - post-write operation
- "**asynchronous job**" - runs in background
- "**target file size**" - compaction goal

**Correct Answer:** E

**Explanation:**
Delta Lake **Auto Compaction** is an optimization feature that automatically manages small files:
- Runs **asynchronously after write operations** complete
- Automatically detects when there are **too many small files** that would benefit from compaction
- If detected, triggers an **OPTIMIZE operation** in the background
- Targets a file size of **128 MB by default** (not 1 GB)
- This happens automatically without user intervention when Auto Compaction is enabled
- Particularly useful for streaming workloads that produce many small files

**Key distinction**: This is different from **Optimized Writes**, which prevents small files during the write itself.

**Why Other Options are Incorrect:**
- **A. Target of 1 GB** - The default target for Auto Compaction is 128 MB, not 1 GB. While you can manually run OPTIMIZE toward 1 GB files, Auto Compaction uses a smaller default of 128 MB for better balance between query performance and compaction overhead.
- **B. OPTIMIZE before cluster terminates on all modified tables** - This isn't Auto Compaction behavior. Auto Compaction runs per-table after writes, not as a cluster-wide operation before termination. Also, it's selective (only when needed), not on all modified tables.
- **C. Logical partitions instead of directory partitions** - This describes **Liquid Clustering**, not Auto Compaction. Liquid Clustering is a different feature that uses logical partitioning to avoid small file problems.
- **D. Data queued in messaging bus** - This describes a completely different architecture pattern (message queue/buffer), not how Delta Lake or Auto Compaction works. Delta Lake writes directly to cloud storage (S3/ADLS/GCS) with ACID guarantees, not through a messaging bus.

---

### Question 23

Which statement characterizes the general programming model used by Spark Structured Streaming?

- A. Structured Streaming leverages the parallel processing of GPUs to achieve highly parallel data throughput.
- B. Structured Streaming is implemented as a messaging bus and is derived from Apache Kafka.
- C. Structured Streaming uses specialized hardware and I/O streams to achieve sub-second latency for data transfer.
- D. Structured Streaming models new data arriving in a data stream as new rows appended to an unbounded table.
- E. Structured Streaming relies on a distributed network of nodes that hold incremental state values for cached stages.

**Topic Mapping:** Section 1: Developing Code for Data Processing using Python and SQL → Build and manage reliable streaming data pipelines using Structured Streaming

**Important Keywords to Focus On:**
- "**programming model**" - conceptual framework
- "**Structured Streaming**" - Spark's streaming API
- "**data stream**" - continuous data flow
- "**unbounded table**" - conceptual model

**Correct Answer:** D

**Explanation:**
The core programming model of **Spark Structured Streaming** is built on a simple but powerful abstraction:
- **Data streams are modeled as unbounded tables** (tables with infinite rows)
- Each **new record** arriving in the stream is conceptually a **new row appended** to this unbounded table
- You write queries using the **same DataFrame/SQL API** as batch processing
- The streaming engine incrementally processes new rows as they arrive
- **Benefits of this model**:
  - Familiar batch-like programming model
  - Same code patterns for batch and streaming
  - Easy reasoning about streaming logic
  - Built-in support for event time, windows, and aggregations

**Example**: Reading from Kafka, each new message is treated as a new row appended to an infinite table, which you query using standard DataFrame operations.

**Why Other Options are Incorrect:**
- **A. Leverages GPU parallel processing** - Spark Structured Streaming runs on standard CPU-based clusters, not GPUs. While Spark can leverage GPUs for certain ML workloads, this isn't the characteristic programming model of Structured Streaming.
- **B. Implemented as messaging bus, derived from Kafka** - Structured Streaming is a **processing engine**, not a messaging bus. While it can read FROM Kafka (and other sources), it's not derived from Kafka nor is it a messaging system itself. It's built on Spark's execution engine.
- **C. Specialized hardware and I/O streams for sub-second latency** - Structured Streaming doesn't require specialized hardware. It runs on standard distributed clusters. While it can achieve low latency, the distinguishing programming model is the unbounded table abstraction, not hardware requirements.
- **E. Distributed network holding incremental state for cached stages** - This describes aspects of Spark's general execution (distributed computing, caching), but it's not the characteristic programming model that distinguishes Structured Streaming. The key differentiator is the unbounded table abstraction.

---

### Question 24

Which configuration parameter directly affects the size of a spark-partition upon ingestion of data into Spark?

- A. spark.sql.files.maxPartitionBytes
- B. spark.sql.autoBroadcastJoinThreshold
- C. spark.sql.files.openCostInBytes
- D. spark.sql.adaptive.coalescePartitions.minPartitionNum
- E. spark.sql.adaptive.advisoryPartitionSizeInBytes

**Topic Mapping:** Section 6: Cost & Performance Optimisation → Query profiling and bottleneck identification

**Important Keywords to Focus On:**
- "**directly affects**" - primary control parameter
- "**size of a spark-partition**" - partition sizing
- "**upon ingestion of data**" - initial data reading phase
- "**into Spark**" - data loading

**Correct Answer:** A

**Explanation:**
`spark.sql.files.maxPartitionBytes` is the primary configuration that controls partition size during data ingestion:
- **Default value**: 128 MB (134217728 bytes)
- **Purpose**: Sets the maximum number of bytes to pack into a single partition when reading files
- **How it works**: During file scanning, Spark creates partitions based on this limit, combining multiple small files or splitting large files
- **Direct impact**: If you set this to 256 MB, Spark will create partitions of approximately 256 MB each during reads
- This is the **primary knob** for controlling initial partition sizes from data sources

**Example**: Reading 1 GB of data with default 128 MB setting creates ~8 partitions.

**Why Other Options are Incorrect:**
- **B. spark.sql.autoBroadcastJoinThreshold** - Controls when Spark broadcasts a small table in joins (default 10 MB). This affects join strategy optimization, not partition sizing during ingestion. It determines whether to use broadcast join vs shuffle join.
- **C. spark.sql.files.openCostInBytes** - This is a cost estimation parameter (default 4 MB) used when Spark decides how to pack small files into partitions. It's an adjustment factor, not the primary control. It influences decisions but doesn't directly set partition size like `maxPartitionBytes` does.
- **D. spark.sql.adaptive.coalescePartitions.minPartitionNum** - Part of Adaptive Query Execution (AQE) that controls the minimum number of partitions after coalescing during shuffle. This affects partitions AFTER ingestion and during shuffles, not during initial data ingestion.
- **E. spark.sql.adaptive.advisoryPartitionSizeInBytes** - Another AQE parameter that suggests target partition size for shuffle operations (default 64 MB). This applies to shuffle/exchange operations during query execution, not initial data ingestion from files.

---

### Question 25

A Spark job is taking longer than expected. Using the Spark UI, a data engineer notes that the Min, Median, and Max Durations for tasks in a particular stage show the minimum and median time to complete a task as roughly the same, but the max duration for a task to be roughly 100 times as long as the minimum.Which situation is causing increased duration of the overall job?

- A. Task queueing resulting from improper thread pool assignment.
- B. Spill resulting from attached volume storage being too small.
- C. Network latency due to some cluster nodes being in different regions from the source data
- D. Skew caused by more data being assigned to a subset of spark-partitions.
- E. Credential validation errors while pulling data from an external system.

**Topic Mapping:** Section 5: Monitoring and Alerting → Use Query Profiler UI and Spark UI to monitor workloads & Section 6: Cost & Performance Optimisation → Identify bottlenecks using query profile

**Important Keywords to Focus On:**
- "**Min and Median roughly the same**" - most tasks complete quickly
- "**Max duration roughly 100 times longer**" - few tasks take much longer
- "**particular stage**" - stage-level performance issue
- "**Spark UI task duration metrics**" - performance diagnosis

**Correct Answer:** D

**Explanation:**
This is a **classic symptom of data skew**:
- **Data skew** occurs when data is unevenly distributed across partitions
- **Symptoms observed**:
  - Most tasks (min/median) complete quickly - these process small/normal partitions
  - Few tasks (max) take 100x longer - these process oversized partitions
  - Overall job is blocked waiting for the slowest tasks (stragglers)
- **Why 100x difference indicates skew**:
  - If all partitions were equal, task times would be similar
  - Huge variance means some partitions have 100x more data
  - One executor gets stuck processing a massive partition while others sit idle
- **Common causes**: Skewed join keys, null values grouped together, hot keys in GROUP BY

This pattern is the telltale sign to look for skew issues in data distribution.

**Why Other Options are Incorrect:**
- **A. Task queueing from thread pool assignment** - Thread pool issues would cause delays across many/all tasks relatively uniformly. You'd see elevated min, median, AND max times, not just max. The pattern wouldn't be 100x difference between min and max.
- **B. Spill from small storage** - Spill affects tasks that exceed memory, but in a severe storage limitation scenario, you'd expect multiple tasks to be affected (higher median), not just a few outliers. Also, spill typically causes 2-10x slowdowns, not 100x.
- **C. Network latency from different regions** - Network latency would affect all tasks reading from remote sources relatively equally. You wouldn't see most tasks fast and few tasks 100x slower. Regional latency issues manifest as uniformly elevated task times.
- **E. Credential validation errors** - Authentication errors typically cause task failures, not just slowness. If credential validation were the issue, you'd see failed tasks or retry patterns, not successful-but-slow tasks. Also wouldn't explain why most tasks are fast while few are slow.

---
