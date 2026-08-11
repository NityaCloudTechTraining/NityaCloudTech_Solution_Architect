# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Module 3: Apache Spark Fundamentals
# MAGIC %md
# MAGIC # Module 3: Apache Spark Fundamentals
# MAGIC
# MAGIC ## Introduction
# MAGIC This module introduces Apache Spark fundamentals - the core concepts you need to start coding with Spark effectively. We'll cover the architecture, key components, and programming model that makes Spark powerful for big data processing.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,What is Apache Spark?
# MAGIC %md
# MAGIC ## 1. What is Apache Spark?
# MAGIC
# MAGIC **Apache Spark** is a unified, open-source, distributed computing engine designed for large-scale data processing.
# MAGIC
# MAGIC ### Key Characteristics:
# MAGIC - **Fast**: In-memory computing makes it 100x faster than Hadoop MapReduce for certain workloads
# MAGIC - **Unified**: Single engine for batch processing, streaming, SQL, machine learning, and graph processing
# MAGIC - **Distributed**: Processes data across multiple machines in parallel
# MAGIC - **Multi-language**: Supports Python, Scala, Java, R, and SQL
# MAGIC - **Open Source**: Apache 2.0 license
# MAGIC
# MAGIC ### Core Components:
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │         Apache Spark Ecosystem          │
# MAGIC ├─────────────────────────────────────────┤
# MAGIC │  Spark SQL  │  Spark Streaming          │
# MAGIC │  (DataFrames, SQL)  │  (Real-time)     │
# MAGIC ├─────────────────────────────────────────┤
# MAGIC │  MLlib      │  GraphX                   │
# MAGIC │  (Machine Learning)  │  (Graph)         │
# MAGIC ├─────────────────────────────────────────┤
# MAGIC │         Spark Core Engine               │
# MAGIC │         (RDD, Distributed Compute)      │
# MAGIC └─────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Why Spark?
# MAGIC %md
# MAGIC ## 2. Why Spark?
# MAGIC
# MAGIC ### Business & Technical Benefits:
# MAGIC
# MAGIC **1. Speed**
# MAGIC - In-memory processing eliminates disk I/O bottlenecks
# MAGIC - Optimized execution engine with Catalyst optimizer
# MAGIC - Lazy evaluation allows for optimization before execution
# MAGIC
# MAGIC **2. Ease of Use**
# MAGIC - High-level APIs (DataFrames, SQL)
# MAGIC - Less code compared to MapReduce
# MAGIC - Interactive shell for rapid development
# MAGIC
# MAGIC **3. Unified Platform**
# MAGIC - Single engine for all data workloads:
# MAGIC   - Batch processing
# MAGIC   - Real-time streaming
# MAGIC   - SQL analytics
# MAGIC   - Machine learning
# MAGIC   - Graph processing
# MAGIC
# MAGIC **4. Scalability**
# MAGIC - Scales from single machine to thousands of nodes
# MAGIC - Automatic parallelization and distribution
# MAGIC
# MAGIC **5. Fault Tolerance**
# MAGIC - Automatic recovery from failures
# MAGIC - Data replication and lineage tracking
# MAGIC
# MAGIC **6. Rich Ecosystem**
# MAGIC - Native integration with Hadoop, S3, Delta Lake, Kafka
# MAGIC - Large community and extensive libraries

# COMMAND ----------

# DBTITLE 1,Spark vs Traditional Processing
# MAGIC %md
# MAGIC ## 3. Spark vs Traditional Processing
# MAGIC
# MAGIC ### Spark vs Hadoop MapReduce:
# MAGIC
# MAGIC | Aspect | Hadoop MapReduce | Apache Spark |
# MAGIC |--------|-----------------|-------------|
# MAGIC | **Speed** | Slower (disk-based) | 100x faster (in-memory) |
# MAGIC | **Ease of Use** | Complex, verbose code | Simple, concise APIs |
# MAGIC | **Processing** | Batch only | Batch + Streaming + Interactive |
# MAGIC | **Iterative Processing** | Poor (writes to disk each iteration) | Excellent (in-memory) |
# MAGIC | **Real-time** | Not supported | Native support |
# MAGIC | **API** | Low-level | High-level (DataFrames, SQL) |
# MAGIC | **Fault Tolerance** | Replication | Lineage + Replication |
# MAGIC
# MAGIC ### Processing Flow Comparison:
# MAGIC
# MAGIC **Traditional (MapReduce):**
# MAGIC ```
# MAGIC Input → HDFS → Map → Disk → Reduce → Disk → Output
# MAGIC         ↓                    ↓
# MAGIC       Slow                 Slow
# MAGIC ```
# MAGIC
# MAGIC **Spark:**
# MAGIC ```
# MAGIC Input → Memory → Transformations → Memory → Action → Output
# MAGIC          ↓                          ↓
# MAGIC         Fast                      Fast
# MAGIC ```
# MAGIC
# MAGIC ### Example: Word Count
# MAGIC
# MAGIC **MapReduce**: ~50 lines of Java code  
# MAGIC **Spark**: 1-3 lines of Python/Scala

# COMMAND ----------

# DBTITLE 1,Spark Architecture
# MAGIC %md
# MAGIC ## 4. Spark Architecture
# MAGIC
# MAGIC Spark follows a **master-slave architecture** with a central coordinator (Driver) and distributed workers (Executors).
# MAGIC
# MAGIC ### Architecture Diagram:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────┐
# MAGIC │                   Cluster Manager                    │
# MAGIC │              (YARN / Kubernetes / Mesos)             │
# MAGIC └──────────────────────────────────────────────────────┘
# MAGIC                           │
# MAGIC         ┌─────────────────┴─────────────────┐
# MAGIC         ▼                                   ▼
# MAGIC ┌───────────────┐                  ┌─────────────────┐
# MAGIC │     Driver    │                  │  Worker Nodes   │
# MAGIC │   Program     │◄────────────────►│                 │
# MAGIC │               │                  │  ┌───────────┐  │
# MAGIC │ SparkContext  │                  │  │ Executor  │  │
# MAGIC │ SparkSession  │                  │  │  ┌─────┐  │  │
# MAGIC │               │                  │  │  │Task │  │  │
# MAGIC │ • DAG         │                  │  │  │Task │  │  │
# MAGIC │ • Scheduler   │                  │  │  └─────┘  │  │
# MAGIC │ • Task        │                  │  │  Cache    │  │
# MAGIC │   Assignment  │                  │  └───────────┘  │
# MAGIC └───────────────┘                  │                 │
# MAGIC                                    │  ┌───────────┐  │
# MAGIC                                    │  │ Executor  │  │
# MAGIC                                    │  │  ┌─────┐  │  │
# MAGIC                                    │  │  │Task │  │  │
# MAGIC                                    │  │  │Task │  │  │
# MAGIC                                    │  │  └─────┘  │  │
# MAGIC                                    │  │  Cache    │  │
# MAGIC                                    │  └───────────┘  │
# MAGIC                                    └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Components:
# MAGIC
# MAGIC **1. Driver**
# MAGIC - The master node that runs your main() program
# MAGIC - Creates SparkContext/SparkSession
# MAGIC - Converts user code into jobs
# MAGIC - Schedules tasks on executors
# MAGIC - Maintains metadata about application
# MAGIC
# MAGIC **2. Executors**
# MAGIC - Worker processes on cluster nodes
# MAGIC - Run tasks assigned by driver
# MAGIC - Store data in memory or disk for caching
# MAGIC - Return results to driver
# MAGIC
# MAGIC **3. Cluster Manager**
# MAGIC - Allocates resources across applications
# MAGIC - Options: YARN, Kubernetes, Mesos, Standalone
# MAGIC
# MAGIC **4. Tasks**
# MAGIC - Smallest unit of work
# MAGIC - Runs on a single executor core
# MAGIC - Processes one partition of data

# COMMAND ----------

# DBTITLE 1,SparkSession
# MAGIC %md
# MAGIC ## 5. SparkSession
# MAGIC
# MAGIC **SparkSession** is the entry point for all Spark functionality. It replaces the older SparkContext, SQLContext, and HiveContext.
# MAGIC
# MAGIC ### Key Features:
# MAGIC - Unified entry point for Spark 2.0+
# MAGIC - Access to SparkContext, SQLContext, and configuration
# MAGIC - Creates DataFrames and Datasets
# MAGIC - Executes SQL queries
# MAGIC - Manages Spark application
# MAGIC
# MAGIC ### SparkSession Hierarchy:
# MAGIC ```
# MAGIC SparkSession
# MAGIC     ├── SparkContext (cluster connection)
# MAGIC     ├── SQLContext (SQL operations)
# MAGIC     └── Configuration (app settings)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Creating SparkSession
# In Databricks, SparkSession is automatically created as 'spark'
# You can verify it:

print(f"Spark Version: {spark.version}")
print(f"SparkSession is available: {spark is not None}")

# If you were creating SparkSession manually (not needed in Databricks):
# from pyspark.sql import SparkSession
# spark = SparkSession.builder \
#     .appName("MyApp") \
#     .config("spark.executor.memory", "4g") \
#     .getOrCreate()

# COMMAND ----------

# DBTITLE 1,DataFrame Overview
# MAGIC %md
# MAGIC ## 6. DataFrame, Dataset, and RDD
# MAGIC
# MAGIC ### Evolution of Spark Data Structures:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────┐
# MAGIC │  RDD (2011) → DataFrame (2013) → Dataset (2015) │
# MAGIC └─────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### 1. **RDD (Resilient Distributed Dataset)**
# MAGIC - Low-level API
# MAGIC - Immutable, distributed collection of objects
# MAGIC - No schema or optimization
# MAGIC - Full control, but verbose
# MAGIC
# MAGIC ### 2. **DataFrame**
# MAGIC - High-level API built on RDD
# MAGIC - Distributed collection of data organized into **named columns** (like a table)
# MAGIC - Has schema
# MAGIC - Optimized execution with Catalyst optimizer
# MAGIC - Language-agnostic (same API in Python, Scala, Java, R)
# MAGIC - **Most commonly used in practice**
# MAGIC
# MAGIC ### 3. **Dataset**
# MAGIC - Type-safe version of DataFrame (Scala/Java only)
# MAGIC - Combines benefits of RDD and DataFrame
# MAGIC - Not available in Python (Python is dynamically typed)
# MAGIC
# MAGIC ### Comparison:
# MAGIC
# MAGIC | Feature | RDD | DataFrame | Dataset |
# MAGIC |---------|-----|-----------|----------|
# MAGIC | **Schema** | No | Yes | Yes |
# MAGIC | **Optimization** | No | Yes (Catalyst) | Yes (Catalyst) |
# MAGIC | **Type Safety** | Compile-time | Runtime | Compile-time |
# MAGIC | **Language** | All | All | Scala/Java only |
# MAGIC | **Ease of Use** | Low | High | High |
# MAGIC | **Performance** | Lower | Higher | Higher |
# MAGIC | **Use Case** | Low-level control | 99% of use cases | Type-safe operations |
# MAGIC
# MAGIC ### When to Use What?
# MAGIC - **DataFrame**: Default choice for 99% of use cases
# MAGIC - **RDD**: Only when you need fine-grained control (rare)
# MAGIC - **Dataset**: When you need compile-time type safety in Scala/Java
# MAGIC
# MAGIC ### DataFrame Structure:
# MAGIC ```
# MAGIC DataFrame = RDD + Schema + Optimization
# MAGIC
# MAGIC Example:
# MAGIC ┌──────┬────────┬───────┐
# MAGIC │ id   │ name   │ age   │  ← Column Names (Schema)
# MAGIC ├──────┼────────┼───────┤
# MAGIC │ 1    │ Alice  │ 25    │  ← Row (distributed)
# MAGIC │ 2    │ Bob    │ 30    │  ← Row (distributed)
# MAGIC │ 3    │ Carol  │ 35    │  ← Row (distributed)
# MAGIC └──────┴────────┴───────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Transformations
# MAGIC %md
# MAGIC ## 7. Transformations
# MAGIC
# MAGIC **Transformations** are operations that create a new DataFrame from an existing one. They are **lazy** (not executed immediately).
# MAGIC
# MAGIC ### Key Characteristics:
# MAGIC - Return a new DataFrame
# MAGIC - Lazy evaluation (deferred execution)
# MAGIC - Build up a computation plan (DAG)
# MAGIC - No data is processed until an action is called
# MAGIC
# MAGIC ### Common Transformations:
# MAGIC
# MAGIC **1. Narrow Transformations** (no shuffle needed):
# MAGIC - `select()` - Select specific columns
# MAGIC - `filter()` / `where()` - Filter rows
# MAGIC - `withColumn()` - Add/modify column
# MAGIC - `drop()` - Remove column
# MAGIC - `distinct()` - Remove duplicates
# MAGIC - `map()`, `flatMap()` - Apply function to each element
# MAGIC
# MAGIC **2. Wide Transformations** (require shuffle):
# MAGIC - `groupBy()` - Group data
# MAGIC - `join()` - Join DataFrames
# MAGIC - `orderBy()` / `sort()` - Sort data
# MAGIC - `repartition()` - Change number of partitions
# MAGIC - `union()` - Combine DataFrames
# MAGIC
# MAGIC ### Narrow vs Wide:
# MAGIC
# MAGIC **Narrow Transformation:**
# MAGIC ```
# MAGIC Partition 1 → Transform → Partition 1
# MAGIC Partition 2 → Transform → Partition 2
# MAGIC Partition 3 → Transform → Partition 3
# MAGIC (No data movement between partitions)
# MAGIC ```
# MAGIC
# MAGIC **Wide Transformation:**
# MAGIC ```
# MAGIC Partition 1 ┐
# MAGIC Partition 2 ├→ Shuffle → New Partition 1
# MAGIC Partition 3 ┘            New Partition 2
# MAGIC (Data movement required - expensive)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Actions
# MAGIC %md
# MAGIC ## 8. Actions
# MAGIC
# MAGIC **Actions** trigger the execution of transformations and return results to the driver or write data to storage.
# MAGIC
# MAGIC ### Key Characteristics:
# MAGIC - Trigger actual computation
# MAGIC - Return results or write data
# MAGIC - Eager evaluation
# MAGIC - Execute the entire DAG
# MAGIC
# MAGIC ### Common Actions:
# MAGIC
# MAGIC **1. Return Data to Driver:**
# MAGIC - `show()` - Display first n rows
# MAGIC - `count()` - Count number of rows
# MAGIC - `collect()` - Return all data to driver (⚠️ use carefully!)
# MAGIC - `take(n)` - Return first n rows
# MAGIC - `first()` - Return first row
# MAGIC - `head()` - Return first row
# MAGIC
# MAGIC **2. Write Data:**
# MAGIC - `write.format().save()` - Save to storage
# MAGIC - `write.parquet()` - Save as Parquet
# MAGIC - `write.csv()` - Save as CSV
# MAGIC - `saveAsTable()` - Save as table
# MAGIC
# MAGIC **3. Aggregate Actions:**
# MAGIC - `reduce()` - Reduce elements
# MAGIC - `foreach()` - Apply function to each partition
# MAGIC
# MAGIC ### ⚠️ Important Warning:
# MAGIC **Never use `collect()` on large datasets!** It brings all data to the driver and can cause out-of-memory errors.
# MAGIC
# MAGIC ### Transformation vs Action:
# MAGIC
# MAGIC ```
# MAGIC Transformation                    Action
# MAGIC ─────────────                    ──────
# MAGIC select()                         show()
# MAGIC filter()                         count()
# MAGIC where()                          collect()
# MAGIC withColumn()                     write.save()
# MAGIC groupBy()        ──────────►     take()
# MAGIC join()                           first()
# MAGIC orderBy()                        
# MAGIC
# MAGIC (Lazy)                           (Triggers execution)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Lazy Evaluation
# MAGIC %md
# MAGIC ## 9. Lazy Evaluation
# MAGIC
# MAGIC **Lazy Evaluation** means Spark delays execution of transformations until an action is called.
# MAGIC
# MAGIC ### Why Lazy Evaluation?
# MAGIC
# MAGIC **1. Optimization**
# MAGIC - Spark can optimize the entire execution plan
# MAGIC - Combine multiple operations
# MAGIC - Eliminate unnecessary steps
# MAGIC - Choose best execution strategy
# MAGIC
# MAGIC **2. Efficiency**
# MAGIC - Avoid unnecessary computations
# MAGIC - Minimize data movement
# MAGIC - Reduce I/O operations
# MAGIC
# MAGIC **3. Fault Tolerance**
# MAGIC - Keep lineage information
# MAGIC - Can recompute lost partitions
# MAGIC
# MAGIC ### Example:
# MAGIC
# MAGIC ```python
# MAGIC # These are transformations - NOT executed yet
# MAGIC df1 = spark.read.csv("data.csv")        # Not executed
# MAGIC df2 = df1.filter(col("age") > 25)       # Not executed
# MAGIC df3 = df2.select("name", "age")         # Not executed
# MAGIC df4 = df3.orderBy("age")                # Not executed
# MAGIC
# MAGIC # This is an action - NOW everything executes
# MAGIC df4.show()                               # EXECUTED!
# MAGIC ```
# MAGIC
# MAGIC ### Execution Flow:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  1. User writes transformations        │
# MAGIC │     (Nothing happens yet)              │
# MAGIC └────────────────┬───────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  2. Spark builds logical plan (DAG)    │
# MAGIC │     - Analyzes operations              │
# MAGIC │     - Plans optimization               │
# MAGIC └────────────────┬───────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  3. User calls action (show, count)    │
# MAGIC └────────────────┬───────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  4. Catalyst Optimizer optimizes plan  │
# MAGIC │     - Predicate pushdown               │
# MAGIC │     - Column pruning                   │
# MAGIC │     - Constant folding                 │
# MAGIC └────────────────┬───────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  5. Generate physical plan             │
# MAGIC └────────────────┬───────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  6. Execute on cluster                 │
# MAGIC │     - Distribute tasks                 │
# MAGIC │     - Process data                     │
# MAGIC │     - Return results                   │
# MAGIC └────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,DAG (Directed Acyclic Graph)
# MAGIC %md
# MAGIC ## 10. DAG (Directed Acyclic Graph)
# MAGIC
# MAGIC **DAG** is Spark's execution plan - a graph of operations with dependencies but no cycles.
# MAGIC
# MAGIC ### What is a DAG?
# MAGIC - **Directed**: Operations have a direction (input → output)
# MAGIC - **Acyclic**: No loops or cycles
# MAGIC - **Graph**: Network of operations
# MAGIC
# MAGIC ### DAG Structure:
# MAGIC
# MAGIC ```
# MAGIC      ┌──────────┐
# MAGIC      │  Read    │  ← Source
# MAGIC      │  Data    │
# MAGIC      └────┬─────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC      ┌──────────┐
# MAGIC      │  Filter  │  ← Transformation
# MAGIC      └────┬─────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC      ┌──────────┐
# MAGIC      │  Select  │  ← Transformation
# MAGIC      └────┬─────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC      ┌──────────┐
# MAGIC      │  Group   │  ← Transformation (Wide)
# MAGIC      │   By     │
# MAGIC      └────┬─────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC      ┌──────────┐
# MAGIC      │   Agg    │  ← Transformation
# MAGIC      └────┬─────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC      ┌──────────┐
# MAGIC      │  Show    │  ← Action (triggers execution)
# MAGIC      └──────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Why DAG?
# MAGIC
# MAGIC **1. Optimization**
# MAGIC - See the entire plan before execution
# MAGIC - Combine operations (pipelining)
# MAGIC - Eliminate redundant steps
# MAGIC
# MAGIC **2. Fault Tolerance**
# MAGIC - Track lineage of each partition
# MAGIC - Recompute only lost partitions
# MAGIC - No need to replicate intermediate data
# MAGIC
# MAGIC **3. Scheduling**
# MAGIC - Identify stages and tasks
# MAGIC - Parallelize independent operations
# MAGIC - Minimize shuffles
# MAGIC
# MAGIC ### Example DAG with Multiple Operations:
# MAGIC
# MAGIC ```
# MAGIC    Data1 ──┐
# MAGIC            ├─→ Join ──→ Filter ──→ Select ──→ Write
# MAGIC    Data2 ──┘
# MAGIC ```
# MAGIC
# MAGIC ### Viewing the DAG:
# MAGIC You can view the DAG in Spark UI or use `explain()` method.

# COMMAND ----------

# DBTITLE 1,Jobs, Stages, and Tasks
# MAGIC %md
# MAGIC ## 11. Jobs, Stages, and Tasks
# MAGIC
# MAGIC Spark breaks down your code into a hierarchy: **Jobs → Stages → Tasks**
# MAGIC
# MAGIC ### Hierarchy:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────┐
# MAGIC │                   JOB                        │
# MAGIC │  (Triggered by one action: show, count...)   │
# MAGIC │                                              │
# MAGIC │  ┌────────────────────────────────────────┐ │
# MAGIC │  │          STAGE 1 (No Shuffle)          │ │
# MAGIC │  │                                        │ │
# MAGIC │  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ │ │
# MAGIC │  │  │ Task │ │ Task │ │ Task │ │ Task │ │ │
# MAGIC │  │  │  1   │ │  2   │ │  3   │ │  4   │ │ │
# MAGIC │  │  └──────┘ └──────┘ └──────┘ └──────┘ │ │
# MAGIC │  │  (Each task processes one partition)  │ │
# MAGIC │  └────────────────┬───────────────────────┘ │
# MAGIC │                   │ Shuffle Boundary        │
# MAGIC │                   ▼                         │
# MAGIC │  ┌────────────────────────────────────────┐ │
# MAGIC │  │          STAGE 2 (After Shuffle)       │ │
# MAGIC │  │                                        │ │
# MAGIC │  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ │ │
# MAGIC │  │  │ Task │ │ Task │ │ Task │ │ Task │ │ │
# MAGIC │  │  │  5   │ │  6   │ │  7   │ │  8   │ │ │
# MAGIC │  │  └──────┘ └──────┘ └──────┘ └──────┘ │ │
# MAGIC │  └────────────────────────────────────────┘ │
# MAGIC └──────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### 1. Job
# MAGIC - **Created by**: Each action (show, count, write, collect)
# MAGIC - **Contains**: One or more stages
# MAGIC - **Lifecycle**: Starts when action is called, ends when complete
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC df.filter(...).groupBy(...).count()  # One action = One job
# MAGIC ```
# MAGIC
# MAGIC ### 2. Stage
# MAGIC - **Created by**: Split at shuffle boundaries (wide transformations)
# MAGIC - **Contains**: Multiple tasks that can run in parallel
# MAGIC - **No shuffle within a stage**: All transformations in a stage are pipelined
# MAGIC
# MAGIC **Shuffle Boundaries:**
# MAGIC - `groupBy()`, `join()`, `orderBy()`, `repartition()`
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC df.filter(...).select(...)           # Stage 1 (narrow transformations)
# MAGIC   .groupBy(...)                      # ← Shuffle boundary
# MAGIC   .agg(...)                          # Stage 2
# MAGIC ```
# MAGIC
# MAGIC ### 3. Task
# MAGIC - **Created by**: One task per partition per stage
# MAGIC - **Smallest unit**: Runs on a single executor core
# MAGIC - **Processes**: One partition of data
# MAGIC - **Parallel execution**: All tasks in a stage run in parallel (if resources available)
# MAGIC
# MAGIC **Task Count Formula:**
# MAGIC ```
# MAGIC Number of Tasks = Number of Partitions × Number of Stages
# MAGIC ```
# MAGIC
# MAGIC ### Complete Example:
# MAGIC
# MAGIC ```python
# MAGIC # Code:
# MAGIC df = spark.read.parquet("data")      # No job yet
# MAGIC      .filter(col("age") > 25)        # No job yet
# MAGIC      .groupBy("city")                # No job yet
# MAGIC      .count()                        # Action → Job created
# MAGIC      .show()                         # Another action → Another job
# MAGIC
# MAGIC # Execution breakdown:
# MAGIC # Job 1 (from count()):
# MAGIC #   Stage 1: Read + Filter (narrow transformations)
# MAGIC #     Task 1, Task 2, Task 3, ... (one per partition)
# MAGIC #   
# MAGIC #   Stage 2: GroupBy + Count (after shuffle)
# MAGIC #     Task N, Task N+1, ... (one per partition after shuffle)
# MAGIC #
# MAGIC # Job 2 (from show()):
# MAGIC #   Stage 1: Take top 20 rows
# MAGIC #     Task 1, Task 2, ...
# MAGIC ```
# MAGIC
# MAGIC ### Key Points:
# MAGIC
# MAGIC ✅ **Fewer stages = Better performance** (avoid shuffles)  
# MAGIC ✅ **More tasks = More parallelism** (but more overhead)  
# MAGIC ✅ **Optimal partition count** ≈ 2-3× number of cores  
# MAGIC ✅ **Monitor in Spark UI**: Jobs → Stages → Tasks
# MAGIC
# MAGIC ### Performance Tips:
# MAGIC
# MAGIC 1. **Minimize shuffles** (wide transformations)
# MAGIC 2. **Right-size partitions** (not too many, not too few)
# MAGIC 3. **Use narrow transformations** when possible
# MAGIC 4. **Cache intermediate results** if reused
# MAGIC 5. **Monitor Spark UI** to understand execution

# COMMAND ----------

# DBTITLE 1,Catalyst Optimizer
# MAGIC %md
# MAGIC ## 15. Catalyst Optimizer
# MAGIC
# MAGIC **Catalyst** is Spark's query optimizer that automatically improves execution plans.
# MAGIC
# MAGIC ### Optimization Pipeline:
# MAGIC
# MAGIC ```
# MAGIC Your Code
# MAGIC     ↓
# MAGIC Logical Plan
# MAGIC     ↓
# MAGIC Optimization Rules
# MAGIC     ↓
# MAGIC Physical Plans
# MAGIC     ↓
# MAGIC Best Plan Selected
# MAGIC     ↓
# MAGIC Code Generation
# MAGIC     ↓
# MAGIC Execution
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Optimizations:
# MAGIC
# MAGIC **1. Predicate Pushdown**
# MAGIC
# MAGIC Push filters close to data source.
# MAGIC
# MAGIC ```python
# MAGIC # Your code:
# MAGIC df = spark.read.parquet("data")
# MAGIC df.filter(col("age") > 25)
# MAGIC
# MAGIC # Catalyst: Reads only rows where age > 25 at source!
# MAGIC ```
# MAGIC
# MAGIC **Impact:** Read 100MB instead of 1GB (10× faster)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **2. Column Pruning**
# MAGIC
# MAGIC Read only needed columns.
# MAGIC
# MAGIC ```python
# MAGIC # Your code:
# MAGIC df.select("name", "age")
# MAGIC
# MAGIC # Catalyst: Reads only 'name' and 'age' columns
# MAGIC ```
# MAGIC
# MAGIC **Impact:** Read 2/10 columns = 5× less data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **3. Constant Folding**
# MAGIC
# MAGIC ```python
# MAGIC # Your code:
# MAGIC df.filter(col("age") > 10 + 15)
# MAGIC
# MAGIC # Catalyst optimizes to:
# MAGIC df.filter(col("age") > 25)  # Computed once
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **4. Join Optimization**
# MAGIC
# MAGIC - **Broadcast Join**: Small tables (<10MB) broadcasted
# MAGIC - **Sort Merge Join**: Large tables
# MAGIC - **Automatic selection**: Catalyst chooses best strategy
# MAGIC
# MAGIC ```python
# MAGIC # Catalyst automatically uses broadcast join for small tables
# MAGIC large_df.join(small_df, "key")  # No manual hint needed!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Viewing Query Plans:
# MAGIC
# MAGIC ```python
# MAGIC # View execution plan
# MAGIC df.explain()
# MAGIC
# MAGIC # Extended plan (all phases)
# MAGIC df.explain(mode="extended")
# MAGIC
# MAGIC # Formatted (readable)
# MAGIC df.explain(mode="formatted")
# MAGIC ```
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC df.filter(col("age") > 25).select("name", "age").explain()
# MAGIC
# MAGIC # Output:
# MAGIC # == Physical Plan ==
# MAGIC # Project [name#10, age#11]         ← Select
# MAGIC # +- Filter (age#11 > 25)           ← Filter
# MAGIC #    +- FileScan parquet [name, age] ← Read only needed columns
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Adaptive Query Execution (AQE):
# MAGIC
# MAGIC Runtime optimization (Spark 3.0+).
# MAGIC
# MAGIC ```python
# MAGIC # Enable AQE (default in Databricks)
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC ```
# MAGIC
# MAGIC **AQE Benefits:**
# MAGIC - Dynamically coalesce partitions
# MAGIC - Switch join strategies at runtime
# MAGIC - Optimize skewed joins
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Best Practices:
# MAGIC
# MAGIC 1. ✅ **Trust Catalyst** - it's smart
# MAGIC 2. ✅ **Filter early** in your code
# MAGIC 3. ✅ **Select only needed columns**
# MAGIC 4. ✅ **Use explain()** to understand plans
# MAGIC 5. ✅ **Enable AQE** for runtime optimization

# COMMAND ----------

# DBTITLE 1,Spark UI - Monitoring and Debugging
# MAGIC %md
# MAGIC ## 17. Spark UI - Monitoring and Debugging
# MAGIC
# MAGIC **Spark UI** is the web interface for monitoring and debugging Spark applications.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Accessing Spark UI:
# MAGIC
# MAGIC **In Databricks:**
# MAGIC - Click on cluster name
# MAGIC - Click "Spark UI" tab
# MAGIC - Or: View it from notebook execution (click job link)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Tabs:
# MAGIC
# MAGIC **1. Jobs Tab**
# MAGIC - View all jobs (triggered by actions)
# MAGIC - See job duration
# MAGIC - Identify slow jobs
# MAGIC - Click to see stages
# MAGIC
# MAGIC **2. Stages Tab**
# MAGIC - View all stages in a job
# MAGIC - See stage duration
# MAGIC - Identify shuffle boundaries
# MAGIC - Check metrics: Shuffle Read, Shuffle Write
# MAGIC
# MAGIC **3. Storage Tab**
# MAGIC - View cached DataFrames
# MAGIC - Memory usage
# MAGIC - Cached partitions
# MAGIC - Storage level
# MAGIC
# MAGIC **4. Executors Tab**
# MAGIC - View all executors
# MAGIC - CPU utilization
# MAGIC - Memory usage per executor
# MAGIC - Task distribution
# MAGIC
# MAGIC **5. SQL Tab**
# MAGIC - View SQL queries
# MAGIC - Execution plans
# MAGIC - Query duration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Metrics to Watch:
# MAGIC
# MAGIC **Performance Indicators:**
# MAGIC
# MAGIC 1. **Duration**
# MAGIC    - Job duration
# MAGIC    - Stage duration
# MAGIC    - Task duration
# MAGIC    - Identify bottlenecks
# MAGIC
# MAGIC 2. **Shuffle**
# MAGIC    - Shuffle Read (input to stage)
# MAGIC    - Shuffle Write (output from stage)
# MAGIC    - Large shuffle = expensive operation
# MAGIC
# MAGIC 3. **Task Metrics**
# MAGIC    - Task count
# MAGIC    - Min/median/max task time
# MAGIC    - Skewed tasks? (some much slower)
# MAGIC
# MAGIC 4. **Data Size**
# MAGIC    - Input size
# MAGIC    - Output size
# MAGIC    - Records read/written
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Common Issues:
# MAGIC
# MAGIC **1. Data Skew**
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Most tasks finish quickly
# MAGIC - Few tasks take very long (stragglers)
# MAGIC - Uneven partition sizes
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Repartition by different key
# MAGIC df.repartition("better_key")
# MAGIC
# MAGIC # Or add salt to skewed keys
# MAGIC df.withColumn("salted_key", concat(col("key"), lit("_"), (rand() * 10).cast("int")))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **2. Too Many/Few Partitions**
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Too many: Short tasks, high overhead
# MAGIC - Too few: Long tasks, underutilized cluster
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Adjust shuffle partitions
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "100")
# MAGIC
# MAGIC # Or repartition
# MAGIC df.repartition(100)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **3. Memory Issues**
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - OOM errors
# MAGIC - Spilling to disk
# MAGIC - Slow performance
# MAGIC
# MAGIC **Solution:**
# MAGIC - Reduce partition size
# MAGIC - Increase executor memory
# MAGIC - Filter/select earlier
# MAGIC - Don't use collect() on large data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **4. Excessive Shuffle**
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Large Shuffle Read/Write metrics
# MAGIC - Multiple stages
# MAGIC - Slow performance
# MAGIC
# MAGIC **Solution:**
# MAGIC - Filter before shuffle operations
# MAGIC - Broadcast small tables
# MAGIC - Pre-partition for joins
# MAGIC - Cache before expensive operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Debugging Workflow:
# MAGIC
# MAGIC ```
# MAGIC 1. Open Spark UI
# MAGIC    ↓
# MAGIC 2. Find slow job (Jobs tab)
# MAGIC    ↓
# MAGIC 3. Click to see stages
# MAGIC    ↓
# MAGIC 4. Identify slow stage
# MAGIC    ↓
# MAGIC 5. Check metrics:
# MAGIC    - Shuffle size?
# MAGIC    - Skewed tasks?
# MAGIC    - Many partitions?
# MAGIC    ↓
# MAGIC 6. Apply fixes
# MAGIC    ↓
# MAGIC 7. Re-run and verify
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Reading Task Metrics:
# MAGIC
# MAGIC **Example Task Metrics:**
# MAGIC ```
# MAGIC Task 1: 2s   (normal)
# MAGIC Task 2: 2s   (normal)
# MAGIC Task 3: 2s   (normal)
# MAGIC Task 4: 45s  (SKEW! - investigate this)
# MAGIC ```
# MAGIC
# MAGIC **Metrics to check:**
# MAGIC - Input Size: Is this task processing more data?
# MAGIC - Records: More records than others?
# MAGIC - Shuffle Read: Larger shuffle input?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Best Practices:
# MAGIC
# MAGIC 1. ✅ **Monitor Spark UI** during development
# MAGIC 2. ✅ **Look for stragglers** (slow tasks)
# MAGIC 3. ✅ **Check shuffle metrics** (minimize shuffle)
# MAGIC 4. ✅ **Balance partition sizes** (not too big/small)
# MAGIC 5. ✅ **Cache strategically** (monitor Storage tab)
# MAGIC 6. ✅ **Use explain()** to understand plans
# MAGIC 7. ✅ **Profile before optimizing** (measure first!)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Quick Health Check:
# MAGIC
# MAGIC **Good Signs:**
# MAGIC - ✅ Tasks complete in similar time
# MAGIC - ✅ Low shuffle read/write
# MAGIC - ✅ Even partition distribution
# MAGIC - ✅ High executor utilization
# MAGIC
# MAGIC **Bad Signs:**
# MAGIC - ❌ Few tasks much slower than others (skew)
# MAGIC - ❌ Large shuffle metrics
# MAGIC - ❌ Many small/few large partitions
# MAGIC - ❌ Spilling to disk
# MAGIC - ❌ OOM errors