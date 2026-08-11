# Databricks notebook source
# DBTITLE 1,Module 3: Delta Lake Fundamentals
# MAGIC %md
# MAGIC # Module 3: Delta Lake Fundamentals
# MAGIC
# MAGIC ## Introduction
# MAGIC Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. It's the foundation of the Databricks Lakehouse architecture.
# MAGIC
# MAGIC ### Topics Covered:
# MAGIC 1. What is Delta Lake?
# MAGIC 2. Delta vs Parquet
# MAGIC 3. Managed vs External Tables
# MAGIC 4. Creating Delta Tables
# MAGIC 5. Reading Delta Tables
# MAGIC 6. INSERT / UPDATE / DELETE Operations
# MAGIC 7. MERGE Operations (Upserts)
# MAGIC 8. Hands-On Exercises
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,1. What is Delta Lake?
# MAGIC %md
# MAGIC ## 1. What is Delta Lake?
# MAGIC
# MAGIC **Delta Lake** is an open-source storage layer that sits on top of data lakes (Parquet files) and adds:
# MAGIC
# MAGIC ### Key Features:
# MAGIC
# MAGIC #### 1. **ACID Transactions**
# MAGIC * **Atomicity**: All-or-nothing operations
# MAGIC * **Consistency**: Data integrity maintained
# MAGIC * **Isolation**: Concurrent operations don't interfere
# MAGIC * **Durability**: Committed changes are permanent
# MAGIC
# MAGIC #### 2. **Time Travel (Versioning)**
# MAGIC * Query historical versions of data
# MAGIC * Rollback to previous states
# MAGIC * Audit data changes over time
# MAGIC
# MAGIC #### 3. **Schema Enforcement & Evolution**
# MAGIC * Prevents bad data from entering
# MAGIC * Validate data types and constraints
# MAGIC * Safely evolve schema over time
# MAGIC
# MAGIC #### 4. **Unified Batch & Streaming**
# MAGIC * Same table for both batch and streaming
# MAGIC * Exactly-once semantics
# MAGIC * No separate Lambda architecture needed
# MAGIC
# MAGIC #### 5. **Scalable Metadata Handling**
# MAGIC * Handles billions of files and partitions
# MAGIC * Fast metadata operations
# MAGIC
# MAGIC #### 6. **DML Operations**
# MAGIC * UPDATE, DELETE, MERGE support
# MAGIC * Not possible with plain Parquet
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │         Delta Lake Table                   │
# MAGIC ├────────────────────────────────────────────┤
# MAGIC │                                            │
# MAGIC │  Transaction Log (_delta_log/)             │
# MAGIC │  ├── 00000000000.json  ← Version 0        │
# MAGIC │  ├── 00000000001.json  ← Version 1        │
# MAGIC │  ├── 00000000002.json  ← Version 2        │
# MAGIC │  └── ...                                   │
# MAGIC │                                            │
# MAGIC │  Data Files (Parquet)                      │
# MAGIC │  ├── part-00000.snappy.parquet            │
# MAGIC │  ├── part-00001.snappy.parquet            │
# MAGIC │  ├── part-00002.snappy.parquet            │
# MAGIC │  └── ...                                   │
# MAGIC └────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Transaction Log:
# MAGIC The **transaction log** (`_delta_log/`) is the source of truth:
# MAGIC * JSON files tracking all changes
# MAGIC * Each file = one version/transaction
# MAGIC * Records: files added, removed, metadata changes
# MAGIC * Enables time travel and ACID guarantees
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why Delta Lake?
# MAGIC
# MAGIC **Without Delta Lake (Plain Parquet):**
# MAGIC ❌ No ACID transactions
# MAGIC ❌ No schema enforcement
# MAGIC ❌ No UPDATE/DELETE support
# MAGIC ❌ Difficult consistency guarantees
# MAGIC ❌ Data quality issues
# MAGIC
# MAGIC **With Delta Lake:**
# MAGIC ✅ ACID transactions
# MAGIC ✅ Schema enforcement
# MAGIC ✅ Full DML support (INSERT/UPDATE/DELETE/MERGE)
# MAGIC ✅ Time travel
# MAGIC ✅ Data quality guarantees
# MAGIC ✅ Unified batch + streaming
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Use Cases:
# MAGIC * **Data Warehousing**: Replace traditional data warehouses
# MAGIC * **ETL Pipelines**: Reliable data transformations
# MAGIC * **Streaming**: Real-time data ingestion with exactly-once semantics
# MAGIC * **ML Feature Stores**: Versioned, consistent training data
# MAGIC * **Compliance**: Audit trails and data versioning

# COMMAND ----------

# DBTITLE 1,2. Delta vs Parquet
# MAGIC %md
# MAGIC ## 2. Delta vs Parquet
# MAGIC
# MAGIC ### Parquet Overview
# MAGIC **Parquet** is a columnar storage format:
# MAGIC * Open-source
# MAGIC * Efficient compression
# MAGIC * Fast reads for analytics
# MAGIC * Just files - no metadata layer
# MAGIC
# MAGIC ### Delta Lake Overview
# MAGIC **Delta Lake** = Parquet files + Transaction log + ACID:
# MAGIC * Built on top of Parquet
# MAGIC * Adds reliability and consistency
# MAGIC * Enables DML operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Detailed Comparison:
# MAGIC
# MAGIC | Feature | Parquet | Delta Lake |
# MAGIC |---------|---------|------------|
# MAGIC | **File Format** | Columnar binary | Parquet + Transaction Log |
# MAGIC | **ACID Transactions** | ❌ No | ✅ Yes |
# MAGIC | **Schema Enforcement** | ❌ No | ✅ Yes |
# MAGIC | **Schema Evolution** | Manual | ✅ Automated |
# MAGIC | **Time Travel** | ❌ No | ✅ Yes (query old versions) |
# MAGIC | **UPDATE Support** | ❌ No | ✅ Yes |
# MAGIC | **DELETE Support** | ❌ No | ✅ Yes |
# MAGIC | **MERGE (Upsert)** | ❌ No | ✅ Yes |
# MAGIC | **Concurrent Writes** | ❌ Unsafe | ✅ Safe (optimistic concurrency) |
# MAGIC | **Data Versioning** | ❌ No | ✅ Yes |
# MAGIC | **Audit Trail** | ❌ No | ✅ Yes (transaction log) |
# MAGIC | **Rollback** | ❌ No | ✅ Yes |
# MAGIC | **Streaming Support** | Append-only | ✅ Full support |
# MAGIC | **File Compaction** | Manual | ✅ OPTIMIZE command |
# MAGIC | **Small File Problem** | Yes | ✅ Auto-compaction |
# MAGIC | **Z-Ordering** | ❌ No | ✅ Yes |
# MAGIC | **Performance** | Fast reads | ✅ Faster (caching + stats) |
# MAGIC | **Storage Cost** | Lower | Slightly higher (transaction log) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Differences Explained:
# MAGIC
# MAGIC #### 1. **ACID Transactions**
# MAGIC
# MAGIC **Parquet:**
# MAGIC ```python
# MAGIC # Multiple writers can corrupt data
# MAGIC df1.write.parquet("path/")  # Writer 1
# MAGIC df2.write.parquet("path/")  # Writer 2 - may corrupt!
# MAGIC ```
# MAGIC
# MAGIC **Delta:**
# MAGIC ```python
# MAGIC # Safe concurrent writes
# MAGIC df1.write.format("delta").save("path/")  # Writer 1
# MAGIC df2.write.format("delta").save("path/")  # Writer 2 - safe!
# MAGIC ```
# MAGIC
# MAGIC #### 2. **Updates & Deletes**
# MAGIC
# MAGIC **Parquet:**
# MAGIC ```python
# MAGIC # To update, must:
# MAGIC # 1. Read entire dataset
# MAGIC # 2. Filter/modify in memory
# MAGIC # 3. Overwrite everything
# MAGIC df = spark.read.parquet("path/")
# MAGIC df_updated = df.filter(col("id") != 5)  # "Delete" row
# MAGIC df_updated.write.mode("overwrite").parquet("path/")  # Rewrite all!
# MAGIC ```
# MAGIC
# MAGIC **Delta:**
# MAGIC ```python
# MAGIC # Direct DELETE statement
# MAGIC from delta.tables import DeltaTable
# MAGIC dt = DeltaTable.forPath(spark, "path/")
# MAGIC dt.delete("id = 5")  # Delete specific rows efficiently
# MAGIC ```
# MAGIC
# MAGIC #### 3. **Time Travel**
# MAGIC
# MAGIC **Parquet:**
# MAGIC ```python
# MAGIC # No history - manual backup required
# MAGIC df = spark.read.parquet("path/")  # Always latest
# MAGIC ```
# MAGIC
# MAGIC **Delta:**
# MAGIC ```python
# MAGIC # Query any version
# MAGIC df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("path/")
# MAGIC df_yesterday = spark.read.format("delta") \
# MAGIC     .option("timestampAsOf", "2026-08-10").load("path/")
# MAGIC ```
# MAGIC
# MAGIC #### 4. **Schema Enforcement**
# MAGIC
# MAGIC **Parquet:**
# MAGIC ```python
# MAGIC # Accepts any schema - data quality issues
# MAGIC df_bad_schema.write.mode("append").parquet("path/")  # No validation!
# MAGIC ```
# MAGIC
# MAGIC **Delta:**
# MAGIC ```python
# MAGIC # Validates schema on write
# MAGIC df_bad_schema.write.format("delta").mode("append").save("path/")
# MAGIC # Error: Schema mismatch!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use What?
# MAGIC
# MAGIC **Use Parquet when:**
# MAGIC * Simple read-only analytics
# MAGIC * No need for updates/deletes
# MAGIC * Single writer
# MAGIC * Immutable data
# MAGIC * Cost is critical (no transaction log overhead)
# MAGIC
# MAGIC **Use Delta Lake when:**
# MAGIC * Multiple concurrent writers
# MAGIC * Need UPDATE/DELETE/MERGE
# MAGIC * Data quality is critical
# MAGIC * Need audit trails
# MAGIC * Streaming + batch workloads
# MAGIC * Production data pipelines
# MAGIC * **Default choice for Databricks!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Migration: Parquet → Delta
# MAGIC
# MAGIC ```python
# MAGIC # Convert existing Parquet to Delta (in-place)
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC DeltaTable.convertToDelta(spark, "parquet.`/path/to/parquet`")
# MAGIC
# MAGIC # Or read and write as Delta
# MAGIC df = spark.read.parquet("/path/to/parquet")
# MAGIC df.write.format("delta").save("/path/to/delta")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,3. Managed vs External Tables
# MAGIC %md
# MAGIC ## 3. Managed vs External Tables
# MAGIC
# MAGIC In Databricks, Delta tables can be **Managed** or **External**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Managed Tables (Recommended)
# MAGIC
# MAGIC **Definition:** Databricks manages both metadata AND data files.
# MAGIC
# MAGIC **Characteristics:**
# MAGIC * Data stored in **Unity Catalog managed location**
# MAGIC * Databricks controls data lifecycle
# MAGIC * Dropping table deletes data files
# MAGIC * Default for `CREATE TABLE` without `LOCATION`
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC -- Managed table
# MAGIC CREATE TABLE catalog.schema.managed_table (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     created_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC ```
# MAGIC
# MAGIC **Storage Location:**
# MAGIC ```
# MAGIC /unity-catalog/<catalog>/<schema>/<table>/
# MAGIC ├── _delta_log/
# MAGIC └── part-*.parquet
# MAGIC ```
# MAGIC
# MAGIC **Lifecycle:**
# MAGIC ```sql
# MAGIC DROP TABLE catalog.schema.managed_table;
# MAGIC -- ✓ Metadata deleted
# MAGIC -- ✓ Data files deleted (cleaned up)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### External Tables
# MAGIC
# MAGIC **Definition:** You manage data files, Databricks manages only metadata.
# MAGIC
# MAGIC **Characteristics:**
# MAGIC * Data stored in **your specified location** (S3, ADLS, GCS)
# MAGIC * You control data lifecycle
# MAGIC * Dropping table keeps data files
# MAGIC * Requires `LOCATION` clause
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC -- External table
# MAGIC CREATE TABLE catalog.schema.external_table (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     created_at TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 's3://my-bucket/data/external_table/';
# MAGIC ```
# MAGIC
# MAGIC **Storage Location:**
# MAGIC ```
# MAGIC s3://my-bucket/data/external_table/
# MAGIC ├── _delta_log/
# MAGIC └── part-*.parquet
# MAGIC ```
# MAGIC
# MAGIC **Lifecycle:**
# MAGIC ```sql
# MAGIC DROP TABLE catalog.schema.external_table;
# MAGIC -- ✓ Metadata deleted
# MAGIC -- ✗ Data files remain (you must clean up manually)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Comparison Table:
# MAGIC
# MAGIC | Aspect | Managed Table | External Table |
# MAGIC |--------|---------------|----------------|
# MAGIC | **Data Location** | Unity Catalog controlled | User-specified (S3/ADLS/GCS) |
# MAGIC | **Ownership** | Databricks | User |
# MAGIC | **DROP TABLE** | Deletes data + metadata | Deletes metadata only |
# MAGIC | **Governance** | Full Unity Catalog | Limited (external location) |
# MAGIC | **Sharing** | Easy (within workspace) | Complex (need access to storage) |
# MAGIC | **Portability** | Limited to workspace | High (data independent) |
# MAGIC | **Cost** | Workspace storage | Your cloud storage |
# MAGIC | **Use Case** | Internal analytics | Shared data, legacy systems |
# MAGIC | **Default** | Yes (no LOCATION) | No (requires LOCATION) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Each?
# MAGIC
# MAGIC #### Use Managed Tables When:
# MAGIC ✅ Working within Databricks ecosystem  
# MAGIC ✅ Don't need direct file access  
# MAGIC ✅ Want Databricks to handle cleanup  
# MAGIC ✅ Using Unity Catalog governance  
# MAGIC ✅ **Default recommendation**
# MAGIC
# MAGIC #### Use External Tables When:
# MAGIC ✅ Sharing data with non-Databricks tools  
# MAGIC ✅ Data owned by another team/system  
# MAGIC ✅ Migrating from existing data lake  
# MAGIC ✅ Need direct cloud storage access  
# MAGIC ✅ Regulatory requirements for data location
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Code Examples:
# MAGIC
# MAGIC #### Creating Managed Table:
# MAGIC ```python
# MAGIC # Python DataFrame API
# MAGIC df.write.format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .saveAsTable("catalog.schema.managed_table")
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- SQL
# MAGIC CREATE TABLE catalog.schema.managed_table
# MAGIC AS SELECT * FROM source_data;
# MAGIC ```
# MAGIC
# MAGIC #### Creating External Table:
# MAGIC ```python
# MAGIC # Python DataFrame API
# MAGIC df.write.format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("path", "s3://bucket/path/") \
# MAGIC     .saveAsTable("catalog.schema.external_table")
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- SQL
# MAGIC CREATE TABLE catalog.schema.external_table
# MAGIC USING DELTA
# MAGIC LOCATION 's3://bucket/path/'
# MAGIC AS SELECT * FROM source_data;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Best Practices:
# MAGIC
# MAGIC 1. ✅ **Default to Managed Tables** unless you have a specific reason
# MAGIC 2. ✅ **Use External Tables** for shared data across systems
# MAGIC 3. ✅ **Document external locations** for team awareness
# MAGIC 4. ✅ **Set up proper IAM roles** for external table access
# MAGIC 5. ✅ **Use Unity Catalog External Locations** for governed access to external data

# COMMAND ----------

# DBTITLE 1,4. Creating Delta Tables
# MAGIC %md
# MAGIC ## 4. Creating Delta Tables
# MAGIC
# MAGIC There are multiple ways to create Delta tables in Databricks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Method 1: CREATE TABLE (SQL)
# MAGIC
# MAGIC #### Empty Table with Schema:
# MAGIC ```sql
# MAGIC CREATE TABLE catalog.schema.employees (
# MAGIC     employee_id INT NOT NULL,
# MAGIC     name STRING,
# MAGIC     department STRING,
# MAGIC     salary DECIMAL(10, 2),
# MAGIC     hire_date DATE,
# MAGIC     is_active BOOLEAN DEFAULT true
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Employee master data'
# MAGIC PARTITIONED BY (department);  -- Optional partitioning
# MAGIC ```
# MAGIC
# MAGIC #### Create Table As Select (CTAS):
# MAGIC ```sql
# MAGIC CREATE TABLE catalog.schema.high_earners
# MAGIC USING DELTA
# MAGIC COMMENT 'Employees with salary > 100K'
# MAGIC AS SELECT * 
# MAGIC FROM catalog.schema.employees 
# MAGIC WHERE salary > 100000;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Method 2: DataFrame API (Python)
# MAGIC
# MAGIC #### From DataFrame:
# MAGIC ```python
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
# MAGIC from pyspark.sql.functions import current_date
# MAGIC
# MAGIC # Create sample data
# MAGIC data = [
# MAGIC     (1, "Alice", "Engineering", 85000, "2020-01-15"),
# MAGIC     (2, "Bob", "Sales", 75000, "2019-06-10"),
# MAGIC     (3, "Carol", "Engineering", 95000, "2021-03-22")
# MAGIC ]
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("employee_id", IntegerType(), False),
# MAGIC     StructField("name", StringType(), True),
# MAGIC     StructField("department", StringType(), True),
# MAGIC     StructField("salary", IntegerType(), True),
# MAGIC     StructField("hire_date", StringType(), True)
# MAGIC ])
# MAGIC
# MAGIC df = spark.createDataFrame(data, schema)
# MAGIC
# MAGIC # Write as Delta table (Managed)
# MAGIC df.write.format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("overwriteSchema", "true") \
# MAGIC     .saveAsTable("catalog.schema.employees")
# MAGIC
# MAGIC print("✅ Delta table created successfully!")
# MAGIC ```
# MAGIC
# MAGIC #### Write to Path (External):
# MAGIC ```python
# MAGIC # Write to specific location
# MAGIC df.write.format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .save("s3://my-bucket/delta/employees/")
# MAGIC
# MAGIC # Then create table pointing to that location
# MAGIC spark.sql("""
# MAGIC     CREATE TABLE catalog.schema.employees
# MAGIC     USING DELTA
# MAGIC     LOCATION 's3://my-bucket/delta/employees/'
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Method 3: DeltaTable API
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC # Create or replace Delta table
# MAGIC DeltaTable.createOrReplace(spark) \
# MAGIC     .tableName("catalog.schema.employees") \
# MAGIC     .addColumn("employee_id", "INT", nullable=False) \
# MAGIC     .addColumn("name", "STRING") \
# MAGIC     .addColumn("department", "STRING") \
# MAGIC     .addColumn("salary", "DECIMAL(10,2)") \
# MAGIC     .addColumn("hire_date", "DATE") \
# MAGIC     .partitionedBy("department") \
# MAGIC     .execute()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Common Options:
# MAGIC
# MAGIC | Option | Description | Example |
# MAGIC |--------|-------------|----------|
# MAGIC | **mode** | Write mode | `overwrite`, `append`, `ignore`, `error` |
# MAGIC | **overwriteSchema** | Replace schema | `"true"` or `"false"` |
# MAGIC | **partitionBy** | Partition columns | `.partitionBy("year", "month")` |
# MAGIC | **mergeSchema** | Merge new columns | `.option("mergeSchema", "true")` |
# MAGIC | **replaceWhere** | Conditional overwrite | `.option("replaceWhere", "date >= '2026-01-01'")` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Write Modes:
# MAGIC
# MAGIC ```python
# MAGIC # 1. overwrite - Replace all data
# MAGIC df.write.format("delta").mode("overwrite").saveAsTable("table")
# MAGIC
# MAGIC # 2. append - Add new data
# MAGIC df.write.format("delta").mode("append").saveAsTable("table")
# MAGIC
# MAGIC # 3. ignore - Skip if exists
# MAGIC df.write.format("delta").mode("ignore").saveAsTable("table")
# MAGIC
# MAGIC # 4. error - Fail if exists (default)
# MAGIC df.write.format("delta").mode("error").saveAsTable("table")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,5. Reading Delta Tables
# MAGIC %md
# MAGIC ## 5. Reading Delta Tables
# MAGIC
# MAGIC ### SQL Queries
# MAGIC
# MAGIC ```sql
# MAGIC -- Read entire table
# MAGIC SELECT * FROM catalog.schema.employees;
# MAGIC
# MAGIC -- With filters
# MAGIC SELECT name, salary 
# MAGIC FROM catalog.schema.employees 
# MAGIC WHERE department = 'Engineering' AND salary > 80000;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### DataFrame API
# MAGIC
# MAGIC ```python
# MAGIC # Read entire table
# MAGIC df = spark.table("catalog.schema.employees")
# MAGIC df.show()
# MAGIC
# MAGIC # Read from path
# MAGIC df = spark.read.format("delta").load("/path/to/table/")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Time Travel
# MAGIC
# MAGIC ```python
# MAGIC # Read version 0
# MAGIC df_v0 = spark.read.format("delta") \
# MAGIC     .option("versionAsOf", 0) \
# MAGIC     .table("catalog.schema.employees")
# MAGIC
# MAGIC # Read by timestamp
# MAGIC df_yesterday = spark.read.format("delta") \
# MAGIC     .option("timestampAsOf", "2026-08-10") \
# MAGIC     .table("catalog.schema.employees")
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- SQL syntax
# MAGIC SELECT * FROM catalog.schema.employees VERSION AS OF 0;
# MAGIC SELECT * FROM catalog.schema.employees TIMESTAMP AS OF '2026-08-10';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### View Table History
# MAGIC
# MAGIC ```sql
# MAGIC DESCRIBE HISTORY catalog.schema.employees;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,6. INSERT Operations
# MAGIC %md
# MAGIC ## 6. INSERT Operations
# MAGIC
# MAGIC ### SQL INSERT
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert single row
# MAGIC INSERT INTO catalog.schema.employees 
# MAGIC VALUES (4, 'David', 'Sales', 72000, '2022-05-10', true);
# MAGIC
# MAGIC -- Insert multiple rows
# MAGIC INSERT INTO catalog.schema.employees 
# MAGIC VALUES 
# MAGIC     (5, 'Eve', 'Engineering', 88000, '2021-09-15', true),
# MAGIC     (6, 'Frank', 'HR', 65000, '2020-11-20', true);
# MAGIC
# MAGIC -- Insert from SELECT
# MAGIC INSERT INTO catalog.schema.employees
# MAGIC SELECT * FROM catalog.schema.new_hires;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### DataFrame API
# MAGIC
# MAGIC ```python
# MAGIC # Create new data
# MAGIC new_employees = [(9, "Iris", "Engineering", 87000, "2023-06-15")]
# MAGIC df_new = spark.createDataFrame(new_employees, ["employee_id", "name", "department", "salary", "hire_date"])
# MAGIC
# MAGIC # Append to existing table
# MAGIC df_new.write.format("delta").mode("append").saveAsTable("catalog.schema.employees")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,7. UPDATE Operations
# MAGIC %md
# MAGIC ## 7. UPDATE Operations
# MAGIC
# MAGIC ### SQL UPDATE
# MAGIC
# MAGIC ```sql
# MAGIC -- Update with WHERE clause
# MAGIC UPDATE catalog.schema.employees
# MAGIC SET salary = salary * 1.15
# MAGIC WHERE department = 'Engineering';
# MAGIC
# MAGIC -- Update multiple columns
# MAGIC UPDATE catalog.schema.employees
# MAGIC SET salary = salary * 1.20, is_active = true
# MAGIC WHERE employee_id = 5;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### DeltaTable API
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC dt = DeltaTable.forName(spark, "catalog.schema.employees")
# MAGIC
# MAGIC # Simple update
# MAGIC dt.update(
# MAGIC     condition = "department = 'Engineering'",
# MAGIC     set = {"salary": "salary * 1.15"}
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Rollback
# MAGIC
# MAGIC ```sql
# MAGIC RESTORE TABLE catalog.schema.employees TO VERSION AS OF 2;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,8. DELETE Operations
# MAGIC %md
# MAGIC ## 8. DELETE Operations
# MAGIC
# MAGIC ### SQL DELETE
# MAGIC
# MAGIC ```sql
# MAGIC -- Delete specific rows
# MAGIC DELETE FROM catalog.schema.employees
# MAGIC WHERE employee_id = 5;
# MAGIC
# MAGIC -- Delete with complex conditions
# MAGIC DELETE FROM catalog.schema.employees
# MAGIC WHERE department = 'Sales' AND is_active = false;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### DeltaTable API
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC dt = DeltaTable.forName(spark, "catalog.schema.employees")
# MAGIC
# MAGIC # Delete specific rows
# MAGIC dt.delete("employee_id = 5")
# MAGIC
# MAGIC # Delete with conditions
# MAGIC dt.delete("department = 'Sales' AND is_active = false")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,9. MERGE Operations (Upserts)
# MAGIC %md
# MAGIC ## 9. MERGE Operations (Upserts)
# MAGIC
# MAGIC **MERGE** combines INSERT, UPDATE, and DELETE - also known as **UPSERT**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### SQL MERGE
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO catalog.schema.employees AS target
# MAGIC USING catalog.schema.new_employee_data AS source
# MAGIC ON target.employee_id = source.employee_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     target.name = source.name,
# MAGIC     target.salary = source.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (employee_id, name, department, salary, hire_date)
# MAGIC   VALUES (source.employee_id, source.name, source.department, source.salary, source.hire_date);
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### DeltaTable MERGE API
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC target = DeltaTable.forName(spark, "catalog.schema.employees")
# MAGIC
# MAGIC # Source data
# MAGIC source_data = [
# MAGIC     (1, "Alice Updated", "Engineering", 90000),
# MAGIC     (11, "New Employee", "Sales", 70000)
# MAGIC ]
# MAGIC source = spark.createDataFrame(source_data, ["employee_id", "name", "department", "salary"])
# MAGIC
# MAGIC # Perform MERGE
# MAGIC target.alias("target").merge(
# MAGIC     source.alias("source"),
# MAGIC     "target.employee_id = source.employee_id"
# MAGIC ).whenMatchedUpdate(set = {
# MAGIC     "name": "source.name",
# MAGIC     "salary": "source.salary"
# MAGIC }).whenNotMatchedInsert(values = {
# MAGIC     "employee_id": "source.employee_id",
# MAGIC     "name": "source.name",
# MAGIC     "department": "source.department",
# MAGIC     "salary": "source.salary"
# MAGIC }).execute()
# MAGIC
# MAGIC print("✅ MERGE completed!")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Use Cases:
# MAGIC * Slowly Changing Dimensions (SCD)
# MAGIC * Change Data Capture (CDC)
# MAGIC * Deduplication
# MAGIC * Data Synchronization

# COMMAND ----------

# DBTITLE 1,10. Hands-On Exercises
# MAGIC %md
# MAGIC ## 10. Hands-On Exercises
# MAGIC
# MAGIC Let's practice with real Delta Lake operations!
# MAGIC
# MAGIC ### Exercises:
# MAGIC 1. Create a Delta table
# MAGIC 2. Insert sample data
# MAGIC 3. Update records
# MAGIC 4. Delete records  
# MAGIC 5. Use MERGE for upserts
# MAGIC 6. Explore Time Travel
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Exercise 1: Create Sample Delta Table
# Exercise 1: Create a sample employees Delta table
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# Sample employee data
employee_data = [
    (1, "Alice Johnson", "Engineering", 85000, "2020-01-15", True),
    (2, "Bob Smith", "Sales", 75000, "2019-06-10", True),
    (3, "Carol Williams", "Engineering", 95000, "2021-03-22", True),
    (4, "David Brown", "Sales", 72000, "2022-05-10", True),
    (5, "Eve Davis", "HR", 68000, "2020-08-15", True)
]

schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("hire_date", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

df_employees = spark.createDataFrame(employee_data, schema)

# Create Delta table
df_employees.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("employees_demo")

print("✅ Delta table 'employees_demo' created!")
print("\nTable contents:")
spark.table("employees_demo").show()

# COMMAND ----------

# DBTITLE 1,Exercise 2: INSERT New Data
# Exercise 2: Insert new employees
new_employees = [
    (6, "Frank Miller", "Engineering", 90000, "2023-01-10", True),
    (7, "Grace Wilson", "Sales", 78000, "2023-03-15", True)
]

df_new = spark.createDataFrame(new_employees, schema)

# Append to table
df_new.write.format("delta") \
    .mode("append") \
    .saveAsTable("employees_demo")

print("✅ New employees inserted!")
spark.table("employees_demo").show()

# COMMAND ----------

# DBTITLE 1,Exercise 3: UPDATE Records
# Exercise 3: Give Engineering department a 10% raise
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "employees_demo")

dt.update(
    condition = "department = 'Engineering'",
    set = {"salary": "CAST(salary * 1.10 AS INT)"}
)

print("✅ Engineering salaries updated!")
spark.table("employees_demo").filter("department = 'Engineering'").show()

# COMMAND ----------

# DBTITLE 1,Exercise 4: DELETE Records
# Exercise 4: Remove employee with ID 5
dt = DeltaTable.forName(spark, "employees_demo")

dt.delete("employee_id = 5")

print("✅ Employee deleted!")
spark.table("employees_demo").show()

# COMMAND ----------

# DBTITLE 1,Exercise 5: MERGE (Upsert)
# Exercise 5: MERGE - Update existing, insert new
source_data = [
    (1, "Alice Johnson Updated", "Engineering", 95000, "2020-01-15", True),  # Update
    (8, "Henry Moore", "HR", 65000, "2023-06-01", True)  # Insert
]

df_source = spark.createDataFrame(source_data, schema)

dt = DeltaTable.forName(spark, "employees_demo")

dt.alias("target").merge(
    df_source.alias("source"),
    "target.employee_id = source.employee_id"
).whenMatchedUpdate(set = {
    "name": "source.name",
    "salary": "source.salary"
}).whenNotMatchedInsert(values = {
    "employee_id": "source.employee_id",
    "name": "source.name",
    "department": "source.department",
    "salary": "source.salary",
    "hire_date": "source.hire_date",
    "is_active": "source.is_active"
}).execute()

print("✅ MERGE completed!")
spark.table("employees_demo").orderBy("employee_id").show()

# COMMAND ----------

# DBTITLE 1,Exercise 6: Time Travel
# Exercise 6: View table history and time travel
dt = DeltaTable.forName(spark, "employees_demo")

# View history
print("Table History:")
history = dt.history()
history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# Read version 0 (original state)
print("\n📜 Version 0 (Original):")
df_v0 = spark.read.format("delta").option("versionAsOf", 0).table("employees_demo")
df_v0.show()

print("\n📜 Current Version:")
spark.table("employees_demo").show()

# COMMAND ----------

# DBTITLE 1,Exercise 7: Restore Previous Version
# MAGIC %sql
# MAGIC -- Exercise 7: Restore to a previous version (if needed)
# MAGIC -- Uncomment to restore:
# MAGIC -- RESTORE TABLE employees_demo TO VERSION AS OF 0;
# MAGIC
# MAGIC -- View current state
# MAGIC SELECT * FROM employees_demo ORDER BY employee_id;

# COMMAND ----------

# DBTITLE 1,Summary & Next Steps
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You've learned:
# MAGIC
# MAGIC ✅ **What is Delta Lake** - ACID transactions for data lakes  
# MAGIC ✅ **Delta vs Parquet** - Why Delta is superior for production  
# MAGIC ✅ **Managed vs External Tables** - Lifecycle management  
# MAGIC ✅ **Creating Delta Tables** - Multiple methods (SQL, DataFrame API, DeltaTable API)  
# MAGIC ✅ **Reading Delta Tables** - Including Time Travel  
# MAGIC ✅ **INSERT** - Adding new data  
# MAGIC ✅ **UPDATE** - Modifying existing records  
# MAGIC ✅ **DELETE** - Removing records  
# MAGIC ✅ **MERGE** - Upsert operations  
# MAGIC ✅ **Time Travel** - Query historical versions  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Takeaways:
# MAGIC
# MAGIC 1. **Delta Lake = Reliability** - ACID transactions prevent data corruption
# MAGIC 2. **Time Travel = Safety** - Always can rollback mistakes
# MAGIC 3. **DML Support = Flexibility** - UPDATE/DELETE/MERGE just work
# MAGIC 4. **Managed Tables = Simplicity** - Let Databricks handle the lifecycle
# MAGIC 5. **MERGE = Power** - Handle complex CDC scenarios elegantly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Next Topics:
# MAGIC * Delta Lake Optimization (OPTIMIZE, Z-ORDER)
# MAGIC * Change Data Feed (CDF)
# MAGIC * Delta Lake Constraints
# MAGIC * Liquid Clustering
# MAGIC * Delta Sharing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Resources:
# MAGIC * [Delta Lake Documentation](https://docs.delta.io/)
# MAGIC * [Databricks Delta Lake Guide](https://docs.databricks.com/delta/index.html)
# MAGIC * [Delta Lake GitHub](https://github.com/delta-io/delta)