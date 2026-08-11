# Databricks notebook source
# DBTITLE 1,Introduction to PySpark
# MAGIC %md
# MAGIC # PySpark and SQL Learning Module
# MAGIC
# MAGIC ## What is PySpark?
# MAGIC
# MAGIC PySpark is the Python API for Apache Spark, a unified analytics engine for large-scale data processing. It enables you to:
# MAGIC - Process large datasets in a distributed manner
# MAGIC - Perform data transformations and analytics
# MAGIC - Use SQL queries on structured data
# MAGIC - Build machine learning pipelines
# MAGIC
# MAGIC ## Key Features:
# MAGIC - **Distributed Computing**: Process data across multiple nodes
# MAGIC - **In-Memory Processing**: Fast data processing with caching
# MAGIC - **Unified API**: Work with DataFrames, SQL, Streaming, and ML
# MAGIC - **Lazy Evaluation**: Optimizes execution plans before running

# COMMAND ----------

# DBTITLE 1,Python Fundamentals for PySpark
# MAGIC %md
# MAGIC ## Python Fundamentals Required for PySpark
# MAGIC
# MAGIC Before diving into PySpark, you should be comfortable with:
# MAGIC
# MAGIC ### Basic Python Concepts:
# MAGIC - **Variables and Data Types**: strings, integers, floats, booleans, lists, tuples, dictionaries
# MAGIC - **Functions**: defining and calling functions
# MAGIC - **Control Flow**: if/else statements, loops (for, while)
# MAGIC - **List Comprehensions**: efficient list creation
# MAGIC - **Lambda Functions**: anonymous functions for simple operations
# MAGIC
# MAGIC ### Important Python Libraries:
# MAGIC - **datetime**: for date and time operations
# MAGIC - **collections**: for advanced data structures
# MAGIC - **functools**: for higher-order functions

# COMMAND ----------

# DBTITLE 1,Python Basics Example
# Python fundamentals examples

# Lists and list comprehensions
numbers = [1, 2, 3, 4, 5]
squares = [x**2 for x in numbers]
print(f"Squares: {squares}")

# Lambda functions
add_ten = lambda x: x + 10
print(f"Add 10 to 5: {add_ten(5)}")

# Dictionary operations
data = {"name": "Alice", "age": 30, "city": "New York"}
print(f"Person: {data['name']}, Age: {data['age']}")

# List filtering
even_numbers = list(filter(lambda x: x % 2 == 0, numbers))
print(f"Even numbers: {even_numbers}")

# COMMAND ----------

# DBTITLE 1,SparkSession Overview
# MAGIC %md
# MAGIC ## SparkSession
# MAGIC
# MAGIC SparkSession is the entry point for all Spark functionality. It's the unified interface to:
# MAGIC - Create DataFrames
# MAGIC - Read and write data
# MAGIC - Execute SQL queries
# MAGIC - Access Spark configuration
# MAGIC
# MAGIC ### Key Points:
# MAGIC - In Databricks notebooks, SparkSession is pre-created as `spark`
# MAGIC - It replaced the older `SparkContext`, `SQLContext`, and `HiveContext`
# MAGIC - You can access it directly without initialization

# COMMAND ----------

# DBTITLE 1,SparkSession Example
# SparkSession is already available as 'spark' in Databricks

# Display SparkSession information
print(f"Spark Version: {spark.version}")
print(f"Application Name: {spark.sparkContext.appName}")

# Get Spark configuration
print(f"\nSpark Configurations:")
for conf in spark.sparkContext.getConf().getAll():
    print(f"  {conf[0]}: {conf[1]}")

# COMMAND ----------

# DBTITLE 1,DataFrame Creation
# MAGIC %md
# MAGIC ## DataFrame Creation
# MAGIC
# MAGIC DataFrames are the primary data structure in PySpark. They represent distributed collections of data organized into named columns.
# MAGIC
# MAGIC ### Ways to Create DataFrames:
# MAGIC 1. **From Python lists/tuples**: Using `createDataFrame()`
# MAGIC 2. **From RDDs**: Converting RDDs to DataFrames
# MAGIC 3. **From external files**: CSV, JSON, Parquet, Delta, etc.
# MAGIC 4. **From SQL queries**: Using `spark.sql()`

# COMMAND ----------

# DBTITLE 1,Create DataFrame from Data
# Method 1: Create DataFrame from list of tuples
data = [
    (1, "Alice", 34, "Data Engineer"),
    (2, "Bob", 45, "Data Scientist"),
    (3, "Charlie", 28, "ML Engineer"),
    (4, "Diana", 32, "Analytics Engineer")
]

columns = ["id", "name", "age", "role"]
df = spark.createDataFrame(data, columns)

print("DataFrame created from list:")
df.show()

# Method 2: Create DataFrame from list of dictionaries
data_dict = [
    {"id": 5, "name": "Eve", "age": 29, "role": "Data Analyst"},
    {"id": 6, "name": "Frank", "age": 38, "role": "Solutions Architect"}
]

df_dict = spark.createDataFrame(data_dict)
print("\nDataFrame created from dictionaries:")
df_dict.show()

# COMMAND ----------

# DBTITLE 1,Reading Files - Overview
# MAGIC %md
# MAGIC ## Reading Files
# MAGIC
# MAGIC Spark can read data from various file formats efficiently. The most common formats are:
# MAGIC
# MAGIC ### File Formats:
# MAGIC - **CSV**: Comma-separated values, human-readable
# MAGIC - **JSON**: JavaScript Object Notation, semi-structured data
# MAGIC - **Parquet**: Columnar storage format, highly efficient
# MAGIC - **Delta**: ACID transactions, time travel, optimized for Databricks
# MAGIC
# MAGIC ### Reading Patterns:
# MAGIC - Use `spark.read` for batch processing
# MAGIC - Use `spark.readStream` for streaming data
# MAGIC - Specify schema for better performance and type safety

# COMMAND ----------

# DBTITLE 1,Create Sample Data Files
# First, let's create sample data to demonstrate reading files
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create sample data
sample_data = [
    (1, "Product A", "Electronics", 299.99, 50),
    (2, "Product B", "Clothing", 49.99, 100),
    (3, "Product C", "Electronics", 599.99, 25),
    (4, "Product D", "Home", 149.99, 75),
    (5, "Product E", "Clothing", 29.99, 200)
]

schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock", IntegerType(), True)
])

products_df = spark.createDataFrame(sample_data, schema)
print("Sample products data created:")
products_df.show()

# COMMAND ----------

# DBTITLE 1,Reading CSV Files
# MAGIC %md
# MAGIC ## Reading CSV Files
# MAGIC
# MAGIC CSV (Comma-Separated Values) is a common format for structured data.
# MAGIC
# MAGIC ### Key Options:
# MAGIC - `header`: Whether the first row contains column names (true/false)
# MAGIC - `inferSchema`: Automatically detect data types (true/false)
# MAGIC - `delimiter`: Character separating values (default: comma)
# MAGIC - `quote`: Character for quoted strings
# MAGIC - `escape`: Character to escape special characters

# COMMAND ----------

# DBTITLE 1,Write and Read CSV
# Write DataFrame to CSV
csv_path = "/tmp/products.csv"
products_df.write.mode("overwrite").option("header", "true").csv(csv_path)
print(f"CSV file written to: {csv_path}")

# Read CSV with options
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path)

print("\nDataFrame read from CSV:")
df_csv.show()
print("\nSchema:")
df_csv.printSchema()

# COMMAND ----------

# DBTITLE 1,Reading JSON Files
# MAGIC %md
# MAGIC ## Reading JSON Files
# MAGIC
# MAGIC JSON (JavaScript Object Notation) is ideal for semi-structured and nested data.
# MAGIC
# MAGIC ### JSON Features:
# MAGIC - Supports nested structures (objects, arrays)
# MAGIC - Schema inference available
# MAGIC - Can handle multi-line JSON records
# MAGIC
# MAGIC ### Key Options:
# MAGIC - `multiLine`: Set to true for pretty-printed JSON (default: false)
# MAGIC - `primitivesAsString`: Load all primitive values as strings

# COMMAND ----------

# DBTITLE 1,Write and Read JSON
# Write DataFrame to JSON
json_path = "/tmp/products.json"
products_df.write.mode("overwrite").json(json_path)
print(f"JSON file written to: {json_path}")

# Read JSON
df_json = spark.read.json(json_path)

print("\nDataFrame read from JSON:")
df_json.show()
print("\nSchema:")
df_json.printSchema()

# COMMAND ----------

# DBTITLE 1,Reading Parquet Files
# MAGIC %md
# MAGIC ## Reading Parquet Files
# MAGIC
# MAGIC Parquet is a columnar storage format optimized for analytics workloads.
# MAGIC
# MAGIC ### Parquet Advantages:
# MAGIC - **Columnar storage**: Only read columns you need
# MAGIC - **Compression**: Efficient data compression
# MAGIC - **Schema embedded**: Schema stored with data
# MAGIC - **Fast queries**: Optimized for analytical queries
# MAGIC - **Type-safe**: Preserves data types

# COMMAND ----------

# DBTITLE 1,Write and Read Parquet
# Write DataFrame to Parquet
parquet_path = "/tmp/products.parquet"
products_df.write.mode("overwrite").parquet(parquet_path)
print(f"Parquet file written to: {parquet_path}")

# Read Parquet
df_parquet = spark.read.parquet(parquet_path)

print("\nDataFrame read from Parquet:")
df_parquet.show()
print("\nSchema:")
df_parquet.printSchema()

# COMMAND ----------

# DBTITLE 1,Reading Delta Files
# MAGIC %md
# MAGIC ## Reading Delta Lake Format
# MAGIC
# MAGIC Delta Lake is an open-source storage layer that brings ACID transactions to data lakes.
# MAGIC
# MAGIC ### Delta Lake Benefits:
# MAGIC - **ACID Transactions**: Reliable, concurrent operations
# MAGIC - **Time Travel**: Query historical versions of data
# MAGIC - **Schema Evolution**: Handle schema changes gracefully
# MAGIC - **Optimized Performance**: Z-ordering, data skipping
# MAGIC - **Unified Batch & Streaming**: Same API for both

# COMMAND ----------

# DBTITLE 1,Write and Read Delta
# Write DataFrame to Delta format
delta_path = "/tmp/products_delta"
products_df.write.mode("overwrite").format("delta").save(delta_path)
print(f"Delta table written to: {delta_path}")

# Read Delta
df_delta = spark.read.format("delta").load(delta_path)

print("\nDataFrame read from Delta:")
df_delta.show()

# Delta-specific operations
print("\nDelta table history:")
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, delta_path)
delta_table.history().select("version", "operation", "operationMetrics").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Schema Definition
# MAGIC %md
# MAGIC ## Working with Schemas
# MAGIC
# MAGIC Schemas define the structure of your DataFrame, including column names and data types.
# MAGIC
# MAGIC ### Benefits of Explicit Schemas:
# MAGIC - **Type safety**: Catch data type issues early
# MAGIC - **Performance**: Skip schema inference (faster reads)
# MAGIC - **Clarity**: Document expected data structure
# MAGIC - **Validation**: Enforce data quality
# MAGIC
# MAGIC ### Common Data Types:
# MAGIC - `StringType()`, `IntegerType()`, `LongType()`, `DoubleType()`, `FloatType()`
# MAGIC - `BooleanType()`, `DateType()`, `TimestampType()`
# MAGIC - `ArrayType()`, `MapType()`, `StructType()`

# COMMAND ----------

# DBTITLE 1,Schema Examples
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, ArrayType

# Define explicit schema
schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("customer_name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=True),
    StructField("purchase_amount", DoubleType(), nullable=True),
    StructField("purchase_date", DateType(), nullable=True),
    StructField("tags", ArrayType(StringType()), nullable=True)
])

print("Defined Schema:")
schema.printTreeString()

# View existing DataFrame schema
print("\nProducts DataFrame Schema:")
products_df.printSchema()

# Get schema as DDL string
ddl_schema = products_df.schema.simpleString()
print(f"\nSchema as DDL: {ddl_schema}")

# COMMAND ----------

# DBTITLE 1,Select Operations
# MAGIC %md
# MAGIC ## Select - Choosing Columns
# MAGIC
# MAGIC The `select()` operation allows you to choose specific columns from a DataFrame.
# MAGIC
# MAGIC ### Usage Patterns:
# MAGIC - Select by column name: `df.select("col1", "col2")`
# MAGIC - Select using Column objects: `df.select(col("col1"), col("col2"))`
# MAGIC - Select with expressions: `df.select(col("price") * 1.1)`
# MAGIC - Select all: `df.select("*")`

# COMMAND ----------

# DBTITLE 1,Select Examples
from pyspark.sql.functions import col, expr

# Select specific columns
print("Select specific columns:")
products_df.select("product_name", "price").show()

# Select with column expressions
print("\nSelect with calculations:")
products_df.select(
    col("product_name"),
    col("price"),
    (col("price") * 1.1).alias("price_with_tax")
).show()

# Select using expr()
print("\nSelect using expr():")
products_df.select(
    "product_name",
    expr("price * stock as total_value")
).show()

# COMMAND ----------

# DBTITLE 1,WithColumn - Adding/Modifying Columns
# MAGIC %md
# MAGIC ## withColumn - Adding or Modifying Columns
# MAGIC
# MAGIC The `withColumn()` method adds a new column or replaces an existing one.
# MAGIC
# MAGIC ### Key Points:
# MAGIC - Creates a new DataFrame (immutable operations)
# MAGIC - Can derive columns from existing ones
# MAGIC - Use for calculations, transformations, type casting
# MAGIC - Can be chained for multiple operations
# MAGIC
# MAGIC ### Syntax:
# MAGIC ```python
# MAGIC df.withColumn("new_col", expression)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,WithColumn Examples
from pyspark.sql.functions import col, lit, round

# Add new column with calculation
df_with_tax = products_df.withColumn("price_with_tax", col("price") * 1.1)

print("Added price_with_tax column:")
df_with_tax.select("product_name", "price", "price_with_tax").show()

# Add multiple columns using chaining
df_enriched = products_df \
    .withColumn("price_with_tax", round(col("price") * 1.1, 2)) \
    .withColumn("total_value", col("price") * col("stock")) \
    .withColumn("discount_price", col("price") * 0.9) \
    .withColumn("in_stock", col("stock") > 0)

print("\nMultiple columns added:")
df_enriched.show()

# COMMAND ----------

# DBTITLE 1,Filter and Where
# MAGIC %md
# MAGIC ## filter() and where() - Filtering Rows
# MAGIC
# MAGIC Both `filter()` and `where()` do the same thing - filter rows based on conditions. They are interchangeable.
# MAGIC
# MAGIC ### Filter Patterns:
# MAGIC - Single condition: `df.filter(col("price") > 100)`
# MAGIC - Multiple conditions (AND): `df.filter((col("price") > 100) & (col("stock") > 50))`
# MAGIC - Multiple conditions (OR): `df.filter((col("price") > 500) | (col("category") == "Electronics"))`
# MAGIC - String operations: `df.filter(col("name").startswith("A"))`
# MAGIC
# MAGIC **Important**: Use `&` for AND, `|` for OR (not `and`/`or`), and wrap conditions in parentheses!

# COMMAND ----------

# DBTITLE 1,Filter/Where Examples
from pyspark.sql.functions import col

# Single condition filter
print("Products with price > 100:")
products_df.filter(col("price") > 100).show()

# Multiple conditions with AND
print("\nElectronics with price > 200:")
products_df.filter(
    (col("category") == "Electronics") & (col("price") > 200)
).show()

# Multiple conditions with OR
print("\nElectronics OR products priced under 50:")
products_df.filter(
    (col("category") == "Electronics") | (col("price") < 50)
).show()

# Using where() - same as filter()
print("\nUsing where() - Low stock items:")
products_df.where(col("stock") < 100).show()

# String filtering
print("\nProducts starting with 'Product C':")
products_df.filter(col("product_name").startswith("Product C")).show()

# COMMAND ----------

# DBTITLE 1,When and Otherwise
# MAGIC %md
# MAGIC ## when() and otherwise() - Conditional Logic
# MAGIC
# MAGIC The `when()` and `otherwise()` functions implement if-then-else logic in PySpark.
# MAGIC
# MAGIC ### Pattern:
# MAGIC ```python
# MAGIC when(condition, value)
# MAGIC   .when(another_condition, another_value)
# MAGIC   .otherwise(default_value)
# MAGIC ```
# MAGIC
# MAGIC ### Use Cases:
# MAGIC - Creating categorical columns
# MAGIC - Implementing business rules
# MAGIC - Data classification
# MAGIC - Conditional transformations

# COMMAND ----------

# DBTITLE 1,When/Otherwise Examples
from pyspark.sql.functions import when, col

# Simple when/otherwise
df_categorized = products_df.withColumn(
    "price_category",
    when(col("price") >= 500, "Premium")
    .when(col("price") >= 100, "Mid-range")
    .otherwise("Budget")
)

print("Price categories:")
df_categorized.select("product_name", "price", "price_category").show()

# Multiple conditions
df_stock_status = products_df.withColumn(
    "stock_status",
    when(col("stock") == 0, "Out of Stock")
    .when(col("stock") < 50, "Low Stock")
    .when(col("stock") < 100, "Medium Stock")
    .otherwise("In Stock")
)

print("\nStock status:")
df_stock_status.select("product_name", "stock", "stock_status").show()

# Nested conditions
df_recommendation = products_df.withColumn(
    "recommendation",
    when((col("category") == "Electronics") & (col("price") < 400), "Great Deal!")
    .when((col("stock") < 30), "Low Stock - Order Soon")
    .otherwise("Available")
)

print("\nRecommendations:")
df_recommendation.select("product_name", "category", "price", "stock", "recommendation").show()

# COMMAND ----------

# DBTITLE 1,Cast - Type Conversion
# MAGIC %md
# MAGIC ## cast() - Data Type Conversion
# MAGIC
# MAGIC The `cast()` method converts columns from one data type to another.
# MAGIC
# MAGIC ### Common Conversions:
# MAGIC - String to Integer: `.cast("integer")` or `.cast(IntegerType())`
# MAGIC - String to Double: `.cast("double")`
# MAGIC - Integer to String: `.cast("string")`
# MAGIC - String to Date: `.cast("date")`
# MAGIC - String to Timestamp: `.cast("timestamp")`
# MAGIC
# MAGIC ### Important:
# MAGIC - Invalid conversions result in `null` values
# MAGIC - Always validate data before casting

# COMMAND ----------

# DBTITLE 1,Cast Examples
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, DoubleType

# Create sample data with string numbers
string_data = [
    ("1", "100.50", "2024-01-01"),
    ("2", "200.75", "2024-01-02"),
    ("3", "300.25", "2024-01-03")
]

df_strings = spark.createDataFrame(string_data, ["id_str", "amount_str", "date_str"])

print("Original DataFrame (all strings):")
df_strings.printSchema()
df_strings.show()

# Cast to appropriate types
df_casted = df_strings \
    .withColumn("id", col("id_str").cast(IntegerType())) \
    .withColumn("amount", col("amount_str").cast("double")) \
    .withColumn("date", col("date_str").cast("date"))

print("\nAfter casting:")
df_casted.printSchema()
df_casted.show()

# Cast products price to integer (will truncate decimals)
print("\nCasting price to integer:")
products_df.select(
    "product_name",
    col("price"),
    col("price").cast("integer").alias("price_int")
).show()

# COMMAND ----------

# DBTITLE 1,Drop, Rename, and Alias
# MAGIC %md
# MAGIC ## drop() - Remove Columns
# MAGIC
# MAGIC Removes one or more columns from a DataFrame.
# MAGIC
# MAGIC ## withColumnRenamed() - Rename Columns
# MAGIC
# MAGIC Renames a single column at a time.
# MAGIC
# MAGIC ## alias() - Column Aliases
# MAGIC
# MAGIC Provides temporary names for columns in expressions.
# MAGIC
# MAGIC ### Key Differences:
# MAGIC - `drop()`: Removes columns permanently (in new DataFrame)
# MAGIC - `withColumnRenamed()`: Changes column name
# MAGIC - `alias()`: Creates temporary name in select/expression

# COMMAND ----------

# DBTITLE 1,Drop/Rename/Alias Examples
from pyspark.sql.functions import col

# Drop single column
print("Drop stock column:")
products_df.drop("stock").show()

# Drop multiple columns
print("\nDrop multiple columns:")
products_df.drop("stock", "category").show()

# Rename column
print("\nRename product_name to name:")
df_renamed = products_df.withColumnRenamed("product_name", "name")
df_renamed.show()

# Multiple renames (chaining)
df_multi_rename = products_df \
    .withColumnRenamed("product_id", "id") \
    .withColumnRenamed("product_name", "name") \
    .withColumnRenamed("category", "cat")

print("\nMultiple renames:")
df_multi_rename.show()

# Using alias in select
print("\nUsing alias in select:")
products_df.select(
    col("product_name").alias("name"),
    col("price").alias("unit_price"),
    (col("price") * col("stock")).alias("inventory_value")
).show()

# COMMAND ----------

# DBTITLE 1,GroupBy and Aggregations
# MAGIC %md
# MAGIC ## groupBy() and Aggregations
# MAGIC
# MAGIC The `groupBy()` method groups rows by one or more columns, followed by aggregation functions.
# MAGIC
# MAGIC ### Common Aggregation Functions:
# MAGIC - `count()`: Count rows in each group
# MAGIC - `sum()`: Sum values in each group
# MAGIC - `avg()` / `mean()`: Average values
# MAGIC - `min()` / `max()`: Minimum/Maximum values
# MAGIC - `collect_list()`: Collect values into a list
# MAGIC - `collect_set()`: Collect unique values into a set
# MAGIC
# MAGIC ### Pattern:
# MAGIC ```python
# MAGIC df.groupBy("column1", "column2").agg(...)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,GroupBy and Aggregation Examples
from pyspark.sql.functions import count, sum, avg, min, max, round, collect_list

# Simple groupBy with count
print("Count products by category:")
products_df.groupBy("category").count().show()

# Multiple aggregations
print("\nMultiple aggregations by category:")
products_df.groupBy("category").agg(
    count("product_id").alias("product_count"),
    round(avg("price"), 2).alias("avg_price"),
    sum("stock").alias("total_stock"),
    min("price").alias("min_price"),
    max("price").alias("max_price")
).show()

# GroupBy with multiple columns
sales_data = [
    ("Electronics", "North", 1000),
    ("Electronics", "South", 1500),
    ("Clothing", "North", 800),
    ("Clothing", "South", 1200),
    ("Electronics", "North", 900)
]

sales_df = spark.createDataFrame(sales_data, ["category", "region", "sales"])

print("\nGroup by multiple columns:")
sales_df.groupBy("category", "region").agg(
    sum("sales").alias("total_sales"),
    count("*").alias("num_transactions")
).show()

# Collect values into list
print("\nCollect product names by category:")
products_df.groupBy("category").agg(
    collect_list("product_name").alias("products")
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Distinct and DropDuplicates
# MAGIC %md
# MAGIC ## distinct() and dropDuplicates()
# MAGIC
# MAGIC Both methods remove duplicate rows, but with different approaches.
# MAGIC
# MAGIC ### distinct():
# MAGIC - Removes completely duplicate rows
# MAGIC - No parameters
# MAGIC - Considers all columns
# MAGIC
# MAGIC ### dropDuplicates():
# MAGIC - Can specify subset of columns
# MAGIC - More flexible
# MAGIC - Keeps first occurrence
# MAGIC
# MAGIC ### Syntax:
# MAGIC ```python
# MAGIC df.distinct()  # All columns
# MAGIC df.dropDuplicates(["col1", "col2"])  # Specific columns
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Distinct/DropDuplicates Examples
# Create data with duplicates
dup_data = [
    (1, "A", "X"),
    (1, "A", "X"),  # Complete duplicate
    (2, "B", "Y"),
    (2, "B", "Z"),  # Duplicate on first two columns
    (3, "C", "X")
]

df_dup = spark.createDataFrame(dup_data, ["id", "name", "category"])

print("Original DataFrame with duplicates:")
df_dup.show()

# Remove complete duplicates using distinct()
print("\nUsing distinct() - removes complete duplicate rows:")
df_dup.distinct().show()

# Remove duplicates based on specific columns
print("\nUsing dropDuplicates(['id', 'name']) - keeps first occurrence:")
df_dup.dropDuplicates(["id", "name"]).show()

# Get distinct categories from products
print("\nDistinct categories:")
products_df.select("category").distinct().show()

# COMMAND ----------

# DBTITLE 1,OrderBy and Sort
# MAGIC %md
# MAGIC ## orderBy() and sort() - Sorting Data
# MAGIC
# MAGIC Both `orderBy()` and `sort()` do the same thing - sort DataFrame rows.
# MAGIC
# MAGIC ### Sorting Options:
# MAGIC - Ascending (default): `df.orderBy("column")`
# MAGIC - Descending: `df.orderBy(col("column").desc())`
# MAGIC - Multiple columns: `df.orderBy("col1", "col2")`
# MAGIC - Using asc() and desc(): `df.orderBy(col("price").desc(), col("name").asc())`
# MAGIC
# MAGIC ### Important:
# MAGIC - Sorting can be expensive on large datasets
# MAGIC - Consider using limit() after sorting
# MAGIC - Sort is a wide transformation (triggers shuffle)

# COMMAND ----------

# DBTITLE 1,OrderBy/Sort Examples
from pyspark.sql.functions import col, desc, asc

# Sort by single column (ascending - default)
print("Sort by price (ascending):")
products_df.orderBy("price").show()

# Sort descending
print("\nSort by price (descending):")
products_df.orderBy(col("price").desc()).show()

# Sort by multiple columns
print("\nSort by category (asc) and price (desc):")
products_df.orderBy(
    col("category").asc(),
    col("price").desc()
).show()

# Using sort() - same as orderBy()
print("\nUsing sort() - by stock ascending:")
products_df.sort("stock").show()

# Top N records
print("\nTop 3 most expensive products:")
products_df.orderBy(col("price").desc()).limit(3).show()

# COMMAND ----------

# DBTITLE 1,Explode - Working with Arrays
# MAGIC %md
# MAGIC ## explode() - Expanding Arrays
# MAGIC
# MAGIC The `explode()` function creates a new row for each element in an array or map column.
# MAGIC
# MAGIC ### Key Points:
# MAGIC - Transforms array elements into separate rows
# MAGIC - One-to-many transformation
# MAGIC - Useful for flattening nested data
# MAGIC - Related functions: `explode_outer()` (keeps nulls), `posexplode()` (includes position)
# MAGIC
# MAGIC ### Use Cases:
# MAGIC - Processing JSON arrays
# MAGIC - Unnesting hierarchical data
# MAGIC - Working with multi-valued columns

# COMMAND ----------

# DBTITLE 1,Explode Examples
from pyspark.sql.functions import explode, explode_outer, posexplode, array, col

# Create data with arrays
array_data = [
    (1, "Alice", ["Python", "SQL", "Spark"]),
    (2, "Bob", ["Java", "Scala"]),
    (3, "Charlie", ["R", "Python", "SQL", "Tableau"]),
    (4, "Diana", [])  # Empty array
]

df_array = spark.createDataFrame(array_data, ["id", "name", "skills"])

print("Original DataFrame with arrays:")
df_array.show(truncate=False)

# Explode array into separate rows
print("\nAfter explode (empty arrays disappear):")
df_exploded = df_array.select(
    "id",
    "name",
    explode("skills").alias("skill")
)
df_exploded.show()

# Explode_outer keeps rows with empty arrays
print("\nUsing explode_outer (keeps empty arrays as null):")
df_array.select(
    "id",
    "name",
    explode_outer("skills").alias("skill")
).show()

# Posexplode includes position/index
print("\nUsing posexplode (includes position):")
df_array.select(
    "id",
    "name",
    posexplode("skills").alias("position", "skill")
).show()

# COMMAND ----------

# DBTITLE 1,Nested JSON - Struct, Array, Map
# MAGIC %md
# MAGIC ## Working with Nested JSON Data
# MAGIC
# MAGIC PySpark provides powerful tools for working with complex nested data structures.
# MAGIC
# MAGIC ### Data Structures:
# MAGIC - **Struct**: Like a JSON object, accessed with dot notation
# MAGIC - **Array**: Ordered list of elements, indexed with brackets
# MAGIC - **Map**: Key-value pairs
# MAGIC
# MAGIC ### Common Operations:
# MAGIC - Access nested fields: `col("struct_col.field")`
# MAGIC - Extract from arrays: `col("array_col")[0]`
# MAGIC - Get map values: `col("map_col")["key"]`
# MAGIC - Flatten structures: Use `select()` with nested paths

# COMMAND ----------

# DBTITLE 1,Nested JSON Examples
from pyspark.sql.functions import col, struct, array, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Create nested data with Struct
nested_data = [
    (1, ("Alice", "alice@email.com", "New York"), ["Python", "Spark"]),
    (2, ("Bob", "bob@email.com", "Los Angeles"), ["Java", "Scala"]),
    (3, ("Charlie", "charlie@email.com", "Chicago"), ["R", "SQL"])
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("contact", StructType([
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("skills", ArrayType(StringType()), True)
])

df_nested = spark.createDataFrame(nested_data, schema)

print("Nested DataFrame structure:")
df_nested.printSchema()
df_nested.show(truncate=False)

# Access nested struct fields
print("\nAccess nested struct fields:")
df_nested.select(
    "id",
    col("contact.name").alias("name"),
    col("contact.email").alias("email"),
    col("contact.city").alias("city")
).show()

# Access array elements
print("\nAccess first skill from array:")
df_nested.select(
    "id",
    col("contact.name").alias("name"),
    col("skills")[0].alias("primary_skill")
).show()

# Flatten completely
print("\nFlattened structure:")
df_flattened = df_nested.select(
    "id",
    col("contact.name").alias("name"),
    col("contact.email").alias("email"),
    col("contact.city").alias("city"),
    explode("skills").alias("skill")
)
df_flattened.show()

# COMMAND ----------

# DBTITLE 1,Complex JSON Flattening
# MAGIC %md
# MAGIC ## Flattening Complex Nested Structures
# MAGIC
# MAGIC When dealing with real-world JSON data, you often need to flatten multiple levels of nesting.
# MAGIC
# MAGIC ### Strategies:
# MAGIC 1. **Select nested paths**: Use dot notation for structs
# MAGIC 2. **Explode arrays**: Convert arrays to rows
# MAGIC 3. **Chain operations**: Combine select and explode
# MAGIC 4. **Use wildcards**: `col("struct.*")` to expand all fields
# MAGIC
# MAGIC ### Best Practices:
# MAGIC - Define schema explicitly for complex JSON
# MAGIC - Use `multiLine=True` for pretty-printed JSON
# MAGIC - Handle nulls with `explode_outer()`

# COMMAND ----------

# DBTITLE 1,JSON Flattening Example
# Create complex nested JSON
import json

json_data = [
    json.dumps({
        "order_id": 1,
        "customer": {
            "id": 101,
            "name": "Alice",
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "zip": "10001"
            }
        },
        "items": [
            {"product": "Laptop", "quantity": 1, "price": 1200},
            {"product": "Mouse", "quantity": 2, "price": 25}
        ]
    }),
    json.dumps({
        "order_id": 2,
        "customer": {
            "id": 102,
            "name": "Bob",
            "address": {
                "street": "456 Oak Ave",
                "city": "Los Angeles",
                "zip": "90001"
            }
        },
        "items": [
            {"product": "Keyboard", "quantity": 1, "price": 75}
        ]
    })
]

df_json = spark.read.json(spark.sparkContext.parallelize(json_data))

print("Complex nested JSON structure:")
df_json.printSchema()
df_json.show(truncate=False)

# Flatten step by step
print("\nStep 1: Flatten customer and keep items:")
df_step1 = df_json.select(
    "order_id",
    col("customer.id").alias("customer_id"),
    col("customer.name").alias("customer_name"),
    col("customer.address.city").alias("city"),
    col("customer.address.zip").alias("zip"),
    "items"
)
df_step1.show(truncate=False)

# Explode items array
print("\nStep 2: Explode items array:")
df_flattened = df_step1.select(
    "order_id",
    "customer_id",
    "customer_name",
    "city",
    "zip",
    explode("items").alias("item")
).select(
    "order_id",
    "customer_id",
    "customer_name",
    "city",
    "zip",
    col("item.product").alias("product"),
    col("item.quantity").alias("quantity"),
    col("item.price").alias("price")
)

df_flattened.show()

# COMMAND ----------

# DBTITLE 1,Spark SQL - Introduction
# MAGIC %md
# MAGIC # Spark SQL
# MAGIC
# MAGIC ## What is Spark SQL?
# MAGIC
# MAGIC Spark SQL is a Spark module for structured data processing. It provides:
# MAGIC - SQL interface to query data
# MAGIC - DataFrame API integration
# MAGIC - Ability to mix SQL and DataFrame operations
# MAGIC - Optimization through Catalyst query optimizer
# MAGIC
# MAGIC ## Using SQL in PySpark:
# MAGIC 1. **Create temporary views** from DataFrames
# MAGIC 2. **Execute SQL queries** using `spark.sql()`
# MAGIC 3. **Results returned as DataFrames**
# MAGIC
# MAGIC ## Key Benefits:
# MAGIC - Familiar SQL syntax
# MAGIC - Seamless integration with DataFrames
# MAGIC - Query optimization
# MAGIC - Support for complex queries (CTEs, subqueries, window functions)

# COMMAND ----------

# DBTITLE 1,Register DataFrames as Temp Views
# Create temporary view from DataFrame
products_df.createOrReplaceTempView("products")

print("Temporary view 'products' created")

# Verify by running a simple query
result = spark.sql("SELECT * FROM products LIMIT 3")
result.show()

# Create additional sample data for joins
customers_data = [
    (101, "Alice", "alice@email.com", "New York"),
    (102, "Bob", "bob@email.com", "Los Angeles"),
    (103, "Charlie", "charlie@email.com", "Chicago")
]

orders_data = [
    (1, 101, 1, 2, "2024-01-15"),
    (2, 101, 3, 1, "2024-01-16"),
    (3, 102, 2, 3, "2024-01-17"),
    (4, 103, 1, 1, "2024-01-18"),
    (5, 103, 4, 2, "2024-01-19")
]

customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "email", "city"])
orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "product_id", "quantity", "order_date"])

customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")

print("\nAll temporary views created successfully")

# COMMAND ----------

# DBTITLE 1,SQL - SELECT and WHERE
# MAGIC %md
# MAGIC ## SELECT and WHERE Clauses
# MAGIC
# MAGIC The foundation of SQL queries:
# MAGIC
# MAGIC ### SELECT:
# MAGIC - Choose specific columns: `SELECT col1, col2`
# MAGIC - All columns: `SELECT *`
# MAGIC - Calculated columns: `SELECT price * 1.1 AS price_with_tax`
# MAGIC - Aggregate functions: `SELECT COUNT(*), AVG(price)`
# MAGIC
# MAGIC ### WHERE:
# MAGIC - Filter rows: `WHERE price > 100`
# MAGIC - Multiple conditions: `WHERE price > 100 AND stock > 50`
# MAGIC - Pattern matching: `WHERE name LIKE 'Product%'`
# MAGIC - IN clause: `WHERE category IN ('Electronics', 'Home')`

# COMMAND ----------

# DBTITLE 1,SELECT and WHERE Examples
# MAGIC %sql
# MAGIC -- Select specific columns
# MAGIC SELECT product_name, price, category
# MAGIC FROM products;
# MAGIC
# MAGIC -- WHERE with single condition
# MAGIC SELECT product_name, price
# MAGIC FROM products
# MAGIC WHERE price > 100;
# MAGIC
# MAGIC -- WHERE with multiple conditions
# MAGIC SELECT product_name, price, stock
# MAGIC FROM products
# MAGIC WHERE category = 'Electronics' AND price < 500;
# MAGIC
# MAGIC -- WHERE with OR
# MAGIC SELECT product_name, category, price
# MAGIC FROM products
# MAGIC WHERE category = 'Electronics' OR price < 50;
# MAGIC
# MAGIC -- WHERE with IN
# MAGIC SELECT product_name, category
# MAGIC FROM products
# MAGIC WHERE category IN ('Electronics', 'Clothing');
# MAGIC
# MAGIC -- Calculated columns
# MAGIC SELECT 
# MAGIC     product_name,
# MAGIC     price,
# MAGIC     price * 1.1 AS price_with_tax,
# MAGIC     price * stock AS inventory_value
# MAGIC FROM products;

# COMMAND ----------

# DBTITLE 1,SQL - GROUP BY and Aggregations
# MAGIC %md
# MAGIC ## GROUP BY and Aggregations
# MAGIC
# MAGIC Group rows and apply aggregate functions:
# MAGIC
# MAGIC ### Aggregate Functions:
# MAGIC - `COUNT(*)`: Count all rows
# MAGIC - `COUNT(column)`: Count non-null values
# MAGIC - `SUM(column)`: Sum of values
# MAGIC - `AVG(column)`: Average value
# MAGIC - `MIN(column)`, `MAX(column)`: Min/Max values
# MAGIC - `COLLECT_LIST(column)`: Collect values into array
# MAGIC
# MAGIC ### GROUP BY:
# MAGIC - Single column: `GROUP BY category`
# MAGIC - Multiple columns: `GROUP BY category, region`
# MAGIC - Must include all non-aggregated columns in SELECT

# COMMAND ----------

# DBTITLE 1,GROUP BY Examples
# MAGIC %sql
# MAGIC -- Simple GROUP BY with COUNT
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) AS product_count
# MAGIC FROM products
# MAGIC GROUP BY category;
# MAGIC
# MAGIC -- Multiple aggregations
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) AS product_count,
# MAGIC     ROUND(AVG(price), 2) AS avg_price,
# MAGIC     SUM(stock) AS total_stock,
# MAGIC     MIN(price) AS min_price,
# MAGIC     MAX(price) AS max_price
# MAGIC FROM products
# MAGIC GROUP BY category;
# MAGIC
# MAGIC -- GROUP BY with calculated fields
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     SUM(price * stock) AS total_inventory_value
# MAGIC FROM products
# MAGIC GROUP BY category
# MAGIC ORDER BY total_inventory_value DESC;

# COMMAND ----------

# DBTITLE 1,SQL - HAVING Clause
# MAGIC %md
# MAGIC ## HAVING Clause
# MAGIC
# MAGIC The HAVING clause filters grouped data (like WHERE for aggregations).
# MAGIC
# MAGIC ### Key Differences:
# MAGIC - **WHERE**: Filters rows BEFORE grouping
# MAGIC - **HAVING**: Filters groups AFTER aggregation
# MAGIC
# MAGIC ### Usage:
# MAGIC ```sql
# MAGIC SELECT category, COUNT(*)
# MAGIC FROM products
# MAGIC WHERE price > 50          -- Filter individual rows first
# MAGIC GROUP BY category
# MAGIC HAVING COUNT(*) > 1       -- Then filter groups
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,HAVING Examples
# MAGIC %sql
# MAGIC -- HAVING with COUNT
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) AS product_count
# MAGIC FROM products
# MAGIC GROUP BY category
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC -- HAVING with AVG
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     ROUND(AVG(price), 2) AS avg_price,
# MAGIC     COUNT(*) AS product_count
# MAGIC FROM products
# MAGIC GROUP BY category
# MAGIC HAVING AVG(price) > 100;
# MAGIC
# MAGIC -- Combining WHERE and HAVING
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     SUM(stock) AS total_stock,
# MAGIC     COUNT(*) AS product_count
# MAGIC FROM products
# MAGIC WHERE price > 50                    -- Filter rows first
# MAGIC GROUP BY category
# MAGIC HAVING SUM(stock) > 50              -- Then filter groups
# MAGIC ORDER BY total_stock DESC;

# COMMAND ----------

# DBTITLE 1,SQL - JOIN Operations
# MAGIC %md
# MAGIC ## JOIN Operations
# MAGIC
# MAGIC JOINs combine rows from two or more tables based on related columns.
# MAGIC
# MAGIC ### Types of JOINs:
# MAGIC - **INNER JOIN**: Returns matching rows from both tables (default)
# MAGIC - **LEFT JOIN** (LEFT OUTER): All rows from left table, matching from right
# MAGIC - **RIGHT JOIN** (RIGHT OUTER): All rows from right table, matching from left
# MAGIC - **FULL OUTER JOIN**: All rows from both tables
# MAGIC - **CROSS JOIN**: Cartesian product of both tables
# MAGIC
# MAGIC ### Syntax:
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM table1
# MAGIC JOIN table2 ON table1.key = table2.key
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,JOIN Examples
# MAGIC %sql
# MAGIC -- INNER JOIN (default)
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     c.name AS customer_name,
# MAGIC     c.city,
# MAGIC     p.product_name,
# MAGIC     o.quantity,
# MAGIC     p.price,
# MAGIC     o.quantity * p.price AS order_value
# MAGIC FROM orders o
# MAGIC INNER JOIN customers c ON o.customer_id = c.customer_id
# MAGIC INNER JOIN products p ON o.product_id = p.product_id;
# MAGIC
# MAGIC -- LEFT JOIN - all customers, even without orders
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.name,
# MAGIC     c.city,
# MAGIC     COUNT(o.order_id) AS order_count
# MAGIC FROM customers c
# MAGIC LEFT JOIN orders o ON c.customer_id = o.customer_id
# MAGIC GROUP BY c.customer_id, c.name, c.city;
# MAGIC
# MAGIC -- Multiple JOINs with aggregation
# MAGIC SELECT 
# MAGIC     c.name AS customer_name,
# MAGIC     c.city,
# MAGIC     COUNT(o.order_id) AS total_orders,
# MAGIC     SUM(o.quantity * p.price) AS total_spent
# MAGIC FROM customers c
# MAGIC LEFT JOIN orders o ON c.customer_id = o.customer_id
# MAGIC LEFT JOIN products p ON o.product_id = p.product_id
# MAGIC GROUP BY c.name, c.city
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# DBTITLE 1,SQL - CASE Statement
# MAGIC %md
# MAGIC ## CASE Statement - Conditional Logic
# MAGIC
# MAGIC The CASE statement implements if-then-else logic in SQL.
# MAGIC
# MAGIC ### Syntax:
# MAGIC ```sql
# MAGIC CASE
# MAGIC     WHEN condition1 THEN result1
# MAGIC     WHEN condition2 THEN result2
# MAGIC     ELSE default_result
# MAGIC END
# MAGIC ```
# MAGIC
# MAGIC ### Use Cases:
# MAGIC - Creating categories
# MAGIC - Conditional calculations
# MAGIC - Data transformation
# MAGIC - Business rule implementation

# COMMAND ----------

# DBTITLE 1,CASE Examples
# MAGIC %sql
# MAGIC -- Simple CASE for categorization
# MAGIC SELECT 
# MAGIC     product_name,
# MAGIC     price,
# MAGIC     CASE
# MAGIC         WHEN price >= 500 THEN 'Premium'
# MAGIC         WHEN price >= 100 THEN 'Mid-range'
# MAGIC         ELSE 'Budget'
# MAGIC     END AS price_category
# MAGIC FROM products;
# MAGIC
# MAGIC -- CASE with multiple conditions
# MAGIC SELECT 
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     stock,
# MAGIC     price,
# MAGIC     CASE
# MAGIC         WHEN stock = 0 THEN 'Out of Stock'
# MAGIC         WHEN stock < 50 THEN 'Low Stock'
# MAGIC         WHEN stock < 100 THEN 'Medium Stock'
# MAGIC         ELSE 'Well Stocked'
# MAGIC     END AS stock_status,
# MAGIC     CASE
# MAGIC         WHEN category = 'Electronics' AND price < 400 THEN 'Great Deal!'
# MAGIC         WHEN stock < 30 THEN 'Order Soon'
# MAGIC         ELSE 'Available'
# MAGIC     END AS recommendation
# MAGIC FROM products;
# MAGIC
# MAGIC -- CASE in aggregation
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) AS total_products,
# MAGIC     SUM(CASE WHEN price < 100 THEN 1 ELSE 0 END) AS budget_count,
# MAGIC     SUM(CASE WHEN price >= 100 AND price < 500 THEN 1 ELSE 0 END) AS midrange_count,
# MAGIC     SUM(CASE WHEN price >= 500 THEN 1 ELSE 0 END) AS premium_count
# MAGIC FROM products
# MAGIC GROUP BY category;

# COMMAND ----------

# DBTITLE 1,SQL - Common Table Expressions (CTEs)
# MAGIC %md
# MAGIC ## Common Table Expressions (CTEs)
# MAGIC
# MAGIC CTEs create temporary named result sets that exist only during query execution.
# MAGIC
# MAGIC ### Benefits:
# MAGIC - **Readability**: Break complex queries into logical steps
# MAGIC - **Reusability**: Reference the same subquery multiple times
# MAGIC - **Maintainability**: Easier to understand and modify
# MAGIC - **Organization**: Structure complex logic
# MAGIC
# MAGIC ### Syntax:
# MAGIC ```sql
# MAGIC WITH cte_name AS (
# MAGIC     SELECT ...
# MAGIC )
# MAGIC SELECT * FROM cte_name;
# MAGIC ```
# MAGIC
# MAGIC ### Multiple CTEs:
# MAGIC ```sql
# MAGIC WITH 
# MAGIC     cte1 AS (SELECT ...),
# MAGIC     cte2 AS (SELECT ...)
# MAGIC SELECT * FROM cte1 JOIN cte2 ...;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,CTE Examples
# MAGIC %sql
# MAGIC -- Simple CTE
# MAGIC WITH expensive_products AS (
# MAGIC     SELECT 
# MAGIC         product_name,
# MAGIC         category,
# MAGIC         price
# MAGIC     FROM products
# MAGIC     WHERE price > 100
# MAGIC )
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) AS expensive_count,
# MAGIC     ROUND(AVG(price), 2) AS avg_price
# MAGIC FROM expensive_products
# MAGIC GROUP BY category;
# MAGIC
# MAGIC -- Multiple CTEs
# MAGIC WITH 
# MAGIC     category_stats AS (
# MAGIC         SELECT 
# MAGIC             category,
# MAGIC             COUNT(*) AS product_count,
# MAGIC             AVG(price) AS avg_price,
# MAGIC             SUM(stock) AS total_stock
# MAGIC         FROM products
# MAGIC         GROUP BY category
# MAGIC     ),
# MAGIC     customer_orders AS (
# MAGIC         SELECT 
# MAGIC             customer_id,
# MAGIC             COUNT(*) AS order_count
# MAGIC         FROM orders
# MAGIC         GROUP BY customer_id
# MAGIC     )
# MAGIC SELECT * FROM category_stats;
# MAGIC
# MAGIC -- CTE with JOIN
# MAGIC WITH customer_spending AS (
# MAGIC     SELECT 
# MAGIC         c.customer_id,
# MAGIC         c.name,
# MAGIC         c.city,
# MAGIC         SUM(o.quantity * p.price) AS total_spent,
# MAGIC         COUNT(o.order_id) AS order_count
# MAGIC     FROM customers c
# MAGIC     JOIN orders o ON c.customer_id = o.customer_id
# MAGIC     JOIN products p ON o.product_id = p.product_id
# MAGIC     GROUP BY c.customer_id, c.name, c.city
# MAGIC )
# MAGIC SELECT 
# MAGIC     name,
# MAGIC     city,
# MAGIC     order_count,
# MAGIC     total_spent,
# MAGIC     ROUND(total_spent / order_count, 2) AS avg_order_value,
# MAGIC     CASE
# MAGIC         WHEN total_spent > 2000 THEN 'VIP'
# MAGIC         WHEN total_spent > 1000 THEN 'Premium'
# MAGIC         ELSE 'Regular'
# MAGIC     END AS customer_tier
# MAGIC FROM customer_spending
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# DBTITLE 1,SQL - Views
# MAGIC %md
# MAGIC ## Views - Saved Queries
# MAGIC
# MAGIC Views are saved SQL queries that can be treated like tables.
# MAGIC
# MAGIC ### Types of Views:
# MAGIC - **Temporary Views**: Exist only in current session
# MAGIC   - `CREATE OR REPLACE TEMP VIEW`
# MAGIC   - `createOrReplaceTempView()` in PySpark
# MAGIC - **Global Temp Views**: Shared across sessions
# MAGIC   - `CREATE OR REPLACE GLOBAL TEMP VIEW`
# MAGIC   - Access via `global_temp.view_name`
# MAGIC - **Permanent Views**: Saved in metastore (Unity Catalog)
# MAGIC   - `CREATE OR REPLACE VIEW`
# MAGIC
# MAGIC ### Benefits:
# MAGIC - Simplify complex queries
# MAGIC - Reuse common logic
# MAGIC - Security/access control
# MAGIC - Data abstraction

# COMMAND ----------

# DBTITLE 1,View Examples
# MAGIC %sql
# MAGIC -- Create a temporary view
# MAGIC CREATE OR REPLACE TEMP VIEW product_summary AS
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     price,
# MAGIC     stock,
# MAGIC     price * stock AS inventory_value,
# MAGIC     CASE
# MAGIC         WHEN price >= 500 THEN 'Premium'
# MAGIC         WHEN price >= 100 THEN 'Mid-range'
# MAGIC         ELSE 'Budget'
# MAGIC     END AS price_tier
# MAGIC FROM products;
# MAGIC
# MAGIC -- Query the view
# MAGIC SELECT * FROM product_summary;
# MAGIC
# MAGIC -- Create view with aggregation
# MAGIC CREATE OR REPLACE TEMP VIEW category_metrics AS
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) AS product_count,
# MAGIC     ROUND(AVG(price), 2) AS avg_price,
# MAGIC     SUM(stock) AS total_stock,
# MAGIC     SUM(price * stock) AS total_inventory_value
# MAGIC FROM products
# MAGIC GROUP BY category;
# MAGIC
# MAGIC -- Query the aggregated view
# MAGIC SELECT * FROM category_metrics
# MAGIC ORDER BY total_inventory_value DESC;
# MAGIC
# MAGIC -- Create view with JOINs
# MAGIC CREATE OR REPLACE TEMP VIEW customer_order_details AS
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.name AS customer_name,
# MAGIC     c.city,
# MAGIC     o.order_id,
# MAGIC     o.order_date,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     o.quantity,
# MAGIC     p.price,
# MAGIC     o.quantity * p.price AS order_line_value
# MAGIC FROM customers c
# MAGIC JOIN orders o ON c.customer_id = o.customer_id
# MAGIC JOIN products p ON o.product_id = p.product_id;
# MAGIC
# MAGIC -- Use the view
# MAGIC SELECT 
# MAGIC     customer_name,
# MAGIC     city,
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC     SUM(order_line_value) AS total_spent
# MAGIC FROM customer_order_details
# MAGIC GROUP BY customer_name, city
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# DBTITLE 1,SQL - CREATE TABLE
# MAGIC %md
# MAGIC ## CREATE TABLE - Persisting Data
# MAGIC
# MAGIC CREATE TABLE statements persist data in the metastore for permanent storage.
# MAGIC
# MAGIC ### Syntax Options:
# MAGIC ```sql
# MAGIC -- From existing data
# MAGIC CREATE TABLE table_name AS SELECT ...;
# MAGIC
# MAGIC -- With explicit schema
# MAGIC CREATE TABLE table_name (
# MAGIC     col1 datatype,
# MAGIC     col2 datatype
# MAGIC ) USING format;
# MAGIC
# MAGIC -- External table
# MAGIC CREATE TABLE table_name
# MAGIC USING format
# MAGIC LOCATION 'path';
# MAGIC ```
# MAGIC
# MAGIC ### Formats:
# MAGIC - `DELTA` (recommended for Databricks)
# MAGIC - `PARQUET`, `CSV`, `JSON`

# COMMAND ----------

# DBTITLE 1,CREATE TABLE Examples
# MAGIC %sql
# MAGIC -- Create table from query (CTAS - Create Table As Select)
# MAGIC CREATE OR REPLACE TABLE product_analytics AS
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     price,
# MAGIC     stock,
# MAGIC     price * stock AS inventory_value,
# MAGIC     CASE
# MAGIC         WHEN price >= 500 THEN 'Premium'
# MAGIC         WHEN price >= 100 THEN 'Mid-range'
# MAGIC         ELSE 'Budget'
# MAGIC     END AS price_tier,
# MAGIC     CASE
# MAGIC         WHEN stock < 50 THEN 'Low Stock'
# MAGIC         WHEN stock < 100 THEN 'Medium Stock'
# MAGIC         ELSE 'Well Stocked'
# MAGIC     END AS stock_status
# MAGIC FROM products;
# MAGIC
# MAGIC -- Verify table creation
# MAGIC SELECT * FROM product_analytics;
# MAGIC
# MAGIC -- Create aggregated table
# MAGIC CREATE OR REPLACE TABLE category_summary AS
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) AS product_count,
# MAGIC     ROUND(AVG(price), 2) AS avg_price,
# MAGIC     MIN(price) AS min_price,
# MAGIC     MAX(price) AS max_price,
# MAGIC     SUM(stock) AS total_stock,
# MAGIC     SUM(price * stock) AS total_inventory_value
# MAGIC FROM products
# MAGIC GROUP BY category;
# MAGIC
# MAGIC SELECT * FROM category_summary;

# COMMAND ----------

# DBTITLE 1,Summary and Best Practices
# MAGIC %md
# MAGIC # Summary and Best Practices
# MAGIC
# MAGIC ## Key Takeaways:
# MAGIC
# MAGIC ### PySpark DataFrames:
# MAGIC - DataFrames are immutable - operations return new DataFrames
# MAGIC - Use explicit schemas for better performance
# MAGIC - Leverage built-in functions instead of UDFs when possible
# MAGIC - Chain operations for readability
# MAGIC - Use `explain()` to understand query plans
# MAGIC
# MAGIC ### Spark SQL:
# MAGIC - Mix SQL and DataFrame operations seamlessly
# MAGIC - Use CTEs for complex queries
# MAGIC - Create views for reusable logic
# MAGIC - Optimize joins by broadcasting small tables
# MAGIC - Use DELTA format for production tables
# MAGIC
# MAGIC ## Performance Tips:
# MAGIC - **Partitioning**: Partition large tables by frequently filtered columns
# MAGIC - **Caching**: Cache DataFrames used multiple times
# MAGIC - **Broadcasting**: Broadcast small lookup tables in joins
# MAGIC - **Column Pruning**: Select only needed columns
# MAGIC - **Predicate Pushdown**: Filter early in the pipeline
# MAGIC - **Avoid Shuffles**: Minimize groupBy and orderBy operations
# MAGIC
# MAGIC ## Next Steps:
# MAGIC - Practice with real datasets
# MAGIC - Learn about Spark optimization techniques
# MAGIC - Explore window functions and advanced SQL
# MAGIC - Study Delta Lake features (time travel, MERGE, etc.)
# MAGIC - Understand Spark execution plans

# COMMAND ----------

# DBTITLE 1,Final Example - Complete Pipeline
# Complete end-to-end example combining everything
from pyspark.sql.functions import col, when, sum, avg, count, round

print("=" * 60)
print("COMPLETE PYSPARK PIPELINE EXAMPLE")
print("=" * 60)

# Step 1: Load data (already have products_df)
print("\n1. Source Data:")
products_df.show()

# Step 2: Data transformation
print("\n2. Transform Data:")
products_enriched = products_df \
    .withColumn("price_with_tax", round(col("price") * 1.1, 2)) \
    .withColumn("inventory_value", col("price") * col("stock")) \
    .withColumn("price_tier", 
        when(col("price") >= 500, "Premium")
        .when(col("price") >= 100, "Mid-range")
        .otherwise("Budget")
    ) \
    .withColumn("stock_status",
        when(col("stock") < 50, "Low")
        .when(col("stock") < 100, "Medium")
        .otherwise("High")
    )

products_enriched.show()

# Step 3: Aggregation
print("\n3. Aggregate by Category:")
category_agg = products_enriched \
    .groupBy("category") \
    .agg(
        count("*").alias("product_count"),
        round(avg("price"), 2).alias("avg_price"),
        sum("inventory_value").alias("total_inventory_value")
    ) \
    .orderBy(col("total_inventory_value").desc())

category_agg.show()

# Step 4: Filter and select
print("\n4. High-Value Products:")
high_value_products = products_enriched \
    .filter(col("inventory_value") > 5000) \
    .select(
        "product_name",
        "category",
        "price",
        "stock",
        "inventory_value",
        "price_tier"
    ) \
    .orderBy(col("inventory_value").desc())

high_value_products.show()

print("\n" + "=" * 60)
print("PIPELINE COMPLETE!")
print("=" * 60)