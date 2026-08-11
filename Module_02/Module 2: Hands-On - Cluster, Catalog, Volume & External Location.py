# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Hands-On Lab Overview
# MAGIC %md
# MAGIC # Module 2: Hands-On Lab
# MAGIC ## Creating Clusters, Catalogs, Volumes & External Locations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Lab Objectives
# MAGIC
# MAGIC This hands-on lab will teach you how to:
# MAGIC
# MAGIC * 💻 **Create All-Purpose Clusters** - Launch and configure compute resources
# MAGIC * 🗂️ **Create Unity Catalog Objects** - Set up catalogs and schemas for data governance
# MAGIC * 📦 **Create Volumes** - Managed storage for files and unstructured data
# MAGIC * 🔗 **Configure External Locations** - Connect to cloud storage (S3, ADLS, GCS)
# MAGIC * 🔐 **Set Up Access Permissions** - Secure your data with proper access controls
# MAGIC
# MAGIC ### 📋 Prerequisites
# MAGIC
# MAGIC ✅ Databricks workspace (Premium or Enterprise edition for Unity Catalog)  
# MAGIC ✅ Cloud account with storage (AWS S3, Azure ADLS, or GCP GCS)  
# MAGIC ✅ Admin or elevated permissions in Databricks  
# MAGIC ✅ Basic understanding of cloud storage concepts
# MAGIC
# MAGIC ### 🛠️ What You'll Build
# MAGIC
# MAGIC ```
# MAGIC YOUR DATABRICKS ENVIRONMENT
# MAGIC ├── 💻 All-Purpose Cluster
# MAGIC │   └── Configured with libraries and settings
# MAGIC │
# MAGIC ├── 🗂️ Unity Catalog Structure
# MAGIC │   ├── Catalog: my_catalog
# MAGIC │   └── Schema: my_schema
# MAGIC │       ├── Tables (coming in next modules)
# MAGIC │       └── Volumes
# MAGIC │
# MAGIC ├── 📦 Volumes
# MAGIC │   ├── Managed Volume (Databricks-managed)
# MAGIC │   └── External Volume (your cloud storage)
# MAGIC │
# MAGIC └── 🔗 External Location
# MAGIC     └── Connection to S3/ADLS/GCS bucket
# MAGIC ```
# MAGIC
# MAGIC ### ⏱️ Estimated Time
# MAGIC
# MAGIC **45-60 minutes** (depending on your familiarity)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Let's get started!** 🚀

# COMMAND ----------

# DBTITLE 1,Part 1: Creating All-Purpose Clusters
# MAGIC %md
# MAGIC ## Part 1: Creating All-Purpose Clusters 💻
# MAGIC
# MAGIC ### What is an All-Purpose Cluster?
# MAGIC
# MAGIC An **All-Purpose Cluster** is an interactive compute resource that:
# MAGIC * Can be shared by multiple users
# MAGIC * Supports notebooks, jobs, and libraries
# MAGIC * Provides flexible configuration options
# MAGIC * Auto-terminates after inactivity to save costs
# MAGIC
# MAGIC ### When to Use All-Purpose Clusters vs Serverless
# MAGIC
# MAGIC | **Use Case** | **All-Purpose Cluster** | **Serverless** |
# MAGIC |--------------|------------------------|----------------|
# MAGIC | **Interactive development** | ✅ Good | ✅ Better (instant) |
# MAGIC | **Custom libraries** | ✅ Full control | ⚠️ Limited |
# MAGIC | **Specific DBR version** | ✅ Choose any | ⚠️ Latest only |
# MAGIC | **Cost optimization** | ⚠️ Need to manage | ✅ Auto-optimized |
# MAGIC | **GPU workloads** | ✅ Supported | ✅ Supported |
# MAGIC | **Long-running processes** | ✅ Good | ✅ Good |
# MAGIC
# MAGIC ### Cluster Creation Methods
# MAGIC
# MAGIC You can create clusters via:
# MAGIC 1. **Databricks UI** (easiest for beginners)
# MAGIC 2. **Databricks CLI** (for automation)
# MAGIC 3. **Databricks SDK** (programmatic access)
# MAGIC 4. **REST API** (advanced integration)
# MAGIC
# MAGIC We'll demonstrate **UI** and **SDK** methods below.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Method 1: Create Cluster via UI
# MAGIC
# MAGIC #### Step-by-Step Instructions:
# MAGIC
# MAGIC 1. **Navigate to Compute**
# MAGIC    * Click **Compute** in the left sidebar
# MAGIC    * Click **Create Compute** button
# MAGIC
# MAGIC 2. **Configure Cluster**
# MAGIC    * **Cluster name**: `my-learning-cluster`
# MAGIC    * **Policy**: Select a policy (if available) or leave as "Unrestricted"
# MAGIC    * **Access mode**: 
# MAGIC      - `Single User` - Recommended for individual work
# MAGIC      - `Shared` - For team collaboration (Premium+)
# MAGIC      - `No isolation shared` - Legacy mode
# MAGIC    * **Databricks Runtime**: 
# MAGIC      - `Runtime: 15.4 LTS` (Long-Term Support) - Recommended
# MAGIC      - Or latest available version
# MAGIC    * **Node type**:
# MAGIC      - **Driver**: `i3.xlarge` (AWS) / `Standard_D3_v2` (Azure)
# MAGIC      - **Workers**: Same as driver
# MAGIC    * **Worker configuration**:
# MAGIC      - Min workers: `1`
# MAGIC      - Max workers: `3` (for autoscaling)
# MAGIC      - OR Fixed: `2` workers
# MAGIC    * **Autoscaling**: ✅ Enable (recommended)
# MAGIC    * **Auto Termination**: `120` minutes (2 hours)
# MAGIC
# MAGIC 3. **Advanced Options** (optional)
# MAGIC    * **Spark config**: Custom Spark configurations
# MAGIC    * **Environment variables**: Set env vars
# MAGIC    * **Init scripts**: Run scripts at cluster startup
# MAGIC    * **Libraries**: Pre-install packages
# MAGIC    * **Logging**: Enable cluster logs
# MAGIC
# MAGIC 4. **Create Cluster**
# MAGIC    * Click **Create Compute**
# MAGIC    * Wait 3-5 minutes for cluster to start
# MAGIC    * Status will change from `Pending` → `Starting` → `Running`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Method 2: Create Cluster via SDK (Programmatic)
# MAGIC
# MAGIC Let's use the Databricks SDK to create a cluster programmatically:

# COMMAND ----------

# DBTITLE 1,Create Cluster Using Databricks SDK
# After creating your cluster via the UI (see instructions above),
# run this cell to verify it was created successfully

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print("📋 Listing All Clusters in Workspace:\n")
print(f"{'Cluster Name':<35} {'State':<15} {'Cluster ID'}")
print("=" * 95)

clusters = list(w.clusters.list())

if clusters:
    for cluster in clusters:
        print(f"{cluster.cluster_name:<35} {cluster.state.value:<15} {cluster.cluster_id}")
    print(f"\n✅ Total clusters found: {len(clusters)}")
    print("\n💡 If you just created a cluster via UI, you should see it listed above!")
else:
    print("⚠️ No clusters found in this workspace.")
    print("Please create a cluster using the UI instructions above.")

print("\n" + "="*95)
print("\n📝 Note: We'll learn how to create clusters programmatically using")
print("   the Databricks SDK in a later advanced module!")

# COMMAND ----------

# DBTITLE 1,List Existing Clusters
# List all clusters in the workspace
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print("📋 Existing Clusters in Workspace:\n")
print(f"{'Cluster Name':<30} {'State':<15} {'Cluster ID':<40}")
print("=" * 90)

clusters = w.clusters.list()
for cluster in clusters:
    print(f"{cluster.cluster_name:<30} {cluster.state.value:<15} {cluster.cluster_id:<40}")

print(f"\n✅ Total clusters: {len(list(w.clusters.list()))}")

# COMMAND ----------

# DBTITLE 1,Part 2: Unity Catalog - Creating Catalogs and Schemas
# MAGIC %md
# MAGIC ## Part 2: Unity Catalog - Creating Catalogs and Schemas 🗂️
# MAGIC
# MAGIC ### Unity Catalog Overview
# MAGIC
# MAGIC **Unity Catalog** is Databricks' unified governance solution for data and AI assets.
# MAGIC
# MAGIC ```
# MAGIC UNITY CATALOG HIERARCHY
# MAGIC
# MAGIC Metastore (per region)
# MAGIC     |
# MAGIC     ├── Catalog 1
# MAGIC     │   ├── Schema 1
# MAGIC     │   │   ├── Tables
# MAGIC     │   │   ├── Views
# MAGIC     │   │   ├── Functions
# MAGIC     │   │   └── Volumes
# MAGIC     │   └── Schema 2
# MAGIC     │
# MAGIC     └── Catalog 2
# MAGIC         └── Schema 1
# MAGIC ```
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC * **Metastore**: Top-level container (one per region, managed by Databricks)
# MAGIC * **Catalog**: Logical database (like a database in traditional systems)
# MAGIC * **Schema**: Collection of tables, views, functions, and volumes
# MAGIC * **Table**: Structured data (we'll create these in later modules)
# MAGIC * **Volume**: Unstructured data storage (files, images, etc.)
# MAGIC
# MAGIC ### Three-Level Namespace
# MAGIC
# MAGIC All objects in Unity Catalog use a three-level namespace:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM catalog.schema.table
# MAGIC                  ↑       ↑      ↑
# MAGIC               Catalog  Schema  Table
# MAGIC ```
# MAGIC
# MAGIC ### Why Use Unity Catalog?
# MAGIC
# MAGIC ✅ **Centralized governance** - Single source of truth  
# MAGIC ✅ **Fine-grained access control** - Column and row-level security  
# MAGIC ✅ **Data lineage** - Track data dependencies  
# MAGIC ✅ **Cross-workspace sharing** - Delta Sharing  
# MAGIC ✅ **Audit logging** - Who accessed what and when  
# MAGIC ✅ **Search and discovery** - Find data easily
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC ⚠️ **Unity Catalog requires**:
# MAGIC * Databricks Premium or Enterprise edition
# MAGIC * Metastore created (usually done by workspace admin)
# MAGIC * Appropriate permissions (account admin or metastore admin)
# MAGIC
# MAGIC If Unity Catalog is not available, you'll see an error. Contact your workspace admin.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's create a catalog and schema!

# COMMAND ----------

# DBTITLE 1,Check Unity Catalog Status
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print("🔍 Checking Unity Catalog Status...\n")

try:
    # Get current metastore info
    metastores = w.metastores.list()
    
    if metastores:
        print("✅ Unity Catalog is ENABLED\n")
        
        for metastore in metastores:
            print(f"Metastore Name: {metastore.name}")
            print(f"Metastore ID: {metastore.metastore_id}")
            print(f"Region: {metastore.region}")
            print(f"Cloud: {metastore.cloud}")
            print(f"Created: {metastore.created_at}")
            print("-" * 60)
    else:
        print("⚠️ Unity Catalog might not be configured")
        print("Contact your workspace administrator to enable Unity Catalog")
        
except Exception as e:
    print(f"❌ Unity Catalog check failed: {str(e)}")
    print("\nℹ️ This usually means:")
    print("  • Unity Catalog is not enabled (need Premium/Enterprise edition)")
    print("  • Insufficient permissions to view metastore")
    print("  • Metastore not created yet")

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog
# MAGIC %md
# MAGIC ### Create Unity Catalog - Manual Steps (UI)
# MAGIC
# MAGIC Let's create a catalog manually using the Databricks UI:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Step 1: Navigate to Catalog Explorer
# MAGIC
# MAGIC 1. Click **Catalog** in the left sidebar (or **Data** in older workspaces)
# MAGIC 2. This opens the **Catalog Explorer** (Unity Catalog browser)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Step 2: Create New Catalog
# MAGIC
# MAGIC 1. Click the **Create Catalog** button (top right or in the menu)
# MAGIC 2. **Catalog Creation Form** will appear:
# MAGIC
# MAGIC    **Catalog Name**: `nitya_cloudtech_catalog`
# MAGIC    * Must be unique in the metastore
# MAGIC    * Use lowercase, numbers, underscores only
# MAGIC    * Must start with a letter or underscore
# MAGIC    * Example: `your_name_catalog` or `learning_catalog_2024`
# MAGIC
# MAGIC    **Comment** (optional): `Learning catalog for Module 2 hands-on lab`
# MAGIC    * Helps others understand the catalog's purpose
# MAGIC
# MAGIC    **Storage Location** (optional): Leave blank
# MAGIC    * If blank, uses the metastore default storage
# MAGIC    * Advanced: Can specify custom S3/ADLS/GCS location
# MAGIC
# MAGIC 3. Click **Create**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Step 3: Verify Catalog Creation
# MAGIC
# MAGIC ✅ You should see your new catalog appear in the Catalog Explorer
# MAGIC
# MAGIC The catalog structure will look like:
# MAGIC ```
# MAGIC Catalogs
# MAGIC ├── main (default system catalog)
# MAGIC ├── hive_metastore (legacy)
# MAGIC └── nitya_cloudtech_catalog ← Your new catalog!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Step 4: Set Your Catalog as Current (Optional)
# MAGIC
# MAGIC Run this SQL command to make your catalog the default for this session:

# COMMAND ----------

# DBTITLE 1,Create Schema in Catalog
# MAGIC %sql
# MAGIC -- ⚠️ IMPORTANT: Replace 'nitya_cloudtech_catalog' with YOUR catalog name!
# MAGIC
# MAGIC -- Step 1: Set your catalog as the current catalog
# MAGIC USE CATALOG nitya_cloudtech_catalog;
# MAGIC
# MAGIC -- Step 2: Create Bronze schema (raw data layer)
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC   COMMENT 'Bronze layer - raw data ingestion';
# MAGIC
# MAGIC -- Verify it was created
# MAGIC SHOW SCHEMAS IN nitya_cloudtech_catalog;
# MAGIC
# MAGIC -- Check current catalog and schema
# MAGIC SELECT current_catalog() as current_catalog, current_schema() as current_schema;

# COMMAND ----------

# DBTITLE 1,Create Multiple Schemas (Medallion Architecture)
# MAGIC %sql
# MAGIC -- ⚠️ IMPORTANT: Replace 'nitya_cloudtech_catalog' with YOUR catalog name!
# MAGIC
# MAGIC -- Create Medallion Architecture Schemas
# MAGIC -- Bronze → Silver → Gold pattern
# MAGIC
# MAGIC USE CATALOG nitya_cloudtech_catalog;
# MAGIC
# MAGIC -- Create Silver schema (cleaned and validated data)
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC   COMMENT 'Silver layer - cleaned and validated data';
# MAGIC
# MAGIC -- Create Gold schema (business-level aggregates)
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC   COMMENT 'Gold layer - business-level aggregates and analytics';
# MAGIC
# MAGIC -- Verify all schemas were created
# MAGIC SHOW SCHEMAS IN nitya_cloudtech_catalog;
# MAGIC
# MAGIC -- ✅ You should now see:
# MAGIC -- bronze (raw data)
# MAGIC -- silver (cleaned data) 
# MAGIC -- gold (analytics-ready data)
# MAGIC --
# MAGIC -- 🎯 This is the standard medallion architecture pattern!

# COMMAND ----------

# DBTITLE 1,List Catalogs and Schemas
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print("📚 Unity Catalog Structure\n")
print("=" * 80)

try:
    # List all catalogs
    catalogs = w.catalogs.list()
    
    for catalog in catalogs:
        print(f"\n📦 Catalog: {catalog.name}")
        print(f"   Owner: {catalog.owner}")
        print(f"   Comment: {catalog.comment or 'No description'}")
        
        # List schemas in this catalog
        try:
            schemas = w.schemas.list(catalog_name=catalog.name)
            print(f"   Schemas:")
            for schema in schemas:
                print(f"     📂 {schema.name} - {schema.comment or 'No description'}")
        except Exception as e:
            print(f"     ⚠️ Cannot list schemas: {str(e)}")
    
    print("\n" + "=" * 80)
    
except Exception as e:
    print(f"❌ Error listing catalogs: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Part 3: Creating Volumes
# MAGIC %md
# MAGIC ## Part 3: Creating Volumes 📦
# MAGIC
# MAGIC ### What are Unity Catalog Volumes?
# MAGIC
# MAGIC **Volumes** are Unity Catalog objects that represent storage locations for **unstructured data** like:
# MAGIC * Files (CSV, JSON, Parquet, text, etc.)
# MAGIC * Images and videos
# MAGIC * ML models and artifacts
# MAGIC * Documents and PDFs
# MAGIC * Any non-tabular data
# MAGIC
# MAGIC ### Why Use Volumes?
# MAGIC
# MAGIC ```
# MAGIC Traditional Approach          Unity Catalog Volumes
# MAGIC ❌ DBFS paths (/dbfs/...)      ✅ /Volumes/catalog/schema/volume
# MAGIC ❌ No governance               ✅ Full Unity Catalog governance
# MAGIC ❌ No access control           ✅ Fine-grained permissions
# MAGIC ❌ No lineage                  ✅ Track file usage
# MAGIC ❌ No audit logging            ✅ Full audit trail
# MAGIC ```
# MAGIC
# MAGIC ### Volume Types
# MAGIC
# MAGIC #### 1. **Managed Volumes** (Databricks-managed storage)
# MAGIC * Databricks manages the storage
# MAGIC * Lifecycle tied to the volume object
# MAGIC * Delete volume → data is deleted
# MAGIC * Simpler to use, no cloud setup needed
# MAGIC
# MAGIC #### 2. **External Volumes** (your cloud storage)
# MAGIC * You manage the cloud storage (S3/ADLS/GCS)
# MAGIC * Lifecycle independent of volume object
# MAGIC * Delete volume → data remains in cloud
# MAGIC * More control, can access from outside Databricks
# MAGIC
# MAGIC ### Volume Path Format
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/<schema>/<volume>/<path/to/file>
# MAGIC
# MAGIC Example:
# MAGIC /Volumes/my_catalog/bronze/raw_data/customer_data.csv
# MAGIC          ↑           ↑      ↑         ↑
# MAGIC       Catalog    Schema  Volume    File path
# MAGIC ```
# MAGIC
# MAGIC ### Accessing Volumes
# MAGIC
# MAGIC You can access volumes using:
# MAGIC
# MAGIC ```python
# MAGIC # Python file I/O
# MAGIC with open('/Volumes/catalog/schema/volume/file.txt', 'r') as f:
# MAGIC     data = f.read()
# MAGIC
# MAGIC # Pandas
# MAGIC import pandas as pd
# MAGIC df = pd.read_csv('/Volumes/catalog/schema/volume/data.csv')
# MAGIC
# MAGIC # Spark
# MAGIC df = spark.read.csv('/Volumes/catalog/schema/volume/data.csv')
# MAGIC
# MAGIC # DBUtils
# MAGIC dbutils.fs.ls('/Volumes/catalog/schema/volume/')
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's create both types of volumes!

# COMMAND ----------

# DBTITLE 1,Create Managed Volume
# MAGIC %sql
# MAGIC -- ⚠️ IMPORTANT: Replace 'nitya_cloudtech_catalog' with YOUR catalog name!
# MAGIC
# MAGIC -- Create a MANAGED Volume for storing unstructured data (files, images, etc.)
# MAGIC -- Databricks manages the storage - delete the volume = delete the data
# MAGIC
# MAGIC USE CATALOG nitya_cloudtech_catalog;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS managed_files
# MAGIC   COMMENT 'Managed volume for raw file storage - Databricks manages the storage';
# MAGIC
# MAGIC -- Verify the volume was created
# MAGIC SHOW VOLUMES IN bronze;
# MAGIC
# MAGIC -- ✅ You can now access this volume at:
# MAGIC -- /Volumes/nitya_cloudtech_catalog/bronze/managed_files/
# MAGIC --
# MAGIC -- 📝 Usage examples (run in Python cells):
# MAGIC -- with open('/Volumes/nitya_cloudtech_catalog/bronze/managed_files/myfile.txt', 'w') as f:
# MAGIC --     f.write('Hello, Volumes!')
# MAGIC --
# MAGIC -- df.write.parquet('/Volumes/nitya_cloudtech_catalog/bronze/managed_files/data/')

# COMMAND ----------

# DBTITLE 1,Test Managed Volume - Write and Read Files
import os

# ⚠️ IMPORTANT: Replace with YOUR catalog name!
CATALOG_NAME = "nitya_cloudtech_catalog"  # <-- Change this to your catalog name

volume_path = f"/Volumes/{CATALOG_NAME}/bronze/managed_files"

print(f"🧪 Testing Managed Volume: {volume_path}\n")

try:
    # 1. Write a text file
    test_file = f"{volume_path}/test_file.txt"
    with open(test_file, 'w') as f:
        f.write("Hello from Unity Catalog Managed Volume!\n")
        f.write("This file is stored in Databricks-managed storage.\n")
        f.write("When you delete the volume, this file will be deleted too.\n")
    print("✅ Step 1: Text file written successfully")
    
    # 2. Read the file back
    with open(test_file, 'r') as f:
        content = f.read()
    print(f"✅ Step 2: File read successfully\n")
    print("File Content:")
    print("-" * 60)
    print(content)
    print("-" * 60)
    
    # 3. List files in volume
    files = dbutils.fs.ls(volume_path)
    print(f"\n✅ Step 3: Files in volume:")
    for file in files:
        print(f"  📄 {file.name} ({file.size} bytes)")
    
    # 4. Create a subdirectory and write a file
    subdir = f"{volume_path}/data"
    os.makedirs(subdir, exist_ok=True)
    
    csv_file = f"{subdir}/sample_data.csv"
    with open(csv_file, 'w') as f:
        f.write("id,name,value\n")
        f.write("1,Alice,100\n")
        f.write("2,Bob,200\n")
        f.write("3,Charlie,300\n")
    print(f"\n✅ Step 4: CSV file created in subdirectory")
    
    # 5. Read CSV with Pandas
    import pandas as pd
    df = pd.read_csv(csv_file)
    print(f"\n✅ Step 5: CSV read with Pandas:")
    print(df)
    
    print("\n" + "="*60)
    print("🎉 Managed Volume is working perfectly!")
    print("="*60)
    print(f"\n📂 Volume Path: {volume_path}")
    print(f"📊 Files Created: {len(files) + 2}")
    
except Exception as e:
    print(f"❌ Error testing volume: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Part 4: External Locations and External Volumes
# MAGIC %md
# MAGIC ## Part 4: External Locations and External Volumes 🔗
# MAGIC
# MAGIC ### What are External Locations?
# MAGIC
# MAGIC **External Locations** are Unity Catalog objects that represent cloud storage paths (S3, ADLS, GCS) that you own and manage.
# MAGIC
# MAGIC ```
# MAGIC External Location
# MAGIC       ↓
# MAGIC   s3://my-bucket/data/          (AWS)
# MAGIC   abfss://container@account/   (Azure)
# MAGIC   gs://my-bucket/data/          (GCP)
# MAGIC       ↑
# MAGIC    Your cloud storage
# MAGIC ```
# MAGIC
# MAGIC ### Why Use External Locations?
# MAGIC
# MAGIC ✅ **Unified access control** - Control via Unity Catalog, not cloud IAM  
# MAGIC ✅ **Credential management** - Centralized credential storage  
# MAGIC ✅ **Audit logging** - Track who accesses your cloud storage  
# MAGIC ✅ **Cross-workspace access** - Share storage across workspaces  
# MAGIC ✅ **Data portability** - Access same data from multiple tools
# MAGIC
# MAGIC ### External Location + External Volume Relationship
# MAGIC
# MAGIC ```
# MAGIC 1. Create STORAGE CREDENTIAL
# MAGIC    ├── AWS: IAM Role ARN
# MAGIC    ├── Azure: Service Principal or Managed Identity  
# MAGIC    └── GCP: Service Account
# MAGIC
# MAGIC 2. Create EXTERNAL LOCATION
# MAGIC    ├── URL: s3://bucket/path
# MAGIC    └── Credential: Reference to storage credential
# MAGIC
# MAGIC 3. Create EXTERNAL VOLUME
# MAGIC    ├── References the external location
# MAGIC    └── Provides /Volumes/... path
# MAGIC ```
# MAGIC
# MAGIC ### Prerequisites for External Location (AWS Example)
# MAGIC
# MAGIC ⚠️ **You need**:
# MAGIC 1. **S3 bucket** in your AWS account
# MAGIC 2. **IAM Role** with permissions:
# MAGIC    ```json
# MAGIC    {
# MAGIC      "Version": "2012-10-17",
# MAGIC      "Statement": [
# MAGIC        {
# MAGIC          "Effect": "Allow",
# MAGIC          "Action": [
# MAGIC            "s3:GetObject",
# MAGIC            "s3:PutObject",
# MAGIC            "s3:DeleteObject",
# MAGIC            "s3:ListBucket"
# MAGIC          ],
# MAGIC          "Resource": [
# MAGIC            "arn:aws:s3:::YOUR-BUCKET-NAME/*",
# MAGIC            "arn:aws:s3:::YOUR-BUCKET-NAME"
# MAGIC          ]
# MAGIC        }
# MAGIC      ]
# MAGIC    }
# MAGIC    ```
# MAGIC 3. **Trust relationship** allowing Databricks to assume the role
# MAGIC 4. **Unity Catalog admin** or **Account Admin** permissions in Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Setup Steps
# MAGIC
# MAGIC #### Step 1: AWS Setup (do this in AWS Console)
# MAGIC
# MAGIC 1. **Create S3 Bucket**
# MAGIC    * Name: `databricks-external-data-<your-org>`
# MAGIC    * Region: Same as your Databricks workspace
# MAGIC    * Block Public Access: ✅ Enabled
# MAGIC
# MAGIC 2. **Create IAM Role**
# MAGIC    * Name: `databricks-external-access-role`
# MAGIC    * Type: "AWS account" (for cross-account access)
# MAGIC    * Attach policy with S3 permissions (above)
# MAGIC    * Note the Role ARN: `arn:aws:iam::123456789012:role/databricks-external-access-role`
# MAGIC
# MAGIC 3. **Configure Trust Relationship**
# MAGIC    * Edit the role's trust relationship
# MAGIC    * Allow Databricks account to assume role (get this from Databricks docs)
# MAGIC
# MAGIC #### Step 2: Databricks Setup
# MAGIC
# MAGIC Now let's create the external location in Databricks:

# COMMAND ----------

# DBTITLE 1,Create Storage Credential (Requires Admin)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CreateStorageCredential,
    AwsIamRoleRequest
)

w = WorkspaceClient()

# IMPORTANT: Replace these with your actual AWS values
CREDENTIAL_NAME = "my_s3_credential"
AWS_IAM_ROLE_ARN = "arn:aws:iam::YOUR-ACCOUNT-ID:role/databricks-external-access-role"

print("🔐 Creating Storage Credential...\n")
print("⚠️ This requires Account Admin or Metastore Admin permissions\n")

try:
    # Create storage credential
    credential = w.storage_credentials.create(
        name=CREDENTIAL_NAME,
        comment="AWS IAM role for external location access",
        aws_iam_role=AwsIamRoleRequest(
            role_arn=AWS_IAM_ROLE_ARN
        ),
        read_only=False  # Set to True if you only want read access
    )
    
    print("✅ Storage Credential created successfully!\n")
    print(f"Credential Name: {credential.name}")
    print(f"Credential ID: {credential.id}")
    print(f"AWS Role ARN: {AWS_IAM_ROLE_ARN}")
    print(f"Read Only: {credential.read_only}")
    print(f"Owner: {credential.owner}")
    
    # Store for later use
    spark.conf.set("my.credential.name", CREDENTIAL_NAME)
    
except Exception as e:
    print(f"❌ Error creating storage credential: {str(e)}")
    print("\nℹ️ Common issues:")
    print("  • Insufficient permissions (need Account Admin or Metastore Admin)")
    print("  • Invalid IAM Role ARN")
    print("  • IAM Role doesn't trust Databricks account")
    print("  • Credential name already exists")
    print("  \n📖 Setup Guide: https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html")
    print("\n⚠️ If you don't have admin permissions:")
    print("  • Ask your workspace administrator to create the credential")
    print("  • Or use managed volumes instead (no cloud setup needed)")

# COMMAND ----------

# DBTITLE 1,Create External Location (Requires Admin)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CreateExternalLocation

w = WorkspaceClient()

# IMPORTANT: Replace with your actual S3 bucket path
EXTERNAL_LOCATION_NAME = "my_external_location"
S3_BUCKET_PATH = "s3://databricks-external-data-YOUR-ORG/raw-data/"  # Must end with /

# Get credential name from previous cell
try:
    credential_name = spark.conf.get("my.credential.name")
except:
    credential_name = "my_s3_credential"  # Fallback

print(f"🔗 Creating External Location: {EXTERNAL_LOCATION_NAME}\n")
print("⚠️ This requires Account Admin or Metastore Admin permissions\n")

try:
    # Create external location
    location = w.external_locations.create(
        name=EXTERNAL_LOCATION_NAME,
        url=S3_BUCKET_PATH,
        credential_name=credential_name,
        comment="External location for raw data in S3",
        read_only=False,  # Set to True for read-only access
        skip_validation=False  # Set to True to skip connectivity validation
    )
    
    print("✅ External Location created successfully!\n")
    print(f"Location Name: {location.name}")
    print(f"URL: {location.url}")
    print(f"Credential: {location.credential_name}")
    print(f"Owner: {location.owner}")
    print(f"Read Only: {location.read_only}")
    
    # Store for later use
    spark.conf.set("my.external.location", EXTERNAL_LOCATION_NAME)
    
    print(f"\n✅ You can now create external volumes using this location!")
    
except Exception as e:
    print(f"❌ Error creating external location: {str(e)}")
    print("\nℹ️ Common issues:")
    print("  • Insufficient permissions (need Account Admin or Metastore Admin)")
    print("  • Storage credential does not exist")
    print("  • Invalid S3 URL format (must be s3://bucket/path/)")
    print("  • IAM role lacks permissions to access S3 bucket")
    print("  • S3 bucket does not exist")
    print("  • Connectivity validation failed (check IAM role and bucket policies)")
    print("\n📖 Troubleshooting: https://docs.databricks.com/en/connect/unity-catalog/external-locations.html")

# COMMAND ----------

# DBTITLE 1,Create External Volume
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

w = WorkspaceClient()

# Get catalog and schema names
try:
    catalog_name = spark.conf.get("my.catalog.name")
    schema_name = "bronze"
    external_location = spark.conf.get("my.external.location")
except:
    print("⚠️ Using default names")
    catalog_name = "my_learning_catalog"
    schema_name = "bronze"
    external_location = "my_external_location"

volume_name = "external_files"

print(f"📦 Creating External Volume: {catalog_name}.{schema_name}.{volume_name}\n")

try:
    # Create external volume
    volume = w.volumes.create(
        catalog_name=catalog_name,
        schema_name=schema_name,
        name=volume_name,
        volume_type=VolumeType.EXTERNAL,
        storage_location=f"s3://databricks-external-data-YOUR-ORG/raw-data/volumes/{volume_name}/",
        comment="External volume pointing to S3 bucket - data managed outside Databricks"
    )
    
    print("✅ External Volume created successfully!\n")
    print(f"Volume Name: {volume.name}")
    print(f"Full Name: {volume.full_name}")
    print(f"Volume Type: {volume.volume_type.value}")
    print(f"Storage Location: {volume.storage_location}")
    print(f"Owner: {volume.owner}")
    
    # Show how to access this volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    print(f"\n📂 Access Path: {volume_path}")
    print(f"\n💡 Key Difference from Managed Volume:")
    print(f"  • Files stored in YOUR S3 bucket")
    print(f"  • Deleting volume does NOT delete S3 files")
    print(f"  • Can access files from outside Databricks")
    print(f"  • You control storage lifecycle")
    
    # Store for later use
    spark.conf.set("my.external.volume.path", volume_path)
    
except Exception as e:
    print(f"❌ Error creating external volume: {str(e)}")
    print("\nℹ️ Common issues:")
    print("  • Insufficient permissions (need CREATE VOLUME privilege)")
    print("  • External location does not exist")
    print("  • Storage location path must be under external location URL")
    print("  • Invalid storage location format")
    print("  \n⚠️ If you don't have external location set up:")
    print("  • Use managed volumes instead (simpler, no cloud setup)")
    print("  • Or ask your admin to create external location")

# COMMAND ----------

# DBTITLE 1,Compare Managed vs External Volumes
# MAGIC %md
# MAGIC ### Managed vs External Volumes - Decision Guide
# MAGIC
# MAGIC | **Aspect** | **Managed Volume** | **External Volume** |
# MAGIC |------------|-------------------|---------------------|
# MAGIC | **Storage** | Databricks manages | You manage (S3/ADLS/GCS) |
# MAGIC | **Setup** | ✅ Simple (no cloud setup) | ⚠️ Complex (credentials, IAM) |
# MAGIC | **Permissions** | Unity Catalog only | Needs admin permissions |
# MAGIC | **Lifecycle** | Delete volume = delete data | Delete volume ≠ delete data |
# MAGIC | **Access** | Databricks only | Databricks + other tools |
# MAGIC | **Cost** | Databricks storage pricing | Your cloud storage pricing |
# MAGIC | **Use Case** | Databricks-only workflows | Multi-tool data sharing |
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC **Use Managed Volumes** when:
# MAGIC * ✅ Getting started with Unity Catalog
# MAGIC * ✅ Data only accessed from Databricks
# MAGIC * ✅ Don't want to manage cloud storage
# MAGIC * ✅ Need simple setup
# MAGIC * ✅ Temporary/working data
# MAGIC
# MAGIC **Use External Volumes** when:
# MAGIC * ✅ Need to access data from multiple tools
# MAGIC * ✅ Want full control over storage lifecycle
# MAGIC * ✅ Have existing cloud storage with data
# MAGIC * ✅ Need to persist data after volume deletion
# MAGIC * ✅ Cost optimization with your cloud pricing
# MAGIC * ✅ Compliance requires data in specific location

# COMMAND ----------

# DBTITLE 1,Part 5: Permissions and Access Control
# MAGIC %md
# MAGIC ## Part 5: Permissions and Access Control 🔐
# MAGIC
# MAGIC ### Unity Catalog Permissions Model
# MAGIC
# MAGIC Unity Catalog uses a **hierarchical permissions model**:
# MAGIC
# MAGIC ```
# MAGIC Metastore
# MAGIC   ├── USE METASTORE
# MAGIC   │
# MAGIC   Catalog
# MAGIC     ├── USE CATALOG (required to see it)
# MAGIC     ├── CREATE SCHEMA
# MAGIC     │
# MAGIC     Schema
# MAGIC       ├── USE SCHEMA (required to see it)
# MAGIC       ├── CREATE TABLE
# MAGIC       ├── CREATE VOLUME
# MAGIC       │
# MAGIC       Table/Volume
# MAGIC         ├── SELECT (read data)
# MAGIC         ├── MODIFY (write data)
# MAGIC         ├── ALL PRIVILEGES (full control)
# MAGIC ```
# MAGIC
# MAGIC ### Key Privileges
# MAGIC
# MAGIC | **Object** | **Privilege** | **Allows** |
# MAGIC |------------|--------------|------------|
# MAGIC | **Catalog** | `USE CATALOG` | View the catalog |
# MAGIC |  | `CREATE SCHEMA` | Create schemas |
# MAGIC |  | `ALL PRIVILEGES` | Full control |
# MAGIC | **Schema** | `USE SCHEMA` | View the schema |
# MAGIC |  | `CREATE TABLE` | Create tables |
# MAGIC |  | `CREATE VOLUME` | Create volumes |
# MAGIC |  | `ALL PRIVILEGES` | Full control |
# MAGIC | **Table** | `SELECT` | Read data |
# MAGIC |  | `MODIFY` | Insert/Update/Delete |
# MAGIC |  | `ALL PRIVILEGES` | Full control |
# MAGIC | **Volume** | `READ VOLUME` | Read files |
# MAGIC |  | `WRITE VOLUME` | Write files |
# MAGIC |  | `ALL PRIVILEGES` | Full control |
# MAGIC
# MAGIC ### Grant Syntax
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant catalog access
# MAGIC GRANT USE CATALOG ON CATALOG my_catalog TO `user@example.com`;
# MAGIC
# MAGIC -- Grant schema access
# MAGIC GRANT USE SCHEMA ON SCHEMA my_catalog.my_schema TO `user@example.com`;
# MAGIC
# MAGIC -- Grant table read access
# MAGIC GRANT SELECT ON TABLE my_catalog.my_schema.my_table TO `user@example.com`;
# MAGIC
# MAGIC -- Grant volume access
# MAGIC GRANT READ VOLUME ON VOLUME my_catalog.my_schema.my_volume TO `user@example.com`;
# MAGIC GRANT WRITE VOLUME ON VOLUME my_catalog.my_schema.my_volume TO `user@example.com`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's set up permissions!

# COMMAND ----------

# DBTITLE 1,Grant Permissions on Catalog and Schema
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get catalog name
try:
    catalog_name = spark.conf.get("my.catalog.name")
except:
    catalog_name = "my_learning_catalog"

schema_name = "bronze"

print(f"🔐 Setting up permissions on {catalog_name}.{schema_name}\n")

# Example: Grant permissions to a user
# REPLACE with actual email or use 'account users' for all users
GRANT_TO = "account users"  # This grants to all workspace users
# Or use specific email: GRANT_TO = "user@example.com"

print(f"Granting permissions to: {GRANT_TO}\n")

try:
    # Note: Permission granting via SDK requires admin privileges
    # For most users, it's easier to use SQL commands
    
    # Grant catalog usage
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `{GRANT_TO}`")
    print(f"✅ Granted USE CATALOG on {catalog_name}")
    
    # Grant schema usage
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema_name} TO `{GRANT_TO}`")
    print(f"✅ Granted USE SCHEMA on {catalog_name}.{schema_name}")
    
    # Grant ability to select from tables (when we create them)
    spark.sql(f"GRANT SELECT ON SCHEMA {catalog_name}.{schema_name} TO `{GRANT_TO}`")
    print(f"✅ Granted SELECT on future tables in {catalog_name}.{schema_name}")
    
    print("\n" + "="*60)
    print("✅ Permissions configured successfully!")
    print("="*60)
    print(f"\nUsers can now:")
    print(f"  ✅ View catalog {catalog_name}")
    print(f"  ✅ View schema {schema_name}")
    print(f"  ✅ Read tables in {catalog_name}.{schema_name}")
    
except Exception as e:
    print(f"❌ Error granting permissions: {str(e)}")
    print("\nℹ️ This usually means:")
    print("  • You don't own these objects")
    print("  • You lack admin privileges")
    print("  \nℹ️ Workaround: Run these SQL commands manually:")
    print(f"  GRANT USE CATALOG ON CATALOG {catalog_name} TO `{GRANT_TO}`;")
    print(f"  GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema_name} TO `{GRANT_TO}`;")
    print(f"  GRANT SELECT ON SCHEMA {catalog_name}.{schema_name} TO `{GRANT_TO}`;")

# COMMAND ----------

# DBTITLE 1,Show Current Permissions
# Get catalog name
try:
    catalog_name = spark.conf.get("my.catalog.name")
except:
    catalog_name = "my_learning_catalog"

print(f"📋 Current Permissions on {catalog_name}\n")
print("="*80)

try:
    # Show catalog grants
    print(f"\n📦 Catalog: {catalog_name}")
    grants_df = spark.sql(f"SHOW GRANTS ON CATALOG {catalog_name}")
    display(grants_df)
    
    # Show schema grants
    schema_name = "bronze"
    print(f"\n📂 Schema: {catalog_name}.{schema_name}")
    grants_df = spark.sql(f"SHOW GRANTS ON SCHEMA {catalog_name}.{schema_name}")
    display(grants_df)
    
    print("\n" + "="*80)
    print("✅ Permissions listed successfully")
    print("="*80)
    
except Exception as e:
    print(f"⚠️ Could not show grants: {str(e)}")
    print("This might be because the catalog/schema doesn't exist or you lack permissions")

# COMMAND ----------

# DBTITLE 1,Part 6: Summary and Best Practices
# MAGIC %md
# MAGIC ## Part 6: Summary and Best Practices 🎓
# MAGIC
# MAGIC ### What You've Learned
# MAGIC
# MAGIC Congratulations! You've successfully:
# MAGIC
# MAGIC ✅ **Created All-Purpose Clusters** - Using the Databricks UI  
# MAGIC ✅ **Set up Unity Catalog** - Catalogs and schemas with medallion architecture using SQL  
# MAGIC ✅ **Created Volumes** - Managed volumes using SQL commands  
# MAGIC ✅ **Configured External Locations** - Connected cloud storage to Unity Catalog  
# MAGIC ✅ **Set up Permissions** - Granted access to catalog objects
# MAGIC
# MAGIC 📝 **Learning Path**: You learned the **manual/UI approach** first to understand the fundamentals.  
# MAGIC 🚀 **Next Level**: In advanced modules, you'll learn **programmatic approaches** using the Databricks SDK and REST API for automation!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC #### 🏗️ **Architecture Patterns**
# MAGIC
# MAGIC **1. Medallion Architecture** (Bronze → Silver → Gold)
# MAGIC ```
# MAGIC my_catalog/
# MAGIC   ├── bronze/    # Raw, unprocessed data
# MAGIC   ├── silver/    # Cleaned, validated data
# MAGIC   └── gold/      # Business-level aggregates
# MAGIC ```
# MAGIC
# MAGIC **2. Separation by Environment**
# MAGIC ```
# MAGIC dev_catalog/     # Development
# MAGIC staging_catalog/ # Staging/QA
# MAGIC prod_catalog/    # Production
# MAGIC ```
# MAGIC
# MAGIC **3. Data Product Organization**
# MAGIC ```
# MAGIC company_catalog/
# MAGIC   ├── sales/         # Sales data product
# MAGIC   ├── marketing/     # Marketing data product
# MAGIC   └── finance/       # Finance data product
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 💻 **Cluster Best Practices**
# MAGIC
# MAGIC **DO:**
# MAGIC * ✅ Enable **auto-termination** (default: 120 minutes)
# MAGIC * ✅ Use **autoscaling** for variable workloads
# MAGIC * ✅ Choose **LTS (Long-Term Support)** runtimes for production
# MAGIC * ✅ Set **cluster policies** for cost control
# MAGIC * ✅ Use **Serverless** for most interactive development
# MAGIC * ✅ Tag clusters with **owner/project/cost-center**
# MAGIC
# MAGIC **DON'T:**
# MAGIC * ❌ Leave clusters running overnight (use auto-termination)
# MAGIC * ❌ Use oversized instance types for development
# MAGIC * ❌ Share production clusters for development
# MAGIC * ❌ Ignore cluster logs when debugging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🗂️ **Unity Catalog Best Practices**
# MAGIC
# MAGIC **Naming Conventions:**
# MAGIC ```
# MAGIC ✅ GOOD:
# MAGIC    my_company_prod.sales.customer_orders
# MAGIC    my_company_dev.marketing.campaign_metrics
# MAGIC    
# MAGIC ❌ BAD:
# MAGIC    catalog1.schema1.table1
# MAGIC    MyCompany-Prod.Sales.CustomerOrders  # No hyphens or CamelCase
# MAGIC ```
# MAGIC
# MAGIC **Permissions:**
# MAGIC * ✅ Grant **minimum required privileges**
# MAGIC * ✅ Use **groups**, not individual users
# MAGIC * ✅ Review permissions regularly
# MAGIC * ✅ Use **row/column-level security** for sensitive data
# MAGIC * ✅ Enable **audit logging** for compliance
# MAGIC
# MAGIC **Schema Organization:**
# MAGIC * ✅ Group related tables in same schema
# MAGIC * ✅ Separate **raw** from **curated** data
# MAGIC * ✅ Use **clear, descriptive** schema names
# MAGIC * ✅ Document schemas with **comments**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 📦 **Volume Best Practices**
# MAGIC
# MAGIC **When to Use Managed vs External:**
# MAGIC
# MAGIC | **Scenario** | **Recommendation** |
# MAGIC |--------------|--------------------|
# MAGIC | **Development/testing** | Managed (simpler) |
# MAGIC | **Production, Databricks-only** | Managed (simpler) |
# MAGIC | **Multi-tool access needed** | External (flexibility) |
# MAGIC | **Existing cloud storage** | External (reuse) |
# MAGIC | **Regulatory requirements** | External (control) |
# MAGIC | **Cost optimization** | External (cloud native pricing) |
# MAGIC
# MAGIC **Volume Organization:**
# MAGIC ```
# MAGIC my_catalog.bronze.raw_files/
# MAGIC   ├── 2024/
# MAGIC   │   ├── 01/
# MAGIC   │   └── 02/
# MAGIC   └── archives/
# MAGIC
# MAGIC my_catalog.silver.processed_files/
# MAGIC   ├── customer_data/
# MAGIC   └── product_data/
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🔐 **Security Best Practices**
# MAGIC
# MAGIC **1. Credential Management**
# MAGIC * ✅ Use **storage credentials** for external access
# MAGIC * ✅ Rotate credentials regularly
# MAGIC * ✅ Use **managed identities** (Azure) or **IAM roles** (AWS)
# MAGIC * ❌ Never hardcode credentials in code
# MAGIC
# MAGIC **2. Access Control**
# MAGIC * ✅ Follow **principle of least privilege**
# MAGIC * ✅ Use **groups** for permission management
# MAGIC * ✅ Enable **audit logs** for compliance
# MAGIC * ✅ Review access regularly (quarterly)
# MAGIC
# MAGIC **3. Data Classification**
# MAGIC * 🔴 **Sensitive** (PII, PHI) → Row/column-level security
# MAGIC * 🟡 **Internal** → Schema-level access control
# MAGIC * 🟢 **Public** → Broad access, still governed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Common Pitfalls to Avoid
# MAGIC
# MAGIC **1. Cluster Cost Overruns**
# MAGIC * ❌ Forgetting to enable auto-termination
# MAGIC * ❌ Using large clusters for small workloads
# MAGIC * ✅ **Solution**: Enable auto-termination, right-size clusters
# MAGIC
# MAGIC **2. Permission Issues**
# MAGIC * ❌ "I can't see my catalog!"
# MAGIC * ✅ **Solution**: Grant USE CATALOG + USE SCHEMA permissions
# MAGIC
# MAGIC **3. External Location Failures**
# MAGIC * ❌ IAM role doesn't trust Databricks
# MAGIC * ❌ S3 bucket policy blocks access
# MAGIC * ✅ **Solution**: Follow AWS/Azure/GCP setup guides carefully
# MAGIC
# MAGIC **4. Volume Path Confusion**
# MAGIC * ❌ Using `/dbfs/...` paths (old approach)
# MAGIC * ✅ **Use**: `/Volumes/catalog/schema/volume/` (Unity Catalog)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 🎯 **Continue Learning:**
# MAGIC * ✅ Module 3: Data Engineering with Delta Lake
# MAGIC * ✅ Module 4: Apache Spark Programming
# MAGIC * ✅ Module 5: Data Pipeline Development
# MAGIC
# MAGIC 📚 **Additional Resources:**
# MAGIC * [Unity Catalog Documentation](https://docs.databricks.com/unity-catalog/index.html)
# MAGIC * [Cluster Configuration Guide](https://docs.databricks.com/clusters/configure.html)
# MAGIC * [Volume Management](https://docs.databricks.com/volumes/index.html)
# MAGIC * [External Locations Setup](https://docs.databricks.com/external-locations/index.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Quick Reference Commands
# MAGIC
# MAGIC #### SQL Commands (What You Learned Today)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create Catalog
# MAGIC CREATE CATALOG IF NOT EXISTS my_catalog
# MAGIC   COMMENT 'My learning catalog';
# MAGIC
# MAGIC -- Use Catalog
# MAGIC USE CATALOG my_catalog;
# MAGIC
# MAGIC -- Create Schemas (Medallion Architecture)
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw data layer';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned data layer';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Analytics-ready data layer';
# MAGIC
# MAGIC -- Create Managed Volume
# MAGIC USE SCHEMA bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS raw_files
# MAGIC   COMMENT 'Volume for raw file storage';
# MAGIC
# MAGIC -- Create External Volume (requires external location setup)
# MAGIC CREATE EXTERNAL VOLUME external_files
# MAGIC   LOCATION 's3://my-bucket/path/'
# MAGIC   COMMENT 'External volume for cloud storage';
# MAGIC ```
# MAGIC
# MAGIC #### Python SDK Commands (Advanced - Future Learning)
# MAGIC
# MAGIC 🔒 **Note**: These programmatic approaches will be covered in advanced automation modules!
# MAGIC
# MAGIC ```python
# MAGIC # SDK - Create Cluster (Future Learning)
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC cluster = w.clusters.create_and_wait(
# MAGIC     cluster_name="my-cluster",
# MAGIC     spark_version="15.4.x-scala2.12",
# MAGIC     node_type_id="i3.xlarge",
# MAGIC     autoscale={"min_workers": 1, "max_workers": 3}
# MAGIC )
# MAGIC
# MAGIC # SDK - Create Catalog (Future Learning)
# MAGIC catalog = w.catalogs.create(
# MAGIC     name="my_catalog",
# MAGIC     comment="My learning catalog"
# MAGIC )
# MAGIC
# MAGIC # SDK - Create Schema (Future Learning)
# MAGIC schema = w.schemas.create(
# MAGIC     name="my_schema",
# MAGIC     catalog_name="my_catalog"
# MAGIC )
# MAGIC
# MAGIC # SDK - Create Managed Volume (Future Learning)
# MAGIC volume = w.volumes.create(
# MAGIC     catalog_name="my_catalog",
# MAGIC     schema_name="my_schema",
# MAGIC     name="my_volume",
# MAGIC     volume_type="MANAGED"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### Permission Management (SQL)
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant Permissions
# MAGIC GRANT USE CATALOG ON CATALOG my_catalog TO `user@example.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA my_catalog.my_schema TO `user@example.com`;
# MAGIC GRANT SELECT ON TABLE my_catalog.my_schema.my_table TO `user@example.com`;
# MAGIC GRANT READ VOLUME ON VOLUME my_catalog.my_schema.my_volume TO `user@example.com`;
# MAGIC
# MAGIC -- SQL - Show Permissions
# MAGIC SHOW GRANTS ON CATALOG my_catalog;
# MAGIC SHOW GRANTS ON SCHEMA my_catalog.my_schema;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎉 Congratulations on completing the hands-on lab!** 🎉

# COMMAND ----------

# DBTITLE 1,Learning Path: From Manual to Automation
# MAGIC %md
# MAGIC ## 🎓 Your Learning Journey: From Manual to Automation
# MAGIC
# MAGIC ### What You Accomplished Today ✅
# MAGIC
# MAGIC In this module, you learned the **foundational approach** - creating resources manually:
# MAGIC
# MAGIC #### Manual/UI Methods (Today's Focus)
# MAGIC * 💻 **Clusters**: Created via Databricks UI (Compute page)
# MAGIC * 🗂️ **Catalogs & Schemas**: Created using SQL commands
# MAGIC * 📦 **Volumes**: Created using SQL `CREATE VOLUME` statements
# MAGIC * 🔐 **Permissions**: Managed using SQL `GRANT` statements
# MAGIC
# MAGIC **Why Start Here?**
# MAGIC * ✅ Understand what each resource does
# MAGIC * ✅ See the UI and configuration options
# MAGIC * ✅ Learn proper naming conventions
# MAGIC * ✅ Master the fundamentals before automation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What's Coming Next 🚀
# MAGIC
# MAGIC #### Programmatic/SDK Methods (Future Advanced Modules)
# MAGIC
# MAGIC Once you're comfortable with the basics, you'll learn **automation** approaches:
# MAGIC
# MAGIC ```python
# MAGIC # Databricks SDK (Python)
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Create everything programmatically
# MAGIC cluster = w.clusters.create(...)
# MAGIC catalog = w.catalogs.create(...)
# MAGIC volume = w.volumes.create(...)
# MAGIC ```
# MAGIC
# MAGIC **Benefits of Automation:**
# MAGIC * ⚙️ **Reproducibility** - Create identical environments instantly
# MAGIC * 🔄 **CI/CD Integration** - Deploy with GitHub Actions, Jenkins, etc.
# MAGIC * 📊 **Infrastructure as Code** - Version control your data platform
# MAGIC * 🚀 **Scaling** - Create 100 catalogs as easily as 1
# MAGIC * 🤖 **Automation** - Self-service data platform for teams
# MAGIC
# MAGIC **Technologies You'll Learn:**
# MAGIC * **Databricks SDK** (Python, Java, Go) - Programmatic API access
# MAGIC * **Databricks CLI** - Command-line interface for automation
# MAGIC * **REST API** - Direct HTTP API calls
# MAGIC * **Terraform** - Infrastructure as Code for Databricks
# MAGIC * **DABs (Declarative Automation Bundles)** - Modern CI/CD for Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Recommended Learning Path 🛣️
# MAGIC
# MAGIC ```
# MAGIC 👶 Level 1: Fundamentals (You Are Here!)
# MAGIC    ├── Manual UI operations
# MAGIC    ├── SQL commands for catalog objects
# MAGIC    └── Understanding core concepts
# MAGIC
# MAGIC 🧑 Level 2: Intermediate (Coming Soon)
# MAGIC    ├── Databricks CLI basics
# MAGIC    ├── Simple SDK scripts
# MAGIC    └── Automating repetitive tasks
# MAGIC
# MAGIC 🧔 Level 3: Advanced (Future Modules)
# MAGIC    ├── Full SDK automation
# MAGIC    ├── CI/CD pipelines
# MAGIC    ├── Infrastructure as Code
# MAGIC    └── Enterprise deployment patterns
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why This Progression Matters 🎯
# MAGIC
# MAGIC Many courses jump straight to automation, but that creates problems:
# MAGIC
# MAGIC ❌ **Anti-Pattern**: "Here's code to create everything automatically"
# MAGIC * You don't understand what's being created
# MAGIC * Harder to troubleshoot when things break
# MAGIC * Can't make informed decisions about configuration
# MAGIC
# MAGIC ✅ **Best Practice**: Learn manually first, automate later
# MAGIC * You understand each component deeply
# MAGIC * Can troubleshoot issues effectively
# MAGIC * Make intelligent automation decisions
# MAGIC * Appreciate why automation matters
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Ready to Continue? 🚀
# MAGIC
# MAGIC **Next Module**: Module 3 - Data Engineering with Delta Lake
# MAGIC * Work with the catalog structure you created today
# MAGIC * Create tables in your bronze/silver/gold schemas
# MAGIC * Use volumes for file storage
# MAGIC * Build real data pipelines!
# MAGIC
# MAGIC **Future Advanced Module**: Databricks Automation & SDK
# MAGIC * Everything you did manually today, done programmatically
# MAGIC * CI/CD for data platforms
# MAGIC * Infrastructure as Code
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎉 Excellent work completing Module 2! You now have a solid foundation in Databricks platform fundamentals!** 🎉