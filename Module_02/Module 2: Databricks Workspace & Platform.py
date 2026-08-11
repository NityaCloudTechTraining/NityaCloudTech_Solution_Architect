# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Module Overview
# MAGIC %md
# MAGIC # Module 2: Databricks Workspace & Platform
# MAGIC
# MAGIC ## 📚 Course Overview
# MAGIC
# MAGIC Welcome to **Databricks Workspace & Platform**! This module teaches you how to navigate and use the Databricks unified data and AI platform. You'll learn the platform architecture, compute options, workspace assets, and hands-on skills to start building data and AI solutions.
# MAGIC
# MAGIC ### 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this module, you will master:
# MAGIC
# MAGIC * 🏢 **Databricks Workspace** - Platform overview and core architecture
# MAGIC * ☁️ **Cloud Architecture** - Azure, AWS, and GCP deployment models
# MAGIC * 🎨 **Databricks Editions** - Community, Standard, Premium, and Enterprise comparison
# MAGIC * 💻 **Compute Options** - Serverless, All-Purpose Clusters, and SQL Warehouses
# MAGIC * 📓 **Notebooks** - Interactive development environment for Python, SQL, R, Scala
# MAGIC * 📁 **Files & Volumes** - Workspace storage and Unity Catalog volumes
# MAGIC * 🔄 **Repos** - Git integration for version control and collaboration
# MAGIC * 📊 **Dashboards** - AI/BI dashboards for data visualization and reporting
# MAGIC * 🚀 **Apps** - Databricks Apps for building custom data applications
# MAGIC * ⚙️ **Jobs** - Workflow orchestration, scheduling, and monitoring
# MAGIC * 🗂️ **Workspace Organization** - Folders, permissions, and best practices
# MAGIC * 🧭 **UI Navigation** - Mastering the Databricks interface
# MAGIC
# MAGIC ### 🛠️ Hands-on Activities
# MAGIC
# MAGIC This module includes practical exercises:
# MAGIC * ✅ **Create your first workspace** and navigate the UI
# MAGIC * ✅ **Launch compute resources** (Serverless, Clusters, SQL Warehouses)
# MAGIC * ✅ **Create and run notebooks** with Python and SQL code
# MAGIC * ✅ **Explore workspace assets** (Files, Volumes, Repos)
# MAGIC * ✅ **Build a simple dashboard** for data visualization
# MAGIC * ✅ **Create a scheduled job** to automate workflows
# MAGIC * ✅ **Organize your workspace** with folders and best practices
# MAGIC
# MAGIC ### 📋 Module Structure
# MAGIC
# MAGIC This module is organized into four main parts:
# MAGIC
# MAGIC **Part 1: Platform Overview (Cells 1-6)**
# MAGIC * Databricks Workspace architecture and components
# MAGIC * Cloud deployment models (Azure, AWS, GCP)
# MAGIC * Editions comparison and feature matrix
# MAGIC
# MAGIC **Part 2: Compute Resources (Cells 7-12)**
# MAGIC * Serverless Compute (CPU & GPU)
# MAGIC * All-Purpose Clusters and cluster configuration
# MAGIC * SQL Warehouses for BI and analytics
# MAGIC * Compute selection guide and best practices
# MAGIC
# MAGIC **Part 3: Workspace Assets (Cells 13-20)**
# MAGIC * Notebooks: Interactive development
# MAGIC * Files: Workspace file storage
# MAGIC * Volumes: Unity Catalog managed storage
# MAGIC * Repos: Git integration
# MAGIC * Dashboards: AI/BI visualization
# MAGIC * Apps: Custom data applications
# MAGIC * Jobs: Workflow automation
# MAGIC * Folders: Asset organization
# MAGIC
# MAGIC **Part 4: UI Navigation & Hands-On (Cells 21-30)**
# MAGIC * Databricks UI tour and navigation
# MAGIC * Hands-on exercises: Create → Configure → Run
# MAGIC * Best practices and next steps
# MAGIC
# MAGIC ---
# MAGIC **Let's get started!** 🚀

# COMMAND ----------

# DBTITLE 1,1. What is Databricks Workspace?
# MAGIC %md
# MAGIC ## 1. What is Databricks Workspace? 🏢
# MAGIC
# MAGIC ### Definition
# MAGIC
# MAGIC **Databricks Workspace** is a unified, collaborative environment for data engineering, data science, analytics, and machine learning on the lakehouse platform.
# MAGIC
# MAGIC ### Key Characteristics
# MAGIC
# MAGIC ✅ **Unified Platform** - One environment for all data and AI workloads  
# MAGIC ✅ **Cloud-Native** - Built for AWS, Azure, and GCP  
# MAGIC ✅ **Collaborative** - Real-time co-authoring, comments, version control  
# MAGIC ✅ **Managed** - No infrastructure management required  
# MAGIC ✅ **Secure** - Enterprise-grade security and governance
# MAGIC
# MAGIC ### What Makes Databricks Different?
# MAGIC
# MAGIC ```
# MAGIC Traditional Approach:              Databricks Approach:
# MAGIC
# MAGIC ❌ Multiple tools                   ✅ Single unified workspace
# MAGIC ❌ Siloed teams                    ✅ Collaborative environment
# MAGIC ❌ Manual infrastructure           ✅ Fully managed platform
# MAGIC ❌ Complex data copying            ✅ Lakehouse architecture
# MAGIC ❌ Separate DE/DS/ML tools         ✅ Integrated workflow
# MAGIC ```
# MAGIC
# MAGIC ### Core Value Propositions
# MAGIC
# MAGIC #### 1️⃣ **Unified Data & AI Platform**
# MAGIC
# MAGIC ```
# MAGIC                     DATABRICKS WORKSPACE
# MAGIC ┌──────────────────────────────────────────────────┐
# MAGIC │                                                  │
# MAGIC │  📓 Notebooks  📁 Files  📊 Dashboards  🚀 Apps  │
# MAGIC │                                                  │
# MAGIC │  ⚙️ Jobs  🔄 Repos  💾 Volumes  🔍 Catalog  │
# MAGIC │                                                  │
# MAGIC │  💻 Compute  🛡️ Security  📊 ML  🤖 GenAI   │
# MAGIC │                                                  │
# MAGIC └──────────────────────────────────────────────────┘
# MAGIC               │
# MAGIC               ↓
# MAGIC       LAKEHOUSE ARCHITECTURE
# MAGIC       (Delta Lake + Unity Catalog)
# MAGIC ```
# MAGIC
# MAGIC #### 2️⃣ **Built on Apache Spark**
# MAGIC
# MAGIC * Created by the **original Spark team** (Matei Zaharia, Ali Ghodsi, et al.)
# MAGIC * Optimized Spark runtime (2-5x faster than open-source)
# MAGIC * Photon query engine for blazing-fast SQL
# MAGIC * Automatic tuning and optimization
# MAGIC
# MAGIC #### 3️⃣ **Lakehouse Architecture**
# MAGIC
# MAGIC ```
# MAGIC Data Warehouse           Data Lake              Lakehouse (Databricks)
# MAGIC ┌────────────┐      ┌────────────┐      ┌────────────┐
# MAGIC │ Structured │      │  All Data  │      │ All + ACID │
# MAGIC │ SQL/BI     │      │  Types     │      │ + SQL/BI   │
# MAGIC │ Expensive  │      │  Cheap     │      │ + ML/AI    │
# MAGIC └────────────┘      └────────────┘      └────────────┘
# MAGIC ❌ No ML/AI          ❌ No ACID          ✅ Best of both!
# MAGIC ```
# MAGIC
# MAGIC ### Who Uses Databricks?
# MAGIC
# MAGIC | **Persona** | **Use Case** | **Primary Tools** |
# MAGIC |-------------|--------------|-------------------|
# MAGIC | **Data Engineers** | ETL/ELT pipelines, data quality | Notebooks, Jobs, Delta Lake |
# MAGIC | **Data Scientists** | ML model development, experimentation | Notebooks, MLflow, Feature Store |
# MAGIC | **Data Analysts** | SQL queries, dashboards, reporting | SQL Warehouses, Dashboards |
# MAGIC | **ML Engineers** | Model deployment, monitoring | Model Serving, MLflow |
# MAGIC | **Analytics Engineers** | dbt models, data transformation | Notebooks, Repos, Jobs |
# MAGIC | **Business Users** | Self-service analytics, dashboards | AI/BI Dashboards, Genie |

# COMMAND ----------

# DBTITLE 1,2. Workspace Architecture Diagram
# Databricks Workspace Architecture Visualization
import warnings
warnings.filterwarnings('ignore')  # Suppress all warnings

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle, Circle
import numpy as np

# Suppress matplotlib font warnings
import logging
logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 14))

# Top: High-level architecture
ax1.set_xlim(0, 10)
ax1.set_ylim(0, 10)
ax1.axis('off')

ax1.text(5, 9.5, 'Databricks Workspace Architecture', fontsize=18, fontweight='bold', ha='center')

# Control Plane
control = FancyBboxPatch((0.5, 6), 9, 2.8, boxstyle="round,pad=0.1",
                        facecolor='#E3F2FD', edgecolor='#1976D2', linewidth=3)
ax1.add_patch(control)
ax1.text(5, 8.5, 'CONTROL PLANE (Databricks Managed)', fontsize=12, fontweight='bold', ha='center')
ax1.text(5, 8, 'Web UI • Notebooks • Jobs • Cluster Manager • Security • Metastore', 
        fontsize=10, ha='center', style='italic')

components = [
    {'x': 1.5, 'label': '📓\nNotebooks', 'color': '#4CAF50'},
    {'x': 3, 'label': '⚙️\nJobs', 'color': '#FF9800'},
    {'x': 4.5, 'label': '📊\nDashboards', 'color': '#2196F3'},
    {'x': 6, 'label': '🔄\nRepos', 'color': '#9C27B0'},
    {'x': 7.5, 'label': '🔍\nCatalog', 'color': '#F44336'},
    {'x': 9, 'label': '🛡️\nSecurity', 'color': '#607D8B'}
]

for comp in components:
    box = FancyBboxPatch((comp['x']-0.4, 6.8), 0.8, 0.9, boxstyle="round,pad=0.05",
                        facecolor=comp['color'], edgecolor='black', linewidth=1.5, alpha=0.7)
    ax1.add_patch(box)
    ax1.text(comp['x'], 7.25, comp['label'], fontsize=8, ha='center', va='center', 
            fontweight='bold', color='white')

# Arrow
arrow = FancyArrowPatch((5, 5.8), (5, 5.2), arrowstyle='->', mutation_scale=30, 
                       linewidth=3, color='#1976D2')
ax1.add_patch(arrow)
ax1.text(5.8, 5.5, 'API Calls', fontsize=9, style='italic')

# Data Plane
data = FancyBboxPatch((0.5, 1), 9, 4, boxstyle="round,pad=0.1",
                     facecolor='#FFF3E0', edgecolor='#F57C00', linewidth=3)
ax1.add_patch(data)
ax1.text(5, 4.7, 'DATA PLANE (Your Cloud Account)', fontsize=12, fontweight='bold', ha='center')
ax1.text(5, 4.2, 'Compute Clusters • Data Storage • Processing', fontsize=10, ha='center', style='italic')

# Compute resources
compute_items = [
    {'x': 2, 'y': 2.5, 'label': 'Serverless\nCompute', 'color': '#66BB6A'},
    {'x': 4.5, 'y': 2.5, 'label': 'All-Purpose\nClusters', 'color': '#42A5F5'},
    {'x': 7, 'y': 2.5, 'label': 'SQL\nWarehouses', 'color': '#AB47BC'}
]

for item in compute_items:
    box = FancyBboxPatch((item['x']-0.6, item['y']), 1.2, 1, boxstyle="round,pad=0.05",
                        facecolor=item['color'], edgecolor='black', linewidth=2, alpha=0.8)
    ax1.add_patch(box)
    ax1.text(item['x'], item['y']+0.5, item['label'], fontsize=9, ha='center', 
            va='center', fontweight='bold', color='white')

# Storage
storage = FancyBboxPatch((1, 1.2), 8, 0.8, boxstyle="round,pad=0.05",
                        facecolor='#78909C', edgecolor='black', linewidth=2, alpha=0.7)
ax1.add_patch(storage)
ax1.text(5, 1.6, '💾 DATA STORAGE: Delta Lake Tables • Unity Catalog Volumes • Cloud Storage (S3/ADLS/GCS)',
        fontsize=9, ha='center', va='center', fontweight='bold', color='white')

# Bottom: Deployment model
ax2.set_xlim(0, 10)
ax2.set_ylim(0, 10)
ax2.axis('off')

ax2.text(5, 9.5, 'Multi-Cloud Deployment', fontsize=16, fontweight='bold', ha='center')

clouds = [
    {'x': 1.5, 'name': 'AWS', 'color': '#FF9900', 'services': ['EC2', 'S3', 'IAM', 'VPC']},
    {'x': 5, 'name': 'Azure', 'color': '#0078D4', 'services': ['VMs', 'ADLS', 'AAD', 'VNET']},
    {'x': 8.5, 'name': 'GCP', 'color': '#4285F4', 'services': ['Compute', 'GCS', 'IAM', 'VPC']}
]

for cloud in clouds:
    # Cloud box
    box = FancyBboxPatch((cloud['x']-1, 5), 2, 3.5, boxstyle="round,pad=0.1",
                        facecolor=cloud['color'], edgecolor='black', linewidth=3, alpha=0.2)
    ax2.add_patch(box)
    ax2.text(cloud['x'], 8.2, cloud['name'], fontsize=14, ha='center', fontweight='bold')
    
    # Databricks on top
    db_box = FancyBboxPatch((cloud['x']-0.7, 6.5), 1.4, 1.2, boxstyle="round,pad=0.05",
                           facecolor='#FF3621', edgecolor='black', linewidth=2)
    ax2.add_patch(db_box)
    ax2.text(cloud['x'], 7.1, 'Databricks', fontsize=10, ha='center', 
            fontweight='bold', color='white')
    
    # Services
    y_pos = 5.8
    for service in cloud['services']:
        ax2.text(cloud['x'], y_pos, service, fontsize=8, ha='center', style='italic')
        y_pos -= 0.4

ax2.text(5, 4, 'Same Databricks Experience Across All Clouds', 
        fontsize=12, ha='center', fontweight='bold',
        bbox=dict(boxstyle='round', facecolor='lightyellow', edgecolor='orange', linewidth=2))

ax2.text(5, 3, '✅ Unified interface  ✅ Portable workloads  ✅ Multi-cloud data mesh', 
        fontsize=10, ha='center', style='italic')

# Benefits
benefits = [
    '🚀 No infrastructure management',
    '🔒 Enterprise security & compliance',
    '⚡ Auto-scaling & optimization',
    '👥 Collaboration & sharing',
    '📊 Unified governance (Unity Catalog)',
    '💰 Pay-as-you-go pricing'
]

y_pos = 1.8
for i, benefit in enumerate(benefits):
    x = 2 if i < 3 else 6
    y = y_pos if i < 3 else 1.8 - (i-3) * 0.5
    ax2.text(x, y_pos - (i%3)*0.5, benefit, fontsize=9, 
            bbox=dict(boxstyle='round', facecolor='#E8F5E9', alpha=0.8))

plt.tight_layout()
plt.show()

print("\n" + "="*70)
print("KEY ARCHITECTURE PRINCIPLES")
print("="*70)
print("1. CONTROL PLANE: Databricks manages UI, notebooks, job scheduling")
print("2. DATA PLANE: Compute & data stay in YOUR cloud account")
print("3. SECURITY: Your data never leaves your environment")
print("4. MULTI-CLOUD: Same experience on AWS, Azure, GCP")
print("="*70)

# COMMAND ----------

# DBTITLE 1,3. Cloud Architecture AWS Azure GCP
# MAGIC %md
# MAGIC ## 3. Cloud Architecture: AWS, Azure, GCP ☁️
# MAGIC
# MAGIC ### Databricks Multi-Cloud Strategy
# MAGIC
# MAGIC Databricks offers **identical experience** across all three major cloud providers.
# MAGIC
# MAGIC ### AWS (Amazon Web Services)
# MAGIC
# MAGIC #### Deployment Model
# MAGIC ```
# MAGIC            YOUR AWS ACCOUNT
# MAGIC ┌───────────────────────────────────────────┐
# MAGIC │                                           │
# MAGIC │  💻 Compute: EC2 instances              │
# MAGIC │  💾 Storage: S3 (data lake)            │
# MAGIC │  🔐 Security: IAM roles & policies     │
# MAGIC │  🌐 Network: VPC, security groups       │
# MAGIC │  🛡️ Encryption: KMS                    │
# MAGIC │                                           │
# MAGIC └───────────────────────────────────────────┘
# MAGIC           ↑ API Connection
# MAGIC    DATABRICKS CONTROL PLANE
# MAGIC ```
# MAGIC
# MAGIC #### Key Components
# MAGIC * **Compute**: EC2 instances (driver + workers)
# MAGIC * **Storage**: S3 buckets for Delta Lake tables
# MAGIC * **Networking**: VPC peering or AWS PrivateLink
# MAGIC * **Security**: IAM instance profiles, bucket policies
# MAGIC * **Regions**: Available in 20+ AWS regions
# MAGIC
# MAGIC #### Unique AWS Features
# MAGIC * **Graviton instances**: ARM-based, cost-effective compute
# MAGIC * **S3 optimization**: Direct S3 access, intelligent tiering
# MAGIC * **AWS Lake Formation**: Integration for governance
# MAGIC * **PrivateLink**: Secure connectivity without public internet
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Azure (Microsoft Azure)
# MAGIC
# MAGIC #### Deployment Model
# MAGIC ```
# MAGIC            YOUR AZURE SUBSCRIPTION
# MAGIC ┌───────────────────────────────────────────┐
# MAGIC │                                           │
# MAGIC │  💻 Compute: Virtual machines           │
# MAGIC │  💾 Storage: ADLS Gen2 (data lake)     │
# MAGIC │  🔐 Security: Azure AD, managed identity │
# MAGIC │  🌐 Network: VNET, NSGs                  │
# MAGIC │  🛡️ Encryption: Azure Key Vault        │
# MAGIC │                                           │
# MAGIC └───────────────────────────────────────────┘
# MAGIC           ↑ API Connection
# MAGIC    DATABRICKS CONTROL PLANE
# MAGIC ```
# MAGIC
# MAGIC #### Key Components
# MAGIC * **Compute**: Azure VMs (Ev3, Dv3 series)
# MAGIC * **Storage**: ADLS Gen2 with hierarchical namespace
# MAGIC * **Networking**: VNET injection, Private Link
# MAGIC * **Security**: Azure AD integration, managed identities
# MAGIC * **Regions**: Available in 25+ Azure regions
# MAGIC
# MAGIC #### Unique Azure Features
# MAGIC * **Azure AD integration**: Single sign-on, conditional access
# MAGIC * **ADLS Gen2**: Hierarchical filesystem for better performance
# MAGIC * **Power BI integration**: Native connectors
# MAGIC * **Azure Synapse**: Lakehouse + data warehouse integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### GCP (Google Cloud Platform)
# MAGIC
# MAGIC #### Deployment Model
# MAGIC ```
# MAGIC            YOUR GCP PROJECT
# MAGIC ┌───────────────────────────────────────────┐
# MAGIC │                                           │
# MAGIC │  💻 Compute: Compute Engine instances    │
# MAGIC │  💾 Storage: GCS (Google Cloud Storage)  │
# MAGIC │  🔐 Security: IAM, service accounts      │
# MAGIC │  🌐 Network: VPC, firewall rules         │
# MAGIC │  🛡️ Encryption: Cloud KMS               │
# MAGIC │                                           │
# MAGIC └───────────────────────────────────────────┘
# MAGIC           ↑ API Connection
# MAGIC    DATABRICKS CONTROL PLANE
# MAGIC ```
# MAGIC
# MAGIC #### Key Components
# MAGIC * **Compute**: Compute Engine VMs (N1, N2 series)
# MAGIC * **Storage**: Google Cloud Storage buckets
# MAGIC * **Networking**: VPC, Private Service Connect
# MAGIC * **Security**: IAM, service accounts
# MAGIC * **Regions**: Available in 15+ GCP regions
# MAGIC
# MAGIC #### Unique GCP Features
# MAGIC * **BigQuery integration**: Query BQ data directly
# MAGIC * **GCS optimization**: Multi-regional buckets
# MAGIC * **Vertex AI**: ML platform integration
# MAGIC * **Private Service Connect**: Secure connectivity
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Multi-Cloud Comparison
# MAGIC
# MAGIC | **Feature** | **AWS** | **Azure** | **GCP** |
# MAGIC |-------------|---------|-----------|----------|
# MAGIC | **Compute** | EC2 | Virtual Machines | Compute Engine |
# MAGIC | **Storage** | S3 | ADLS Gen2 | GCS |
# MAGIC | **Auth** | IAM Roles | Azure AD | IAM |
# MAGIC | **Network** | VPC | VNET | VPC |
# MAGIC | **Encryption** | KMS | Key Vault | Cloud KMS |
# MAGIC | **Maturity** | ⭐⭐⭐⭐⭐ Most mature | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐⭐ Growing |
# MAGIC | **Regions** | 20+ | 25+ | 15+ |
# MAGIC | **Pricing** | Competitive | Competitive | Competitive |
# MAGIC
# MAGIC ### Choosing Your Cloud
# MAGIC
# MAGIC 🟢 **AWS**: Best for
# MAGIC * Largest ecosystem and marketplace
# MAGIC * Most mature Databricks features
# MAGIC * Widest region availability
# MAGIC * Strong S3 ecosystem
# MAGIC
# MAGIC 🔵 **Azure**: Best for
# MAGIC * Microsoft ecosystem shops
# MAGIC * Azure AD/Entra ID integration
# MAGIC * Power BI users
# MAGIC * Enterprise agreements
# MAGIC
# MAGIC 🔴 **GCP**: Best for
# MAGIC * Google Cloud native
# MAGIC * BigQuery users
# MAGIC * AI/ML with Vertex AI
# MAGIC * Startups on GCP
# MAGIC
# MAGIC ### Data Portability
# MAGIC
# MAGIC ✅ **Open formats**: Delta Lake, Parquet  
# MAGIC ✅ **No vendor lock-in**: Move data between clouds  
# MAGIC ✅ **Same code**: Notebooks work across clouds  
# MAGIC ✅ **Unity Catalog**: Cross-cloud governance

# COMMAND ----------

# DBTITLE 1,4. Databricks Editions Comparison
# MAGIC %md
# MAGIC ## 4. Databricks Editions 🎨
# MAGIC
# MAGIC ### Edition Tiers
# MAGIC
# MAGIC Databricks offers four editions with progressive capabilities:
# MAGIC
# MAGIC ```
# MAGIC   Community      Standard       Premium        Enterprise
# MAGIC      🎓            💼            💪             🏢
# MAGIC     FREE!       Pay-as-go     Advanced      Full Suite
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎓 Community Edition
# MAGIC
# MAGIC **Target**: Learning, experimentation, personal projects
# MAGIC
# MAGIC #### What's Included
# MAGIC * ✅ Free forever
# MAGIC * ✅ Single user
# MAGIC * ✅ Notebooks (Python, SQL, R, Scala)
# MAGIC * ✅ Small cluster (15 GB RAM)
# MAGIC * ✅ Basic compute
# MAGIC * ✅ Sample datasets
# MAGIC
# MAGIC #### Limitations
# MAGIC * ❌ No Unity Catalog
# MAGIC * ❌ No job scheduling
# MAGIC * ❌ No collaboration features
# MAGIC * ❌ No SQL warehouses
# MAGIC * ❌ Limited to 2 hours per session
# MAGIC * ❌ No production use
# MAGIC
# MAGIC **Best For**: Students, tutorials, learning Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💼 Standard Edition
# MAGIC
# MAGIC **Target**: Small teams, basic production workloads
# MAGIC
# MAGIC #### What's Included
# MAGIC * ✅ All Community features
# MAGIC * ✅ **Collaboration**: Share notebooks, multi-user
# MAGIC * ✅ **Job scheduling**: Automated workflows
# MAGIC * ✅ **RBAC**: Role-based access control
# MAGIC * ✅ **SQL Analytics**: SQL warehouses
# MAGIC * ✅ **Delta Lake**: ACID transactions
# MAGIC * ✅ **MLflow**: Experiment tracking
# MAGIC * ✅ **Cluster policies**: Cost control
# MAGIC
# MAGIC #### Limitations
# MAGIC * ❌ No Unity Catalog (limited governance)
# MAGIC * ❌ No audit logs
# MAGIC * ❌ No SCIM provisioning
# MAGIC * ❌ No advanced security features
# MAGIC
# MAGIC **Pricing**: DBU-based (Databricks Units)  
# MAGIC **Best For**: Startups, small data teams, dev/test
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💪 Premium Edition
# MAGIC
# MAGIC **Target**: Enterprise teams with governance needs
# MAGIC
# MAGIC #### What's Included
# MAGIC * ✅ All Standard features
# MAGIC * ✅ **Unity Catalog**: Centralized governance
# MAGIC * ✅ **Advanced security**: SSO, SCIM, audit logs
# MAGIC * ✅ **Data lineage**: Track data dependencies
# MAGIC * ✅ **Advanced networking**: PrivateLink, VNet injection
# MAGIC * ✅ **Serverless compute**: Instant startup
# MAGIC * ✅ **AI/BI Dashboards**: Advanced visualizations
# MAGIC * ✅ **Feature Store**: ML feature management
# MAGIC * ✅ **Model Serving**: Deploy ML models
# MAGIC
# MAGIC #### Limitations
# MAGIC * ❌ No automatic cluster optimization (ACO)
# MAGIC * ❌ Limited support SLA
# MAGIC * ❌ No dedicated success manager
# MAGIC
# MAGIC **Pricing**: Higher DBU rate than Standard  
# MAGIC **Best For**: Most enterprises, production workloads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏢 Enterprise Edition
# MAGIC
# MAGIC **Target**: Large enterprises with critical workloads
# MAGIC
# MAGIC #### What's Included
# MAGIC * ✅ All Premium features
# MAGIC * ✅ **Compliance**: HIPAA, SOC 2, FedRAMP
# MAGIC * ✅ **Customer-managed keys**: Encryption control
# MAGIC * ✅ **Private connectivity**: Fully private network
# MAGIC * ✅ **Enhanced support**: 24/7, faster response
# MAGIC * ✅ **Success manager**: Dedicated CSM
# MAGIC * ✅ **Advanced monitoring**: Enhanced metrics
# MAGIC * ✅ **Custom SLAs**: Uptime guarantees
# MAGIC
# MAGIC **Pricing**: Custom, volume discounts  
# MAGIC **Best For**: Fortune 500, regulated industries, mission-critical
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Feature Comparison Matrix
# MAGIC
# MAGIC | **Feature** | **Community** | **Standard** | **Premium** | **Enterprise** |
# MAGIC |-------------|---------------|--------------|-------------|----------------|
# MAGIC | **Notebooks** | ✅ | ✅ | ✅ | ✅ |
# MAGIC | **Collaboration** | ❌ | ✅ | ✅ | ✅ |
# MAGIC | **Job Scheduling** | ❌ | ✅ | ✅ | ✅ |
# MAGIC | **SQL Warehouses** | ❌ | ✅ | ✅ | ✅ |
# MAGIC | **Unity Catalog** | ❌ | ❌ | ✅ | ✅ |
# MAGIC | **Serverless** | ❌ | ❌ | ✅ | ✅ |
# MAGIC | **Audit Logs** | ❌ | ❌ | ✅ | ✅ |
# MAGIC | **SSO/SCIM** | ❌ | ❌ | ✅ | ✅ |
# MAGIC | **PrivateLink** | ❌ | ❌ | ✅ | ✅ |
# MAGIC | **Customer Keys** | ❌ | ❌ | ❌ | ✅ |
# MAGIC | **24/7 Support** | ❌ | ❌ | ❌ | ✅ |
# MAGIC | **Compliance** | ❌ | ❌ | ⚠️ Partial | ✅ Full |
# MAGIC
# MAGIC ### Quick Selection Guide
# MAGIC
# MAGIC ```
# MAGIC Your Scenario                         → Recommended Edition
# MAGIC ────────────────────────────────────────────────────
# MAGIC Learning Databricks                   → Community
# MAGIC Startup, small team                   → Standard
# MAGIC Enterprise, need governance           → Premium
# MAGIC Regulated industry (healthcare, fin)  → Enterprise
# MAGIC Need Unity Catalog                    → Premium+
# MAGIC Need HIPAA/SOC 2 compliance           → Enterprise
# MAGIC ```
# MAGIC
# MAGIC ### Pricing Model
# MAGIC
# MAGIC All paid editions use **Databricks Units (DBUs)**:
# MAGIC
# MAGIC ```
# MAGIC Cost = (Cloud Compute Cost) + (DBU Cost)
# MAGIC
# MAGIC Example on AWS:
# MAGIC - r5.xlarge EC2: $0.252/hour
# MAGIC - Standard DBU: $0.40/hour
# MAGIC - Total: $0.652/hour
# MAGIC
# MAGIC Premium DBU costs more (~1.5x Standard)
# MAGIC Enterprise: Custom pricing
# MAGIC ```
# MAGIC
# MAGIC ### Upgrading Between Editions
# MAGIC
# MAGIC 🔼 **Community → Standard**: Instant  
# MAGIC 🔼 **Standard → Premium**: Contact sales  
# MAGIC 🔼 **Premium → Enterprise**: Custom contract
# MAGIC
# MAGIC **Note**: You can't downgrade without losing features!

# COMMAND ----------

# DBTITLE 1,5. Compute Options Overview
# MAGIC %md
# MAGIC ## 5. Compute Options 💻
# MAGIC
# MAGIC ### Three Main Compute Types
# MAGIC
# MAGIC Databricks offers three compute options for different workloads:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │                COMPUTE OPTIONS                     │
# MAGIC │                                                    │
# MAGIC │  ┌────────────────┐   ┌────────────────┐   ┌────────────────┐  │
# MAGIC │  │   Serverless   │   │  All-Purpose  │   │  SQL         │  │
# MAGIC │  │   Compute      │   │   Clusters    │   │  Warehouses  │  │
# MAGIC │  │                │   │               │   │             │  │
# MAGIC │  │  ⚡ Instant     │   │ ⚙️ Configurable│   │ 📊 SQL/BI    │  │
# MAGIC │  │  🚀 Auto-scale  │   │ 👥 Multi-user  │   │ 🚀 Optimized │  │
# MAGIC │  │  💰 Pay-per-use │   │ 📓 Notebooks   │   │ 📊 Dashboards│  │
# MAGIC │  └────────────────┘   └────────────────┘   └────────────────┘  │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣ Serverless Compute
# MAGIC
# MAGIC **What**: Fully managed compute with instant startup
# MAGIC
# MAGIC #### Key Features
# MAGIC * ⚡ **Instant startup** (< 1 minute)
# MAGIC * 🚀 **Auto-scaling** based on workload
# MAGIC * 💰 **Pay only for what you use** (per-second billing)
# MAGIC * 🔒 **Fully managed** (no cluster configuration)
# MAGIC * ⚙️ **Automatic optimization** (no tuning needed)
# MAGIC
# MAGIC #### Two Flavors
# MAGIC
# MAGIC **Serverless CPU**
# MAGIC * General-purpose workloads
# MAGIC * Notebooks, ETL, data processing
# MAGIC * Python, SQL, Scala
# MAGIC
# MAGIC **Serverless GPU**
# MAGIC * ML model training
# MAGIC * Deep learning
# MAGIC * LLM inference
# MAGIC
# MAGIC #### When to Use
# MAGIC * ✅ Interactive notebook development
# MAGIC * ✅ Ad-hoc queries and exploration
# MAGIC * ✅ Bursty/unpredictable workloads
# MAGIC * ✅ Quick prototyping
# MAGIC * ✅ Learning Databricks
# MAGIC
# MAGIC #### Limitations
# MAGIC * ❌ Not available in all regions (yet)
# MAGIC * ❌ Less customization than clusters
# MAGIC * ❌ Premium edition required
# MAGIC
# MAGIC **You're using Serverless right now!** 🎉
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ All-Purpose Clusters
# MAGIC
# MAGIC **What**: Configurable, multi-user compute clusters
# MAGIC
# MAGIC #### Key Features
# MAGIC * ⚙️ **Fully customizable** (instance types, libraries, config)
# MAGIC * 👥 **Multi-user** shared access
# MAGIC * 🔄 **Persistent** or auto-terminating
# MAGIC * 📓 **Notebook-friendly** (attach/detach)
# MAGIC * 📦 **Custom libraries** (PyPI, Maven, custom JARs)
# MAGIC
# MAGIC #### Configuration Options
# MAGIC * **Instance types**: CPU, GPU, memory-optimized
# MAGIC * **Auto-scaling**: Min/max workers
# MAGIC * **Auto-termination**: Shut down after idle time
# MAGIC * **Libraries**: Pre-install packages
# MAGIC * **Init scripts**: Custom setup on startup
# MAGIC * **Cluster policies**: Enforce standards
# MAGIC
# MAGIC #### When to Use
# MAGIC * ✅ Development and experimentation
# MAGIC * ✅ Long-running sessions
# MAGIC * ✅ Need specific instance types or libraries
# MAGIC * ✅ Team collaboration
# MAGIC * ✅ Require full Spark customization
# MAGIC
# MAGIC #### Cluster Lifecycle
# MAGIC ```
# MAGIC Create → Start → Running → Terminate → Restart
# MAGIC   ↓        ↓        ↓          ↓
# MAGIC Config   Spin-up  Active   Shut down
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ SQL Warehouses
# MAGIC
# MAGIC **What**: Optimized compute for SQL queries and BI
# MAGIC
# MAGIC #### Key Features
# MAGIC * 📊 **SQL-optimized** (Photon engine)
# MAGIC * 🚀 **Ultra-fast queries** (2-5x faster)
# MAGIC * 📈 **BI tool integration** (Tableau, Power BI, etc.)
# MAGIC * 👥 **Concurrent users** (many analysts querying simultaneously)
# MAGIC * 💰 **Pay per query** (Serverless SQL)
# MAGIC
# MAGIC #### Warehouse Types
# MAGIC
# MAGIC **Serverless SQL**
# MAGIC * Instant startup
# MAGIC * Auto-scaling
# MAGIC * No management
# MAGIC
# MAGIC **Pro SQL**
# MAGIC * More control
# MAGIC * Custom configuration
# MAGIC * Advanced features
# MAGIC
# MAGIC **Classic SQL**
# MAGIC * Legacy option
# MAGIC * Being phased out
# MAGIC
# MAGIC #### When to Use
# MAGIC * ✅ SQL queries and analytics
# MAGIC * ✅ BI dashboards (Tableau, Power BI)
# MAGIC * ✅ Data analysts (non-coders)
# MAGIC * ✅ Many concurrent users
# MAGIC * ✅ Ad-hoc reporting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Compute Comparison
# MAGIC
# MAGIC | **Feature** | **Serverless** | **All-Purpose** | **SQL Warehouse** |
# MAGIC |-------------|----------------|-----------------|-------------------|
# MAGIC | **Startup Time** | < 1 min | 5-10 min | < 1 min |
# MAGIC | **Languages** | Python, SQL, Scala | All (Py/SQL/R/Scala) | SQL only |
# MAGIC | **Use Case** | Notebooks, dev | Dev, prod, custom | SQL, BI |
# MAGIC | **Cost Model** | Per-second | Per-hour | Per-query |
# MAGIC | **Customization** | Low | High | Medium |
# MAGIC | **Multi-user** | ✅ | ✅ | ✅ |
# MAGIC | **Auto-scale** | ✅ Auto | ✅ Config | ✅ Auto |
# MAGIC | **Best For** | Dev, exploration | Production, custom | Analytics, BI |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Decision Tree
# MAGIC
# MAGIC ```
# MAGIC What do you want to do?
# MAGIC │
# MAGIC ├── Write Python/SQL notebooks?
# MAGIC │   └── ✅ Use SERVERLESS COMPUTE
# MAGIC │
# MAGIC ├── Run SQL queries for dashboards?
# MAGIC │   └── ✅ Use SQL WAREHOUSES
# MAGIC │
# MAGIC ├── Need custom libraries or GPU?
# MAGIC │   └── ✅ Use ALL-PURPOSE CLUSTERS
# MAGIC │
# MAGIC ├── Scheduled production jobs?
# MAGIC │   └── ✅ Use JOB CLUSTERS (ephemeral)
# MAGIC │
# MAGIC └── Learning/experimenting?
# MAGIC     └── ✅ Use SERVERLESS (easiest!)
# MAGIC ```
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC 🟢 **For Development**
# MAGIC * Start with Serverless (fastest, easiest)
# MAGIC * Use All-Purpose for custom needs
# MAGIC * Enable auto-termination (save costs)
# MAGIC
# MAGIC 🟢 **For Production**
# MAGIC * Use Job Clusters (ephemeral, cost-effective)
# MAGIC * Set cluster policies (enforce standards)
# MAGIC * Monitor usage and costs
# MAGIC
# MAGIC 🟢 **For Analytics**
# MAGIC * Use SQL Warehouses (optimized for BI)
# MAGIC * Enable Serverless SQL (instant on)
# MAGIC * Set auto-stop timers

# COMMAND ----------

# DBTITLE 1,6. Notebooks Interactive Development
# MAGIC %md
# MAGIC ## 6. Notebooks - Interactive Development 📓
# MAGIC
# MAGIC ### What are Databricks Notebooks?
# MAGIC
# MAGIC **Notebooks** are interactive, web-based development environments for data engineering, data science, and analytics.
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC #### 1️⃣ **Multi-Language Support**
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────┐
# MAGIC │         SAME NOTEBOOK, MULTIPLE LANGUAGES      │
# MAGIC │                                              │
# MAGIC │  🐍 Python   📊 SQL   ☕ Scala   📈 R    | Java  │
# MAGIC │                                              │
# MAGIC │  Default language + Magic commands (%py, %sql)  │
# MAGIC └──────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC * **Python**: Default for most data engineering/DS
# MAGIC * **SQL**: Query tables, create views
# MAGIC * **Scala**: Performance-critical Spark code
# MAGIC * **R**: Statistical analysis, ggplot
# MAGIC * **Markdown**: Documentation with `%md`
# MAGIC
# MAGIC #### 2️⃣ **Cell-Based Execution**
# MAGIC
# MAGIC ```python
# MAGIC # Cell 1: Import libraries
# MAGIC import pandas as pd
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # Cell 2: Load data
# MAGIC df = spark.table("sales")
# MAGIC
# MAGIC # Cell 3: Transform
# MAGIC df_agg = df.groupBy("region").agg(F.sum("revenue"))
# MAGIC
# MAGIC # Cell 4: Visualize
# MAGIC display(df_agg)
# MAGIC ```
# MAGIC
# MAGIC * Run cells individually or all at once
# MAGIC * Results displayed inline
# MAGIC * Stateful execution (variables persist)
# MAGIC
# MAGIC #### 3️⃣ **Real-Time Collaboration**
# MAGIC
# MAGIC * 👥 Multiple users edit simultaneously
# MAGIC * 💬 Comments and discussions
# MAGIC * 👁️ See who's viewing/editing
# MAGIC * 🔔 Notifications for mentions
# MAGIC
# MAGIC #### 4️⃣ **Visualizations**
# MAGIC
# MAGIC * Built-in charts (bar, line, pie, etc.)
# MAGIC * `display()` function for rich output
# MAGIC * Support for matplotlib, plotly, seaborn
# MAGIC * Interactive tables with filtering/sorting
# MAGIC
# MAGIC #### 5️⃣ **Version Control**
# MAGIC
# MAGIC * Revision history (automatic)
# MAGIC * Git integration via Repos
# MAGIC * Compare versions
# MAGIC * Restore previous versions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Notebook Structure
# MAGIC
# MAGIC ```
# MAGIC 📓 Notebook
# MAGIC │
# MAGIC ├── 📝 Title & Description
# MAGIC ├── 💻 Compute Selection (attach to cluster)
# MAGIC ├── 📦 Cell 1: Imports
# MAGIC ├── 📦 Cell 2: Configuration
# MAGIC ├── 📦 Cell 3: Data Loading
# MAGIC ├── 📦 Cell 4: Transformations
# MAGIC ├── 📦 Cell 5: Visualizations
# MAGIC └── 📦 Cell N: Export/Save
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Magic Commands
# MAGIC
# MAGIC | **Command** | **Purpose** | **Example** |
# MAGIC |-------------|-------------|-------------|
# MAGIC | `%python` | Python code | `%python print("Hello")` |
# MAGIC | `%sql` | SQL query | `%sql SELECT * FROM table` |
# MAGIC | `%scala` | Scala code | `%scala val x = 10` |
# MAGIC | `%r` | R code | `%r plot(x, y)` |
# MAGIC | `%md` | Markdown | `%md # Title` |
# MAGIC | `%sh` | Shell command | `%sh ls -la` |
# MAGIC | `%fs` | File system | `%fs ls /mnt/data` |
# MAGIC | `%pip` | Install package | `%pip install requests` |
# MAGIC | `%run` | Run another notebook | `%run ./setup` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Keyboard Shortcuts
# MAGIC
# MAGIC | **Action** | **Shortcut** |
# MAGIC |------------|---------------|
# MAGIC | Run cell | `Shift + Enter` |
# MAGIC | Run all cells | `Ctrl/Cmd + Shift + Enter` |
# MAGIC | Add cell below | `B` |
# MAGIC | Delete cell | `D D` (press D twice) |
# MAGIC | Convert to markdown | `M` |
# MAGIC | Convert to code | `Y` |
# MAGIC | Save notebook | `Ctrl/Cmd + S` |
# MAGIC | Comment/uncomment | `Ctrl/Cmd + /` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Notebook vs Traditional IDE
# MAGIC
# MAGIC | **Feature** | **Notebook** | **IDE (PyCharm, VS Code)** |
# MAGIC |-------------|--------------|----------------------------|
# MAGIC | **Interactivity** | ✅ High | ❌ Lower |
# MAGIC | **Visualization** | ✅ Built-in | ⚠️ Requires setup |
# MAGIC | **Collaboration** | ✅ Real-time | ❌ File-based |
# MAGIC | **Big Data** | ✅ Native Spark | ⚠️ Complex setup |
# MAGIC | **Documentation** | ✅ Inline markdown | ⚠️ Separate docs |
# MAGIC | **Debugging** | ⚠️ Basic | ✅ Advanced |
# MAGIC | **Version Control** | ✅ Git + Repos | ✅ Git |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Use markdown cells for documentation
# MAGIC * Organize code into logical cells
# MAGIC * Add cell titles for clarity
# MAGIC * Use `display()` for DataFrames
# MAGIC * Save frequently (auto-save enabled)
# MAGIC * Use version control (Repos)
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC * Put all code in one giant cell
# MAGIC * Forget to attach to compute
# MAGIC * Hardcode credentials (use secrets)
# MAGIC * Skip documentation
# MAGIC * Ignore warnings/errors
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Notebook Features You're Using Now!
# MAGIC
# MAGIC **This notebook demonstrates:**
# MAGIC * ✅ Multi-language (Python, Markdown, SQL)
# MAGIC * ✅ Cell-based execution
# MAGIC * ✅ Inline visualizations
# MAGIC * ✅ Compute attachment (Serverless)
# MAGIC * ✅ Rich text formatting
# MAGIC * ✅ Code + documentation together
# MAGIC
# MAGIC **Try it yourself in the hands-on section below!** 👇

# COMMAND ----------

# DBTITLE 1,7. Workspace Assets Overview
# MAGIC %md
# MAGIC ## 7. Workspace Assets Overview 🗂️
# MAGIC
# MAGIC ### All Workspace Assets
# MAGIC
# MAGIC Databricks Workspace contains multiple asset types for different purposes:
# MAGIC
# MAGIC ```
# MAGIC       DATABRICKS WORKSPACE ASSETS
# MAGIC ┌─────────────────────────────────────────────────┐
# MAGIC │                                                 │
# MAGIC │  📓 Notebooks       Interactive development      │
# MAGIC │  📁 Files           Python, config, data files  │
# MAGIC │  💾 Volumes         Unity Catalog storage       │
# MAGIC │  🔄 Repos           Git integration             │
# MAGIC │  📊 Dashboards      AI/BI visualizations        │
# MAGIC │  🚀 Apps            Custom data applications    │
# MAGIC │  ⚙️ Jobs            Workflow automation         │
# MAGIC │  🔍 Catalog         Data discovery              │
# MAGIC │  💻 Compute         Clusters & warehouses       │
# MAGIC │                                                 │
# MAGIC └─────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📁 Files
# MAGIC
# MAGIC **What**: Store Python scripts, config files, data files in workspace
# MAGIC
# MAGIC **File Types**:
# MAGIC * `.py` - Python modules
# MAGIC * `.sql` - SQL scripts
# MAGIC * `.json`, `.yaml`, `.txt` - Config/data files
# MAGIC * `.md` - Documentation
# MAGIC
# MAGIC **Use Cases**:
# MAGIC * Shared utilities (import in notebooks)
# MAGIC * Configuration files
# MAGIC * Small reference data
# MAGIC * Documentation
# MAGIC
# MAGIC **Location**: `/Workspace/Users/<your-email>/`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💾 Unity Catalog Volumes
# MAGIC
# MAGIC **What**: Managed cloud storage for files (governed by Unity Catalog)
# MAGIC
# MAGIC **Features**:
# MAGIC * 🔒 **Governed**: Permissions, audit logs
# MAGIC * 🌐 **Cloud-native**: S3, ADLS, GCS
# MAGIC * 📄 **Any file type**: Images, PDFs, models
# MAGIC * ⚡ **High performance**: Optimized access
# MAGIC
# MAGIC **Structure**:
# MAGIC ```
# MAGIC catalog.schema.volume_name
# MAGIC   /path/to/files
# MAGIC ```
# MAGIC
# MAGIC **Use Cases**:
# MAGIC * ML models (save/load)
# MAGIC * Images, videos for ML
# MAGIC * PDFs, documents for processing
# MAGIC * Checkpoints, logs
# MAGIC
# MAGIC **Access**:
# MAGIC ```python
# MAGIC # Read/write files in volumes
# MAGIC path = "/Volumes/catalog/schema/volume/file.csv"
# MAGIC df = spark.read.csv(path)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Repos (Git Integration)
# MAGIC
# MAGIC **What**: Connect Git repositories to Databricks workspace
# MAGIC
# MAGIC **Supported**:
# MAGIC * GitHub
# MAGIC * GitLab
# MAGIC * Azure DevOps
# MAGIC * Bitbucket
# MAGIC * Any Git provider
# MAGIC
# MAGIC **Workflow**:
# MAGIC ```
# MAGIC 1. Clone repo → 2. Edit in notebooks/files → 3. Commit & push
# MAGIC ```
# MAGIC
# MAGIC **Benefits**:
# MAGIC * ✅ Version control for notebooks
# MAGIC * ✅ CI/CD integration
# MAGIC * ✅ Team collaboration
# MAGIC * ✅ Branch management
# MAGIC * ✅ Pull requests
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Dashboards (AI/BI)
# MAGIC
# MAGIC **What**: Interactive data visualizations and reporting
# MAGIC
# MAGIC **Features**:
# MAGIC * 📊 Drag-and-drop charts
# MAGIC * 🔄 Auto-refresh
# MAGIC * 🔗 Shareable links
# MAGIC * 🤖 Natural language queries (Genie)
# MAGIC * 📱 Mobile-friendly
# MAGIC
# MAGIC **Dashboard Types**:
# MAGIC * **AI/BI Dashboards**: Modern, Genie-powered
# MAGIC * **SQL Dashboards**: Classic SQL-based
# MAGIC
# MAGIC **Use Cases**:
# MAGIC * Executive KPI dashboards
# MAGIC * Operational monitoring
# MAGIC * Self-service analytics
# MAGIC * Embedded reporting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Apps (Databricks Apps)
# MAGIC
# MAGIC **What**: Deploy custom data applications
# MAGIC
# MAGIC **Framework Support**:
# MAGIC * Streamlit
# MAGIC * Gradio
# MAGIC * Flask
# MAGIC * Dash
# MAGIC * Custom Python apps
# MAGIC
# MAGIC **Features**:
# MAGIC * 🚀 One-click deployment
# MAGIC * 🔒 Built-in auth
# MAGIC * 📊 Direct data access
# MAGIC * 📈 Auto-scaling
# MAGIC
# MAGIC **Use Cases**:
# MAGIC * ML model demos
# MAGIC * Internal tools
# MAGIC * Data apps for business users
# MAGIC * Interactive dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚙️ Jobs (Workflows)
# MAGIC
# MAGIC **What**: Schedule and orchestrate data workflows
# MAGIC
# MAGIC **Job Types**:
# MAGIC * Notebook jobs
# MAGIC * Python script jobs
# MAGIC * JAR jobs
# MAGIC * Pipeline jobs
# MAGIC
# MAGIC **Features**:
# MAGIC * 📅 Scheduling (cron, manual, API)
# MAGIC * 🔄 Dependencies (task DAGs)
# MAGIC * 🔔 Alerts (email, Slack, webhooks)
# MAGIC * 📊 Monitoring (logs, metrics)
# MAGIC * ↩️ Retries & error handling
# MAGIC
# MAGIC **Workflow Example**:
# MAGIC ```
# MAGIC Job: Daily Sales ETL
# MAGIC   Task 1: Extract raw data
# MAGIC   Task 2: Transform & clean
# MAGIC   Task 3: Load to gold table
# MAGIC   Task 4: Send email report
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Asset Organization
# MAGIC
# MAGIC #### Workspace Folder Structure
# MAGIC ```
# MAGIC /Workspace/
# MAGIC   /Users/
# MAGIC     /your-email@company.com/
# MAGIC       /Projects/
# MAGIC         /project-1/
# MAGIC           notebook1.py
# MAGIC           notebook2.sql
# MAGIC       /Scripts/
# MAGIC         utils.py
# MAGIC       /Config/
# MAGIC         settings.json
# MAGIC   /Shared/
# MAGIC     /Team/
# MAGIC       shared-notebook.py
# MAGIC ```
# MAGIC
# MAGIC #### Best Practices
# MAGIC * 📁 Organize by project
# MAGIC * 📝 Use descriptive names
# MAGIC * 👥 Share via `/Shared` folder
# MAGIC * 🔄 Use Repos for production code
# MAGIC * 💾 Use Volumes for data/models
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Quick Reference
# MAGIC
# MAGIC | **Asset** | **When to Use** | **Location** |
# MAGIC |-----------|-----------------|---------------|
# MAGIC | **Notebooks** | Interactive dev | Workspace |
# MAGIC | **Files** | Python modules, scripts | Workspace |
# MAGIC | **Volumes** | Data files, ML models | Unity Catalog |
# MAGIC | **Repos** | Git version control | Workspace |
# MAGIC | **Dashboards** | BI & reporting | Dashboards tab |
# MAGIC | **Apps** | Custom applications | Apps tab |
# MAGIC | **Jobs** | Scheduled workflows | Workflows tab |

# COMMAND ----------

# DBTITLE 1,8. Databricks UI Navigation
# MAGIC %md
# MAGIC ## 8. Databricks UI Navigation 🧭
# MAGIC
# MAGIC ### Left Navigation Panel
# MAGIC
# MAGIC The left sidebar is your main navigation hub:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────┐
# MAGIC │  DATABRICKS WORKSPACE  │
# MAGIC │                       │
# MAGIC │  🏠 Home               │ ← Your personal space
# MAGIC │  🔍 Catalog            │ ← Browse data (Unity Catalog)
# MAGIC │  📓 Workspace          │ ← Notebooks, files, folders
# MAGIC │  💻 Compute            │ ← Clusters & SQL warehouses
# MAGIC │  ⚙️ Workflows           │ ← Jobs & pipelines
# MAGIC │  📊 Dashboards         │ ← AI/BI visualizations
# MAGIC │  🚀 Apps               │ ← Databricks Apps
# MAGIC │  🤖 Machine Learning   │ ← MLflow, models, experiments
# MAGIC │  🛡️ Admin Settings     │ ← Workspace configuration
# MAGIC │                       │
# MAGIC └─────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Sections Explained
# MAGIC
# MAGIC #### 🏠 **Home**
# MAGIC * Recent notebooks & queries
# MAGIC * Quick actions (create notebook, query, etc.)
# MAGIC * Recommended assets
# MAGIC * Activity feed
# MAGIC
# MAGIC #### 🔍 **Catalog (Data Explorer)**
# MAGIC * **Browse data**: Catalogs → Schemas → Tables
# MAGIC * **Table details**: Schema, sample data, history
# MAGIC * **Data lineage**: Upstream/downstream dependencies
# MAGIC * **Permissions**: Grant/revoke access
# MAGIC * **Volumes**: Browse Unity Catalog volumes
# MAGIC
# MAGIC **Navigation**:
# MAGIC ```
# MAGIC Catalog
# MAGIC  └─ main
# MAGIC      └─ default
# MAGIC          ├─ customers (table)
# MAGIC          ├─ orders (table)
# MAGIC          └─ volume_name (volume)
# MAGIC ```
# MAGIC
# MAGIC #### 📓 **Workspace**
# MAGIC * **Folders**: Your files, shared folders, repos
# MAGIC * **Notebooks**: `.ipynb`, `.py`, `.sql`, `.r`, `.scala`
# MAGIC * **Files**: Python scripts, configs, data
# MAGIC * **Repos**: Git repositories
# MAGIC
# MAGIC **Actions**:
# MAGIC * Create new notebook/file/folder
# MAGIC * Import notebooks
# MAGIC * Clone repos
# MAGIC * Move/rename/delete
# MAGIC
# MAGIC #### 💻 **Compute**
# MAGIC * **All-Purpose Clusters**: Create, start, stop, config
# MAGIC * **SQL Warehouses**: Serverless SQL, Pro, Classic
# MAGIC * **Cluster Policies**: Enforce standards
# MAGIC * **Cluster logs**: Driver/executor logs
# MAGIC
# MAGIC **Key Info Displayed**:
# MAGIC * Cluster state (running, terminated, pending)
# MAGIC * Cost per hour
# MAGIC * Configuration (nodes, instance type)
# MAGIC * Attached notebooks
# MAGIC
# MAGIC #### ⚙️ **Workflows**
# MAGIC * **Jobs**: Scheduled and on-demand workflows
# MAGIC * **Runs**: Job run history and logs
# MAGIC * **Pipelines**: Delta Live Tables / Lakeflow pipelines
# MAGIC
# MAGIC **Job Dashboard**:
# MAGIC * Run status (success, failed, running)
# MAGIC * Schedule and next run time
# MAGIC * Task dependencies (DAG view)
# MAGIC * Logs and metrics
# MAGIC
# MAGIC #### 📊 **Dashboards**
# MAGIC * **AI/BI Dashboards**: Modern dashboards
# MAGIC * **Create**: New dashboard from scratch
# MAGIC * **Genie Spaces**: Natural language analytics
# MAGIC * **Sharing**: Permissions and embedding
# MAGIC
# MAGIC #### 🚀 **Apps**
# MAGIC * **Your Apps**: Deployed applications
# MAGIC * **Deploy**: New app from notebook
# MAGIC * **Status**: Running, stopped, building
# MAGIC * **Logs**: Application logs
# MAGIC
# MAGIC #### 🤖 **Machine Learning**
# MAGIC * **Experiments**: MLflow experiment tracking
# MAGIC * **Models**: Registered models (Unity Catalog)
# MAGIC * **Model Serving**: Deployed model endpoints
# MAGIC * **Feature Engineering**: Feature Store
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Top Bar Navigation
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────┐
# MAGIC │  🔍 Search  |  🔔 Notifications  |  ❓ Help  |  👤 Profile  │
# MAGIC └────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC * **🔍 Search**: Find notebooks, tables, users, anything
# MAGIC * **🔔 Notifications**: Alerts, job failures, mentions
# MAGIC * **❓ Help**: Documentation, tutorials, support
# MAGIC * **👤 Profile**: Settings, preferences, account
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Quick Actions
# MAGIC
# MAGIC **Create Assets Fast**:
# MAGIC * **Notebook**: `Cmd/Ctrl + N` or click `+ New` button
# MAGIC * **SQL Query**: Click `+ New` → Query
# MAGIC * **Dashboard**: Dashboards → `+ Create Dashboard`
# MAGIC * **Job**: Workflows → `+ Create Job`
# MAGIC
# MAGIC **Keyboard Shortcuts**:
# MAGIC * **Search**: `Cmd/Ctrl + K`
# MAGIC * **Command Palette**: `Cmd/Ctrl + Shift + P`
# MAGIC * **Quick Navigate**: `Cmd/Ctrl + P`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Common Workflows
# MAGIC
# MAGIC #### 📊 **Analytics Workflow**
# MAGIC 1. **Catalog** → Browse tables
# MAGIC 2. **Workspace** → Create SQL notebook
# MAGIC 3. **Compute** → Attach SQL warehouse
# MAGIC 4. Write query → Run
# MAGIC 5. **Dashboards** → Create visualization
# MAGIC
# MAGIC #### 🚀 **Data Engineering Workflow**
# MAGIC 1. **Workspace** → Create Python notebook
# MAGIC 2. **Compute** → Attach cluster
# MAGIC 3. Write ETL code → Run
# MAGIC 4. **Workflows** → Create job
# MAGIC 5. Schedule → Monitor
# MAGIC
# MAGIC #### 🤖 **ML Workflow**
# MAGIC 1. **Catalog** → Find training data
# MAGIC 2. **Workspace** → Create notebook
# MAGIC 3. Train model → Log with MLflow
# MAGIC 4. **Machine Learning** → Register model
# MAGIC 5. **Model Serving** → Deploy endpoint
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Navigation Tips
# MAGIC
# MAGIC ✅ **Breadcrumbs**: Always visible at top, click to go back
# MAGIC ✅ **Right-click**: Context menus for quick actions
# MAGIC ✅ **Search**: Fastest way to find anything (`Cmd/Ctrl + K`)
# MAGIC ✅ **Recent**: Home page shows recent items
# MAGIC ✅ **Favorites**: Star important assets for quick access
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Admin Settings (Admins Only)
# MAGIC
# MAGIC * **Workspace settings**: Configuration
# MAGIC * **Users & groups**: Manage access
# MAGIC * **Unity Catalog**: Governance setup
# MAGIC * **Compute policies**: Cluster constraints
# MAGIC * **Billing**: Usage and costs

# COMMAND ----------

# DBTITLE 1,HANDS-ON 1: Your First Python Code
print("🎉 HANDS-ON EXERCISE 1: Your First Python Code in Databricks!")
print("="*70)

# Task 1: Simple Python
print("\n🐍 Task 1: Basic Python")
print("-" * 40)

name = "Databricks User"
greeting = f"Hello, {name}! Welcome to Databricks!"
print(greeting)

numbers = [1, 2, 3, 4, 5]
total = sum(numbers)
print(f"Sum of {numbers} = {total}")

# Task 2: Create a PySpark DataFrame
print("\n⚡ Task 2: Your First Spark DataFrame")
print("-" * 40)

from pyspark.sql import functions as F

# Create sample data
data = [
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Sales", 75000),
    (3, "Charlie", "Engineering", 100000),
    (4, "Diana", "Marketing", 80000),
    (5, "Eve", "Engineering", 92000)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

print(f"\u2705 Created DataFrame with {df.count()} rows")
print("\nFirst 5 rows:")
df.show()

# Task 3: Transform data
print("\n🔄 Task 3: Data Transformation")
print("-" * 40)

# Add bonus column (10% of salary)
df_with_bonus = df.withColumn("bonus", F.col("salary") * 0.10)

print("Added 10% bonus column:")
df_with_bonus.show()

# Task 4: Aggregate data
print("\n📊 Task 4: Aggregation")
print("-" * 40)

# Average salary by department
df_avg_salary = df.groupBy("department").agg(
    F.avg("salary").alias("avg_salary"),
    F.count("*").alias("employee_count")
).orderBy("avg_salary", ascending=False)

print("Average salary by department:")
df_avg_salary.show()

# Task 5: Visualization
print("\n📊 Task 5: Data Visualization")
print("-" * 40)

print("Using display() for interactive visualization:")
print("(Click the chart icon in the output below to see different visualizations)")

# Convert to pandas for visualization
import matplotlib.pyplot as plt
import pandas as pd

pandas_df = df_avg_salary.toPandas()

fig, ax = plt.subplots(figsize=(10, 6))
ax.bar(pandas_df['department'], pandas_df['avg_salary'], color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
ax.set_xlabel('Department', fontsize=12, fontweight='bold')
ax.set_ylabel('Average Salary ($)', fontsize=12, fontweight='bold')
ax.set_title('Average Salary by Department', fontsize=14, fontweight='bold')
ax.grid(axis='y', alpha=0.3)

# Add value labels on bars
for i, (dept, sal) in enumerate(zip(pandas_df['department'], pandas_df['avg_salary'])):
    ax.text(i, sal + 1000, f'${sal:,.0f}', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.show()

# Summary
print("\n" + "="*70)
print("✅ EXERCISE 1 COMPLETED!")
print("="*70)
print("\nWhat you learned:")
print("1️⃣  Basic Python in Databricks notebooks")
print("2️⃣  Creating Spark DataFrames from data")
print("3️⃣  Adding and transforming columns")
print("4️⃣  Aggregating data (groupBy, avg, count)")
print("5️⃣  Visualizing results with matplotlib")
print("\n🎉 Great job! Ready for SQL? Run the next cell!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,HANDS-ON 2: Your First SQL Query
# MAGIC %sql
# MAGIC -- 📊 HANDS-ON EXERCISE 2: Your First SQL Query in Databricks!
# MAGIC -- ================================================================
# MAGIC
# MAGIC -- First, let's create a temporary view from our Python DataFrame
# MAGIC -- (Run the Python cell above first!)
# MAGIC
# MAGIC -- Task 1: Create a table with sample data
# MAGIC CREATE OR REPLACE TEMP VIEW employees AS
# MAGIC SELECT * FROM VALUES
# MAGIC     (1, 'Alice', 'Engineering', 95000, 'San Francisco'),
# MAGIC     (2, 'Bob', 'Sales', 75000, 'New York'),
# MAGIC     (3, 'Charlie', 'Engineering', 100000, 'San Francisco'),
# MAGIC     (4, 'Diana', 'Marketing', 80000, 'Boston'),
# MAGIC     (5, 'Eve', 'Engineering', 92000, 'San Francisco'),
# MAGIC     (6, 'Frank', 'Sales', 70000, 'New York'),
# MAGIC     (7, 'Grace', 'Marketing', 85000, 'Boston'),
# MAGIC     (8, 'Henry', 'Engineering', 98000, 'Seattle'),
# MAGIC     (9, 'Iris', 'Sales', 72000, 'Seattle'),
# MAGIC     (10, 'Jack', 'Marketing', 83000, 'Boston')
# MAGIC AS employees(id, name, department, salary, location);
# MAGIC
# MAGIC SELECT '✅ Created employees table with 10 rows' AS status;
# MAGIC
# MAGIC -- Task 2: Basic SELECT
# MAGIC SELECT 
# MAGIC     '📋 Task 2: View All Employees' AS task,
# MAGIC     id,
# MAGIC     name,
# MAGIC     department,
# MAGIC     salary,
# MAGIC     location
# MAGIC FROM employees
# MAGIC ORDER BY salary DESC;
# MAGIC
# MAGIC -- Task 3: Filter data
# MAGIC SELECT 
# MAGIC     '🔍 Task 3: Engineering Department Only' AS task,
# MAGIC     name,
# MAGIC     salary,
# MAGIC     location
# MAGIC FROM employees
# MAGIC WHERE department = 'Engineering'
# MAGIC ORDER BY salary DESC;
# MAGIC
# MAGIC -- Task 4: Aggregations
# MAGIC SELECT 
# MAGIC     '📊 Task 4: Department Statistics' AS task,
# MAGIC     department,
# MAGIC     COUNT(*) AS employee_count,
# MAGIC     AVG(salary) AS avg_salary,
# MAGIC     MIN(salary) AS min_salary,
# MAGIC     MAX(salary) AS max_salary
# MAGIC FROM employees
# MAGIC GROUP BY department
# MAGIC ORDER BY avg_salary DESC;
# MAGIC
# MAGIC -- Task 5: Multiple aggregations by location
# MAGIC SELECT 
# MAGIC     '🌎 Task 5: Location Analysis' AS task,
# MAGIC     location,
# MAGIC     COUNT(*) AS employees,
# MAGIC     ROUND(AVG(salary), 0) AS avg_salary,
# MAGIC     COLLECT_LIST(department) AS departments
# MAGIC FROM employees
# MAGIC GROUP BY location
# MAGIC ORDER BY employees DESC, avg_salary DESC;
# MAGIC
# MAGIC -- Task 6: Advanced - CASE statement
# MAGIC SELECT 
# MAGIC     '🏆 Task 6: Salary Bands' AS task,
# MAGIC     name,
# MAGIC     department,
# MAGIC     salary,
# MAGIC     CASE 
# MAGIC         WHEN salary >= 90000 THEN 'Senior'
# MAGIC         WHEN salary >= 75000 THEN 'Mid-Level'
# MAGIC         ELSE 'Junior'
# MAGIC     END AS level
# MAGIC FROM employees
# MAGIC ORDER BY salary DESC;
# MAGIC
# MAGIC -- Summary
# MAGIC SELECT '✅ EXERCISE 2 COMPLETED!' AS status,
# MAGIC        'You learned: SELECT, WHERE, GROUP BY, aggregations, CASE' AS topics;