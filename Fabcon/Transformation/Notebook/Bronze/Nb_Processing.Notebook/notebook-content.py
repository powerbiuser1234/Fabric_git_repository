# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a43617b8-dd1a-46c9-aa3c-c2645e30c949",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785",
# META       "known_lakehouses": [
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
# META         },
# META         {
# META           "id": "a28baf80-5d91-4995-9acc-3a8bfa39eed9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import com.microsoft.spark.fabric  # Required in Fabric Notebooks
from pyspark.sql.functions import col
from delta.tables import DeltaTable


spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")


# Step 1: Read metadata from Warehouse
df_meta = spark.read.synapsesql("DataOpsWarehouse.Metadata.Src_Brn_Tbl_Lst")

# Step 2: Filter for CBS + Full Load
df_filtered = df_meta.filter(
    (col("SourceSystem") == "CBS") &
    (col("LoadType") == "Full") &
    (col("IsActive") == 1)
)

# Step 3: Rename columns as per new naming convention
df_filtered_renamed = df_filtered.withColumnRenamed("TargetSchema", "BronzeSchema") \
                                 .withColumnRenamed("TargetTable", "BronzeTable")



# Step 4: Collect metadata entries to loop
table_list = df_filtered_renamed.select("BronzeSchema", "BronzeTable").collect()

# Step 5: Loop over each table
for row in table_list:
     bronze_schema = row["BronzeSchema"]
     bronze_table = row["BronzeTable"]

     full_table_name = f"{bronze_schema}.{bronze_table}"

     print(f"Processing table: {full_table_name}")

     try:

         # Step 5.3: Truncate original Bronze table
         spark.sql(f"DELETE FROM {full_table_name}")
         print(f"Truncated table {full_table_name}")

     except Exception as e:
         print(f"Error processing {full_table_name}: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import com.microsoft.spark.fabric  # Required in Fabric Notebooks
from pyspark.sql.functions import col
from delta.tables import DeltaTable

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# Step 1: Read metadata from Warehouse
df_meta = spark.read.synapsesql("DataOpsWarehouse.Metadata.Src_Brn_Tbl_Lst")

# Step 2: Filter for CBS + Full Load
df_filtered = df_meta.filter(
    (col("SourceSystem") == "CBS") &
    (col("LoadType") == "Full") &
    (col("IsActive") == 1)
)

# Step 3: Rename columns as per new naming convention
df_filtered_renamed = df_filtered.withColumnRenamed("TargetTable", "BronzeTable")

# Step 4: Collect metadata entries to loop
table_list = df_filtered_renamed.select("BronzeTable").collect()

# Step 5: Loop over each table
for row in table_list:
    bronze_schema = "BronzeLakehouseHistory"  # Fixed schema name
    bronze_table = row["BronzeTable"]

    full_table_name = f"{bronze_schema}.{bronze_table}"

    print(f"Processing table: {full_table_name}")

    try:
        # Step 5.3: Truncate original Bronze table
        spark.sql(f"DELETE FROM {full_table_name}")
        print(f"Truncated table {full_table_name}")

    except Exception as e:
        print(f"Error processing {full_table_name}: {str(e)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Required libraries
import com.microsoft.spark.fabric  # Required in Fabric Notebooks
from pyspark.sql.functions import col

# Spark config settings
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# Step 1: Read metadata from Warehouse
df_meta = spark.read.synapsesql("DataOpsWarehouse.Metadata.Src_Brn_Tbl_Lst")

# Step 2: Filter for CBS + Full Load
df_filtered = df_meta.filter(
    (col("SourceSystem") == "CBS") &
    (col("LoadType") == "Full") &
    (col("IsActive") == 1)
)

# Step 3: Rename columns
df_filtered_renamed = df_filtered.withColumnRenamed("TargetSchema", "BronzeSchema") \
                                 .withColumnRenamed("TargetTable", "BronzeTable")

# Step 4: Collect table list
table_list = df_filtered_renamed.select("BronzeSchema", "BronzeTable").collect()

# Step 5: Recursive file size function
def get_total_size(path):
    total = 0
    try:
        items = dbutils.fs.ls(path)
        for item in items:
            if item.isDir():
                total += get_total_size(item.path)
            else:
                total += item.size
    except Exception as e:
        print(f"Path not found or inaccessible: {path}, error: {e}")
    return total

# Step 6: Loop through tables and calculate size
results = []

for row in table_list:
    schema = row["BronzeSchema"]
    table = row["BronzeTable"]
    
    # Adjust this path if your lakehouse has a different name
    # You can browse /lakehouse/ in Fabric notebook to confirm the exact folder name
    path = f"Bronze/{table}"  # Replace YourLakehouseName

    size_bytes = get_total_size(path)
    size_mb = round(size_bytes / (1024 * 1024), 2)
    
    print(f"{schema}.{table}: {size_mb} MB")

    results.append((schema, table, size_mb))

# Step 7: Create and display DataFrame
df_sizes = spark.createDataFrame(results, ["Schema", "Table", "Size_MB"])
df_sizes.show(200, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("DESCRIBE DETAIL BronzeLakehouse.BKALCOPI")
df.select("location").show(truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
