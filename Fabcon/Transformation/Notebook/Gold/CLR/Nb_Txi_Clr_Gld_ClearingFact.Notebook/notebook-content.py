# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03",
# META       "default_lakehouse_name": "SilverLakehouse",
# META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785",
# META       "known_lakehouses": [
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         },
# META         {
# META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load Staging Table
df_staging = spark.read.format("delta").table("SilverLakehouse.clearingfact")

# # Load Data Warehouse Table
df_dwh = spark.read.format("delta").table("GoldLakehouse.ClearingFact")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Perform Left Join to Identify New Records
df_new_records = df_staging.alias("staging").join(
    df_dwh.alias("dwh"),
    col("staging.ClearingCode") == col("dwh.ClearingCode"),
    "left_anti"  # Fetch only records that are in staging but not in DWH
).select("staging.*")
display(df_new_records)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write New Records to Data Warehouse Table
df_new_records.write.format("delta").mode("Append").saveAsTable("GoldLakehouse.ClearingFact")

print("New records inserted successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
