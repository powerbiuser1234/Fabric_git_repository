# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b554114f-f6f1-42fa-b182-35c9b03d7bfd",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "4f2aaff8-44cb-476d-805b-1c95e486af08",
# META       "known_lakehouses": [
# META         {
# META           "id": "7ba102aa-777a-4ad5-8f68-ce62922c90b3"
# META         },
# META         {
# META           "id": "5953e930-bc05-403f-8cad-ba3d204e63d3"
# META         },
# META         {
# META           "id": "b554114f-f6f1-42fa-b182-35c9b03d7bfd"
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
df_staging = spark.read.format("delta").table("SilverLakehouse.ClearingEventFact")

# Load Data Warehouse Table
df_dwh = spark.read.format("delta").table("GoldLakehouse.ClearingEventFact")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Perform Left Join to Identify New Records
df_new_records = df_staging.alias("staging").join(
    df_dwh.alias("dwh"),
    (col("staging.ClearingCode") == col("dwh.ClearingCode")) & 
    (col("staging.Sequence") == col("dwh.Sequence")),
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
df_new_records.write.format("delta").mode("append").saveAsTable("GoldLakehouse.ClearingEventFact")

print("New records inserted successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
