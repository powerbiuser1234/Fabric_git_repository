# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9c04cb09-56e5-49d6-a2b0-5e2e578f481a",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "197819da-2cec-4c3c-a96a-0fe62ae2300b",
# META       "known_lakehouses": [
# META         {
# META           "id": "0bba3a6e-afd6-42a2-a721-dff287a67789"
# META         },
# META         {
# META           "id": "2e2c9b55-5b79-4ca1-880c-bf0b64fd0495"
# META         },
# META         {
# META           "id": "9c04cb09-56e5-49d6-a2b0-5e2e578f481a"
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
df_staging = spark.read.format("delta").table("SilverLakehouse.ClearingCommFact")

# Load Data Warehouse Table
df_dwh = spark.read.format("delta").table("GoldLakehouse.ClearingCommFact")

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
    (col("staging.Sequence") == col("dwh.Sequence")) &
    (col("staging.ChargeType")==col("dwh.ChargeType")),
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
df_new_records.write.format("delta").mode("append").saveAsTable("GoldLakehouse.ClearingCommFact")

print("New records inserted successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
