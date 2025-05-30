# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ef516724-eca9-40d5-92bc-40212bb6944e",
# META       "default_lakehouse_name": "GoldLakehouse",
# META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785",
# META       "known_lakehouses": [
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

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define function to perform SCD1 Merge
def scd1_merge(source_table, target_table, primary_key):
    # Load Source and Target as Delta Tables
    source_df = spark.read.format("delta").table(source_table)
    target_delta = DeltaTable.forName(spark, target_table)

    # Define Merge Condition (Matching Primary Key)
    merge_condition = f"target.{primary_key} = source.{primary_key} AND target.AgenceCode = source.AgenceCode"

    # Define Update Clause (Excluding Primary Key & System Columns)
    update_columns = [col for col in source_df.columns if col not in [primary_key, "CreatedOn"]]
    update_expr = {f"target.{col}": f"source.{col}" for col in update_columns}

    # Perform Delta MERGE (UPSERT)
    target_delta.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        set=update_expr | {"target.Updated_On": "source.Batch_Date"}  # Update timestamp
    ).whenNotMatchedInsert(
        values={col: f"source.{col}" for col in source_df.columns}
    ).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run the function for your table
scd1_merge("SilverLakehouse.CbsODLimitTypeDim", "GoldLakehouse.CbsODLimitTypeDim", "ODLimitTypeCode")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from GoldLakehouse.CbsODLimitTypeDim

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
