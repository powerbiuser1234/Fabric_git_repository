# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2e2c9b55-5b79-4ca1-880c-bf0b64fd0495",
# META       "default_lakehouse_name": "GoldLakehouse",
# META       "default_lakehouse_workspace_id": "197819da-2cec-4c3c-a96a-0fe62ae2300b",
# META       "known_lakehouses": [
# META         {
# META           "id": "0bba3a6e-afd6-42a2-a721-dff287a67789"
# META         },
# META         {
# META           "id": "2e2c9b55-5b79-4ca1-880c-bf0b64fd0495"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### 1. Libraries

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Defining SCD1 Function

# CELL ********************

# Define function to perform SCD1 Merge
def scd1_merge(source_table, target_table, primary_key):
    # Load Source and Target as Delta Tables
    source_df = spark.read.format("delta").table(source_table)
    target_delta = DeltaTable.forName(spark, target_table)

    # Define Merge Condition (Matching Primary Key)
    merge_condition = f"target.{primary_key} = source.{primary_key}"

    # Define Update Clause (Excluding Primary Key & System Columns)
    update_columns = [col for col in source_df.columns if col not in [primary_key, "CreatedOn"]]
    update_expr = {f"target.{col}": f"source.{col}" for col in update_columns}

    # Perform Delta MERGE (UPSERT)
    target_delta.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        condition="target.RowHash <> source.RowHash",  # Update only if data has changed
        set=update_expr | {"target.UpdatedOn": "source.BatchDate"}  # Update timestamp
    ).whenNotMatchedInsert(
        values={col: f"source.{col}" for col in source_df.columns}
    ).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Applying SCD1 on Target Table "**GoldLakehouse.ClearingDim**"

# CELL ********************

# Run the function for your table
scd1_merge("SilverLakehouse.ClearingDim", "GoldLakehouse.ClearingDim", "ClearingID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select CreatedOn, UpdatedOn, count(*) from GoldLakehouse.ClearingDim
# MAGIC group by CreatedOn, UpdatedOn
# MAGIC order by CreatedOn, UpdatedOn;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
