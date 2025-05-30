# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7ba102aa-777a-4ad5-8f68-ce62922c90b3",
# META       "default_lakehouse_name": "GoldLakehouse",
# META       "default_lakehouse_workspace_id": "4f2aaff8-44cb-476d-805b-1c95e486af08",
# META       "known_lakehouses": [
# META         {
# META           "id": "5953e930-bc05-403f-8cad-ba3d204e63d3"
# META         },
# META         {
# META           "id": "7ba102aa-777a-4ad5-8f68-ce62922c90b3"
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
