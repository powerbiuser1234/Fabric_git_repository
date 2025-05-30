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
# META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### 1. Libraries

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType, BinaryType, DateType, TimestampType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Reading tables

# CELL ********************

#Bronze Tables
stg_prov_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Stg_Provience")
#Gold Tables
bknom_df = spark.read.format("delta").table("GoldLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Branch Silver ETL

# MARKDOWN ********************

# #### 3.1 Creating Resultant Dataframe

# CELL ********************

stg_prov_df = stg_prov_df.alias("prov")
bknom_df = bknom_df.alias("b")

# Perform the join and select with trimmed columns
result_df_branch = (
    stg_prov_df
    .join(
        bknom_df.filter(col("b.CTAB") == "001"),
        col("prov.BRANCH_CODE") == col("b.CACC"),
        "left"
    )
    .select(
        col("prov.BRANCH_CODE").alias("BranchCode"),
        col("prov.NAME_OF_THE_BRANCH").alias("BranchName"),
        trim(col("prov.CITY")).alias("City"),
        trim(col("prov.PROVINCE")).alias("Province"),
        trim(col("prov.ZONE")).alias("Zone"),
        trim(col("prov.TYPE")).alias("Type"),
        col("prov.BatchID"),
        col("prov.BatchDate"),
        to_date(col("prov.BatchDate")).alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        col("prov.SystemCode").alias("SystemCode"),
        lit(None).cast("binary").alias("RowHash"),
        lit("P_Dly_Cbs_Slv_Gld").alias("WorkflowName")
    )
    .dropDuplicates()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3.2 Adding Values to RowHash Column

# CELL ********************

# Make sure to use only the columns that exist in result_df_branch
result_df_branch = result_df_branch.withColumn(
    "RowHash",
    sha2(
        concat_ws(
            "",  # separator
            col("BranchCode"),
            col("BranchName"),
            col("City"),
            col("Province"),
            col("Zone"),
            col("Type")
        ),
        256
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.caseSensitive", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Writing Data into "**SilverLakehouse.Branch**"

# CELL ********************

result_df_branch.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsBranch")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
