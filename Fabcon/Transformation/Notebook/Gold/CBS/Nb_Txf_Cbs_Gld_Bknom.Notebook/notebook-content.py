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
# META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType, BinaryType, DateType, TimestampType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#read Staging_Table
Staging_df = spark.read.format("delta").table("SilverLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df_bknom = Staging_df.select(
    "AGE",
    "CTAB",
    "CACC",
    "LIB1",
    "LIB2",
    "LIB3",
    "LIB4",
    "LIB5",
    "MNT1",
    "MNT2",
    "MNT3",
    "MNT4",
    "MNT5",
    "MNT6",
    "MNT7",
    "MNT8",
    "TAU1",
    "TAU2",
    "TAU3",
    "TAU4",
    "TAU5",
    "DUTI",
    "DDOU",
    "DDMO",
    "BatchID",
    "BatchDate",
    "CREATEDON",
    "UPDATEDON",
    "SystemCode",
    "ROWHASH",
    "WorkflowName" 
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

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df_bknom.write.format("delta").mode("overwrite").saveAsTable("GoldLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) from GoldLakehouse.CbsBknom

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
