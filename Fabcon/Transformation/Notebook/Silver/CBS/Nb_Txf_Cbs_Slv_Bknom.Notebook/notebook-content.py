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

BKNOM = spark.read.format("delta").table("BronzeLakehouse.CBS_Bknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Bknom Silver ETL

# MARKDOWN ********************

# #### 3.1 Creating Dataframe

# CELL ********************

BKNOM.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df_bknom = BKNOM.select(
    trim(col("AGE")).alias("AGE"),
    trim(col("CTAB")).alias("CTAB"),
    trim(col("CACC")).alias("CACC"),
    trim(col("LIB1")).alias("LIB1"),
    trim(col("LIB2")).alias("LIB2"),
    trim(col("LIB3")).alias("LIB3"),
    trim(col("LIB4")).alias("LIB4"),
    trim(col("LIB5")).alias("LIB5"),
    col("MNT1"),
    col("MNT2"),
    col("MNT3"),
    col("MNT4"),
    col("MNT5"),
    col("MNT6"),
    col("MNT7"),
    col("MNT8"),
    col("TAU1"),
    col("TAU2"),
    col("TAU3"),
    col("TAU4"),
    col("TAU5"),
    trim(col("DUTI")).alias("DUTI"),
    col("DDOU"),
    col("DDMO"),
    col("BatchID"),
    col("BatchDate"),
    to_date(col("BatchDate")).alias("CREATEDON"),
    lit(None).cast("date").alias("UPDATEDON"),
    col("SystemCode"),
    lit(None).cast("binary").alias("ROWHASH"),
    lit("P_Dly_Cbs_Slv_Gld").alias("WorkflowName")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df = result_df_bknom.filter(~((col("CTAB") == "035") & (col("LIB1") == "SUDI  FAITH")))

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

filtered_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Writing Data into "**SilverLakehouse.Bknom**"

# CELL ********************

filtered_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT count(*) from SilverLakehouse.CbsBknom 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
