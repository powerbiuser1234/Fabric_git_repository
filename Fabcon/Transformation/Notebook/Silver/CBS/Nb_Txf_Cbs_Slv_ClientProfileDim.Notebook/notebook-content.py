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

# CELL ********************

#Bronze Tables
Cbs_Bkprocli = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkprocli")
# Perform the equivalent transformations in PySpark
Cbs_Bkprocli_trimmed = Cbs_Bkprocli.select(
    trim(col("PRO")).alias("PRO"),
    trim(col("CLI")).alias("CLI"),
    trim(col("DPRO")).alias("DPRO"),
    'BatchID',
    'BatchDATE',
    'SystemCode'
)

#Gold Tables
Client_Dim = spark.read.format("delta").table("GoldLakehouse.CbsClientDim")

Bknom = spark.read.format("delta").table("GoldLakehouse.CbsBknom")
# Filter BKNOM table for CTAB = '050'
bknom_filtered_df = Bknom.filter(col("CTAB") == '050')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df = (
    Cbs_Bkprocli_trimmed
    .join(Client_Dim, Cbs_Bkprocli_trimmed.CLI == Client_Dim.ClientBK, "left")
    .join(bknom_filtered_df, Cbs_Bkprocli_trimmed.PRO == bknom_filtered_df.CACC, "left")
    .select(
        Client_Dim.ClientID.alias("ClientID"),
        Cbs_Bkprocli_trimmed.PRO.alias("ProfileCode"),
        bknom_filtered_df.LIB1.alias("Profile"),
        Cbs_Bkprocli_trimmed.CLI.alias("ClientBK"),
        Cbs_Bkprocli_trimmed.DPRO.alias("ProfileDate"),
        Cbs_Bkprocli_trimmed.BatchID,
        Cbs_Bkprocli_trimmed.BatchDATE,
        Cbs_Bkprocli_trimmed.BatchDATE.cast("date").alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        Cbs_Bkprocli_trimmed.SystemCode,
        lit(None).cast("binary").alias("RowHash"),
        lit("P_Dly_Cbs_Slv").alias("WorkflowName")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generating Row Hash

# CELL ********************

#creating rowhash

result_df_hash = result_df.withColumn("RowHash", sha2(concat_ws("", "ClientID", "ProfileCode", "Profile", "ClientBK", "ProfileDate"), \
                                                256).cast("binary")) #empty string in start is a seperator

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

result_df_hash.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsClientProfileDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
