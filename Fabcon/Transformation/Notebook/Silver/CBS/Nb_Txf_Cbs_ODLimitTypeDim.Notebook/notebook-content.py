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
Cbs_Bktyaut = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bktyaut")
# Perform the equivalent transformations in PySpark
Cbs_Bktyaut_ = Cbs_Bktyaut.select(
    'DOU',
    'DMO',
    trim(col("AGE")).alias("AGE"),
    trim(col("TYP")).alias("TYP"),
    trim(col("LIBE")).alias("LIBE"),
    trim(col("OPE")).alias("OPE"),
    trim(col("UTI")).alias("UTI"),
    trim(col("ATRF")).alias("ATRF"),
    trim(col("CPRO")).alias("CPRO"),
    trim(col("TEG_ARR")).alias("TEG_ARR"),
    trim(col("CALCDEC")).alias("CALCDEC"),
    'BatchID',
    'BatchDATE',
    'SystemCode'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Bknom = spark.read.format("delta").table("GoldLakehouse.CbsBknom")
# Filter BKNOM table for CTAB = '001'
bknom_filtered_df = Bknom.filter(col("CTAB") == '001') 

Cbs_Branch = spark.read.format("delta").table("GoldLakehouse.Branch")

Cbs_Evuti = spark.read.format("delta").table("BronzeLakehouse.Cbs_Evuti")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#CbsUsers = spark.read.format("delta").table("GoldLakehouse.CbsUsers")
Cbs_Branch

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Cbs_Evuti

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Cbs_Bktyaut_

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df = (
    Cbs_Bktyaut_
    .join(bknom_filtered_df, Cbs_Bktyaut_.AGE == bknom_filtered_df.CACC, "left")
    .join(Cbs_Branch, Cbs_Bktyaut_.AGE == Cbs_Branch.BranchCode, "left")
    .join(Cbs_Evuti, Cbs_Bktyaut_.UTI == Cbs_Evuti.CUTI, "left")
    #.join(CbsUsers, CbsUsers.UserID = Cbs_Bktyaut_.UserCode)
    .select(
        Cbs_Bktyaut_.TYP.alias("ODLimitTypeCode"), 
        Cbs_Bktyaut_.AGE.alias("AgenceCode"),
        bknom_filtered_df.LIB1.alias("Agence"),
        Cbs_Branch.Batch_ID.alias("BranchID"),
        Cbs_Bktyaut_.CPRO.alias("ProductCode"),
        Cbs_Bktyaut_.DOU.alias("TypeCreatedOn"),
        Cbs_Bktyaut_.DMO.alias("TypeUpdatedOn"),
        Cbs_Bktyaut_.LIBE.alias("Description"),
        Cbs_Bktyaut_.OPE.alias("OperCode"),
        #CbsUsers.UTI.alias("UserCode"),
        Cbs_Evuti.LIB.alias("UserName"),
        #CbsUsers.UserID.alias("UserID"),
        Cbs_Bktyaut_.BatchID.alias("Batch_ID"),
        Cbs_Bktyaut_.BatchDATE.alias("Batch_Date"),
        Cbs_Bktyaut_.BatchDATE.alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        Cbs_Bktyaut_.SystemCode.alias("System_Code"),
        lit("P_Dly_Cbs_Slv").alias("Workflow_Name")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df

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

result_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsODLimitTypeDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#result_df.write.format("delta").mode("overwrite").saveAsTable("GoldLakehouse.CbsODLimitTypeDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

