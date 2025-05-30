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
dop_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkdopi")
#Gold Tables
sk_df = spark.read.format("delta").table("GoldLakehouse.TransferSK")
event_status_df = spark.read.format("delta").table("GoldLakehouse.EventStatus")
module_df = spark.read.format("delta").table("GoldLakehouse.Module")
tran_nature_df = spark.read.format("delta").table("GoldLakehouse.TranNature")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transfers_df = dop_df.alias("D") \
    .join(sk_df.alias("SK"),
          (trim(col("D.AGE")) == col("SK.BranchCode")) &
          (trim(col("D.TYP")) == col("SK.TransferType")) &
          (trim(col("D.EVE")) == col("SK.EventNumber")) &
          (trim(col("D.NDOS")) == col("SK.FileNumber")),
          "left") \
    .join(event_status_df.alias("E"),
          trim(col("D.ETA")) == col("E.StatusCode"),
          "left") \
    .join(module_df.alias("M"),
          trim(col("D.MODU")) == col("M.ModuleCode"),
          "left") \
    .join(tran_nature_df.alias("TN"),
          (trim(col("D.NAT")) == col("TN.NatureCode")) &
          (col("TN.NatureSource") == lit("CBS")),
          "left") \
    .select(
        col("SK.TransferID"),
        trim(col("D.AGE")).alias("BranchCode"),
        col("D.DENV").alias("SendingDate"),
        trim(col("D.DEV")).alias("CurrencyCode"),
        trim(col("D.ETA")).alias("FileStatus"),
        col("E.StatusDesc").alias("FileStatusDesc"),
        trim(col("D.ETAC")).alias("CorrInst"),
        trim(col("D.EVE")).alias("EventNumber"),
        trim(col("D.FCORR")).alias("ChargeMethod"),
        when(trim(col("D.FCORR")) == "BEN",
             "tous les frais sont à la charge du bénéficiaire")
        .when(trim(col("D.FCORR")) == "OUR",
              "tous les frais sont à la charge du donneur d'ordre")
        .when(trim(col("D.FCORR")) == "SHA",
              "les frais du côté de l'émetteur sont à la charge du donneur d'ordre et les frais du côté du receveur sont à la charge du bénéficiaire")
        .otherwise("UNKNOWN").alias("ChargeMethodDesc"),
        trim(col("D.GUIC")).alias("CorrCounter"),
        trim(col("D.MODU")).alias("Module"),
        col("M.ModuleName").alias("ModuleDesc"),
        trim(col("D.NAT")).alias("Nature"),
        col("TN.NatureDesc").alias("NatureDesc"),
        col("D.NBE").alias("InstallNum"),
        trim(col("D.NCHE")).alias("ChequeNumber"),
        trim(col("D.NDOS")).alias("FileNumber"),
        trim(col("D.NOMBF")).alias("BeneName"),
        trim(col("D.NOMDO")).alias("CustName"),
        trim(col("D.OPE")).alias("OperationCode"),
        trim(col("D.PAYSDP")).alias("CountryCode"),
        trim(col("D.REFDO")).alias("CustReference"),
        trim(col("D.TYP")).alias("TransferType"),
        col("D.batchID"),
        col("D.BatchDate"),
        to_date(col("D.BatchDate")).alias("Created_On"),
        lit(None).cast("timestamp").alias("Updated_On"),
        col("D.SystemCode"),
        lit(None).cast("binary").alias("Row_Hash"),
        lit("P_Dly_Cbs_Slv_Gld").alias("WorkflowName")
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3.2 Adding Values to RowHash Column

# CELL ********************

transfers_df = transfers_df.withColumn(
    "RowHash",
    sha2(concat_ws("",
        col("TransferID"),
        col("BranchCode"),
        col("SendingDate"),
        col("CurrencyCode"),
        col("FileStatus"),
        col("FileStatusDesc"),
        col("CorrInst"),
        col("EventNumber"),
        col("ChargeMethod"),
        col("ChargeMethodDesc"),
        col("CorrCounter"),
        col("Module"),
        col("ModuleDesc"),
        col("Nature"),
        col("NatureDesc"),
        col("InstallNum"),
        col("ChequeNumber"),
        col("FileNumber"),
        col("BeneName"),
        col("CustName"),
        col("OperationCode"),
        col("CountryCode"),
        col("CustReference"),
        col("TransferType")
    ), 256)
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

# ### 4. Writing Data into "**SilverLakehouse.Transfers**"

# CELL ********************

transfers_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsTransfers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
