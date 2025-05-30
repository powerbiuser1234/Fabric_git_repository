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
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
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
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Bronze Tables
Cbs_Bkadcli = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkadcli")
# Perform the equivalent transformations in PySpark
Cbs_Bkadcli_ = Cbs_Bkadcli.select(
    trim(col("TYP")).alias("TYP"),
    trim(col("CLI")).alias("CLI"),
    trim(col("LANG")).alias("LANG"),
    trim(col("FMT")).alias("FMT"),
    trim(col("ADR1")).alias("ADR1"),
    trim(col("ADR2")).alias("ADR2"),
    trim(col("ADR3")).alias("ADR3"),
    trim(col("VILLE")).alias("VILLE"),
    trim(col("CPOS")).alias("CPOS"),
    trim(col("BDIS")).alias("BDIS"),
    trim(col("BPOS")).alias("BPOS"),
    trim(col("SPOS")).alias("SPOS"),
    trim(col("LSPOS")).alias("LSPOS"),
    trim(col("CPAY")).alias("CPAY"),
    trim(col("GUI")).alias("GUI"),
    trim(col("CAS")).alias("CAS"),
    trim(col("SER")).alias("SER"),
    trim(col("TRS")).alias("TRS"),
    trim(col("EMAIL")).alias("EMAIL"),
    'DPRVD',
    'DPRVF',
    'NRCE',
    'DRCE',
    trim(col("ATRF")).alias("ATRF"),
    trim(col("DEP")).alias("DEP"),
    trim(col("REG")).alias("REG"),
    'BatchID',
    'BatchDATE',
    'SystemCode'
)


#Gold Tables
CbsClientSK = spark.read.format("delta").table("GoldLakehouse.CbsClientSK")

CbsBknom = spark.read.format("delta").table("GoldLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ADDR = Cbs_Bkadcli_.alias("ADDR")
C = CbsClientSK.alias("C")
B1 = CbsBknom.alias("B1")
B2 = CbsBknom.alias("B2")
B3 = CbsBknom.alias("B3")

result_df = (
    ADDR
    .join(C, ADDR.CLI == C.SourceID, "left")
    .join(B1, (ADDR.CPAY == B1.CACC) & (B1.CTAB == '040'), "left")
    .join(B2, (ADDR.FMT == B2.CACC) & (B2.CTAB == '181'), "left")
    .join(B3, (ADDR.LANG == B3.CACC) & (B3.CTAB == '190'), "left")
    .select(
        ADDR.ADR1.alias("AddressLine1"),
        ADDR.ADR2.alias("AddressLine2"),
        ADDR.ADR3.alias("AddressLine3"),
        ADDR.ATRF.alias("ToTransfer"),
        ADDR.BDIS.alias("PostOfficeName"),
        ADDR.BPOS.alias("POBox"),
        ADDR.CAS.alias("BoxNumber"),
        ADDR.CLI.alias("Client"),
        C.ClientID.alias("ClientID"),
        ADDR.CPAY.alias("CountryCode"),
        B1.LIB1.alias("Country"),
        ADDR.CPOS.alias("PostCode"),
        ADDR.DPRVD.alias("TempAddStartDate"),
        ADDR.DPRVF.alias("TempAddEndDate"),
        ADDR.DRCE.alias("ErrMailFirstDate"),
        ADDR.EMAIL.alias("Email"),
        ADDR.FMT.alias("AddFormatCode"),
        B2.LIB1.alias("AddressFormat"),
        ADDR.GUI.alias("Counter"),
        ADDR.LANG.alias("DisplayLangCode"),
        B3.LIB1.alias("DisplayLanguage"),
        ADDR.LSPOS.alias("PostalSectorDesc"),
        ADDR.NRCE.alias("ErrMailCount"),
        ADDR.SER.alias("Service"),
        ADDR.SPOS.alias("PostalSector"),
        ADDR.TRS.alias("Transport"),
        ADDR.TYP.alias("AddressType"),
        ADDR.VILLE.alias("Town"),
        ADDR.BatchID,
        ADDR.BatchDATE,
        ADDR.BatchDATE.cast("date").alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        ADDR.SystemCode,
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

# ## **Generating Row Hash**

# CELL ********************

#creating rowhash

result_df_hash = result_df.withColumn("RowHash", sha2(concat_ws("", "AddressLine1", "AddressLine2", "AddressLine3", "ToTransfer", "PostOfficeName", "POBox", "BoxNumber", "Client", "ClientID", "CountryCode", "Country", "PostCode", "TempAddStartDate", "TempAddEndDate", "ErrMailFirstDate", "Email", "AddFormatCode", "AddressFormat", "Counter", "DisplayLangCode", "DisplayLanguage", "PostalSectorDesc", "ErrMailCount", "Service", "PostalSector", "Transport", "AddressType", "Town"), \
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

result_df_hash.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsCliAddressDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
