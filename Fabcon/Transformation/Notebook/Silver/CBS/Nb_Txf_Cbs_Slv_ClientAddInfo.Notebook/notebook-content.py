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
# META         },
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
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
Cbs_Bkprfcli = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkicli")

max_batch_date = Cbs_Bkprfcli.select(max("BatchDATE").alias("max_date")).collect()[0]["max_date"]

filtered_cli = Cbs_Bkprfcli.filter(
    (to_date("BatchDATE") == lit(max_batch_date)) & 
    (col("CLI") != "")
)

#Gold Tables
ClientSK = spark.read.format("delta").table("GoldLakehouse.CbsClientSK")

Bknom = spark.read.format("delta").table("GoldLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CLI = filtered_cli.alias("CLI")
CLISK = ClientSK.alias("CLISK")
B1 = Bknom.alias("B1")
B2 = Bknom.alias("B2")

joined_df = (
    CLI
    .join(CLISK, CLI.CLI == CLISK.SourceID, "left")
    .join(B1, (CLI.IDEN == B1.CACC) & (B1.CTAB == '171'), "left")
    .join(B2, (CLI.VALA == B2.CACC) & (B2.CTAB == B1.MNT6), "left")
)

result_df = joined_df.filter(CLISK.SourceID.isNotNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = result_df.select(
    CLISK.ClientID.alias("ClientID"),
    trim(col("CLI.CLI")).alias("ClientCode"),
    trim(col("CLI.IDEN")).alias("DataIdentifierCode"),
    trim(col("B1.LIB1")).alias("DataIdentifier"),
    trim(col("CLI.VALA")).alias("DataValue"),
    trim(col("B2.LIB1")).alias("DataValueDesc"),
    CLI.DMO.alias("LastUpdDate"),
    CLI.BatchID,
    CLI.BatchDATE,
    CLI.BatchDATE.cast("date").alias("CreatedOn"),
    lit(None).cast("date").alias("UpdatedOn"),
    CLI.SystemCode,
    lit(None).cast("binary").alias("RowHash"),
    lit("P_Dly_Cbs_Slv").alias("WorkflowName")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Generating Row Hash

# CELL ********************

#creating rowhash

result_df_hash = final_df.withColumn("RowHash", sha2(concat_ws("", "ClientID", "ClientCode", "DataIdentifierCode", "DataIdentifier", "DataValue", "DataValueDesc", "LastUpdDate"), \
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

result_df_hash.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsClientAddInfo")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
