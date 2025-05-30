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
Cbs_Bkprfcli = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkprfcli")
# Perform the equivalent transformations in PySpark
Cbs_Bkprfcli_ = Cbs_Bkprfcli.select(
    trim(col("CLI")).alias("CLI"),
    trim(col("PRF")).alias("PRF"),
    trim(col("EMP")).alias("EMP"),
    'DEMB',
    trim(col("TREV")).alias("TREV"),
    trim(col("ATRF")).alias("ATRF"),
    trim(col("DEMP")).alias("DEMP"),
    'BatchID',
    'BatchDATE',
    'SystemCode'
)

Cbs_Bkempl = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkempl")
# Perform the equivalent transformations in PySpark
Cbs_Bkempl_ = Cbs_Bkempl.select(
    trim(col("EMP")).alias("EMP"),
    trim(col("NOM")).alias("NOM"),
    trim(col("TYP")).alias("TYP"),
    trim(col("SIG")).alias("SIG"),
    trim(col("CLTI")).alias("CLTI"),
    trim(col("ADR1")).alias("ADR1"),
    trim(col("ADR2")).alias("ADR2"),
    trim(col("ADR3")).alias("ADR3"),
    trim(col("CPOS")).alias("CPOS"),
    trim(col("VILLE")).alias("VILLE"),
    trim(col("BDIS")).alias("BDIS"),
    trim(col("CPAY")).alias("CPAY"),
    trim(col("LANG")).alias("LANG"),
    trim(col("CTC")).alias("CTC"),
    trim(col("TEL")).alias("TEL"),
    trim(col("EMAIL")).alias("EMAIL"),
    trim(col("UTI")).alias("UTI"),
    trim(col("ATRF")).alias("ATRF"),
    trim(col("DEP")).alias("DEP"),
    trim(col("REG")).alias("REG"),
    trim(col("NIDN")).alias("NIDN"),
    trim(col("CON")).alias("CON")
)

#Gold Tables
Client_Dim = spark.read.format("delta").table("GoldLakehouse.CbsClientDim")

Bknom = spark.read.format("delta").table("GoldLakehouse.CbsBknom")
# Filter BKNOM table for CTAB = '050'
bknom_filtered_df = Bknom.filter(col("CTAB") == '045')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df = (
    Cbs_Bkprfcli_
    .join(Client_Dim, Cbs_Bkprfcli_.CLI == Client_Dim.ClientBK, "left")
    .join(Cbs_Bkempl_, Cbs_Bkprfcli_.EMP == Cbs_Bkempl_.EMP, "left")
    .join(bknom_filtered_df, Cbs_Bkprfcli_.PRF == bknom_filtered_df.CACC, "left")
    .select(
        Client_Dim.ClientID.alias("ClientID"),
        Cbs_Bkprfcli_.CLI.alias("ClientCode"),
        Cbs_Bkprfcli_.EMP.alias("EmployerCode"),
        Cbs_Bkempl_.NOM.alias("Employer"),
        Cbs_Bkprfcli_.PRF.alias("ProfessionCode"),
        bknom_filtered_df.LIB1.alias("Profession"),
        Cbs_Bkprfcli_.DEMP.alias("Department"),
        Cbs_Bkprfcli_.DEMB.alias("HireDate"),
        Cbs_Bkprfcli_.BatchID,
        Cbs_Bkprfcli_.BatchDATE,
        Cbs_Bkprfcli_.BatchDATE.cast("date").alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        Cbs_Bkprfcli_.SystemCode,
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

result_df_hash = result_df.withColumn("RowHash", sha2(concat_ws("", "ClientID", "ClientCode", "EmployerCode", "Employer", "ProfessionCode", "Profession", "Department", "HireDate"), \
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

result_df_hash.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsClientPrfDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
