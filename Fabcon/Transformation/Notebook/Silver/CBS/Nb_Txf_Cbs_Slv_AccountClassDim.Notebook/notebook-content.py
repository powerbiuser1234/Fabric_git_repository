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
bkchap_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkchap")
trial_bal_df = spark.read.format("delta").table("GoldLakehouse.TrialBalance")
#Gold Tables
bknom_df = spark.read.format("delta").table("GoldLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Prepairing Chap_Hier

# CELL ********************

chap_hier_df = trial_bal_df.select(
    trim(col("Combined_Rule")).alias("C_RULE"),
    trim(col("D/C")).alias("D_C"),
    trim(col("Chapter_ID")).alias("Chapter_ID"),
    trim(col("Name of the Chapter")).alias("Chapter_Name"),
    trim(col("Heading 1")).alias("Heading1"),
    trim(col("Heading 2")).alias("Heading2"),
    trim(col("Heading 3")).alias("Heading3")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. AccountClassDim Silver ETL

# MARKDOWN ********************

# #### 3.1 Creating Resultant Dataframe

# CELL ********************

# 1. Filter BKNOM for CTAB = '032'
bknom_filtered_df = bknom_df.filter(col("CTAB") == "032")

# 2. Prepare CHAP_HIER credit and debit mappings
chap_credit_df = chap_hier_df.filter(col("D_C") == "Credit").select(
    col("chapter_id"), col("C_RUle").alias("CreditName")
)

chap_debit_df = chap_hier_df.filter(col("D_C") == "Debit").select(
    col("chapter_id"), col("C_RULE").alias("DebitName")
)

# 3. Trim Bkchap columns
bkchap_df_trimmed = bkchap_df.withColumn("CHA", trim(col("CHA"))) \
    .withColumn("CCLI", trim(col("CCLI"))) \
    .withColumn("CRP", trim(col("CRP"))) \
    .withColumn("LIB", trim(col("LIB"))) \
    .withColumn("SENS", trim(col("SENS"))) \
    .withColumn("TYPC", trim(col("TYPC")))


# 3. Join tables using chaining
final_df = (
    bkchap_df_trimmed.alias("bk")
    .join(bknom_filtered_df.alias("bn"), col("bk.TYPC") == col("bn.CACC"), "left")
    .join(chap_credit_df.alias("cc"), col("bk.CHA") == col("cc.chapter_id"), "left")
    .join(chap_debit_df.alias("cd"), col("bk.CHA") == col("cd.chapter_id"), "left")
    .select(
        col("bk.CHA").alias("AccClassCode"),
        col("bk.CCLI").alias("IsClientAccount"),

        when(col("bk.CCLI") == "O", "OUI")
        .when(col("bk.CCLI") == "N", "Non")
        .when(col("bk.CCLI") == "U", "Indifférent")
        .otherwise("Valeur Inconnue").alias("ClientAccountDesc"),

        col("bk.CRP").alias("StdMatchingCode"),

        when(col("bk.CRP") == "0", "Pas d'émargement")
        .when(col("bk.CRP") == "1", "Un débit pour un crédit")
        .when(col("bk.CRP") == "2", "Un débit pour plusieurs crédits")
        .when(col("bk.CRP") == "3", "Un crédit pour plusieurs débits")
        .when(col("bk.CRP") == "9", "Inter-branch matching")
        .alias("StdMatchingDesc"),

        col("bk.DOU").alias("CreationDate"),
        col("bk.LIB").alias("AccountClass"),
        col("bk.SENS").alias("AccSens"),

        when(col("bk.SENS") == "U", "Indifférent")
        .when(col("bk.SENS") == "D", "Débit")
        .when(col("bk.SENS") == "C", "Crédit")
        .alias("AccSensDesc"),

        col("bk.TYPC").alias("DefLinkAccTypeCode"),
        col("bn.LIB1").alias("DefLinkAccType"),
        col("cc.C_RUle").alias("CreditName"),
        col("cd.C_RULE").alias("DebitName"),
        col("bk.BATCHID").alias("BatchID"),
        col("bk.BATCHDATE").alias("BatchDate"),
        to_date(col("bk.BATCHDATE")).alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        col("bk.SYSTEMCODE").alias("SystemCode"),
        lit(None).cast("binary").alias("RowHash"),
        lit("P_Dly_Cbs_Slv_Gld").alias("WorkflowName")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3.2 Adding Values to RowHash Column

# CELL ********************

#creating rowhash

final_df = final_df.withColumn("RowHash", sha2(concat_ws("", "AccClassCode", "IsClientAccount", "ClientAccountDesc", "StdMatchingCode", "StdMatchingDesc", "CreationDate", "AccountClass", "AccSens", "AccSensDesc", "DefLinkAccTypeCode", "DefLinkAccType", "CreditName", "DebitName",\
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

# MARKDOWN ********************

# ### 4. Writing Data into "**SilverLakehouse.AccountClassDim**"

# CELL ********************

final_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsAccountClassDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
