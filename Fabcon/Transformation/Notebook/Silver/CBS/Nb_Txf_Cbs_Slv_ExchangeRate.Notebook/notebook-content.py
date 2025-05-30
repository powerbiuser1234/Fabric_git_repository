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
bktau_df = spark.read.format("delta").table("BronzeLakehouse.CBS_Bktau")
#Gold Tables
bknom_df = spark.read.format("delta").table("GoldLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. USD Table

# CELL ********************

bktau_df = (
    bktau_df
    .withColumn("AGE", trim(col("AGE")))
    .withColumn("DEV", trim(col("DEV")))
    .alias("a")
)

USDTable = (
    bktau_df
    .filter(col("DEV") == "840")
    .select(
        concat(col("AGE"), col("DCO")).alias("Pkey"),
        col("TAC"),
        col("TVE")
    )
    .alias("usdRate")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. ExchangeRate Silver ETL

# MARKDOWN ********************

# #### 4.1 Creating Resultant Dataframe

# CELL ********************

#Prepare BKNOM tables with aliases and filters
bknom_branch_df = (
    bknom_df
    .filter(col("CTAB") == "001")
    .select("CACC", "LIB1")
    .alias("b1")
)

bknom_currency_df = (
    bknom_df
    .filter(col("CTAB") == "005")
    .select("CACC", "LIB2")
    .alias("b2")
)

exchange_rate_df = (
    bktau_df
    .join(USDTable, concat(col("a.AGE"), col("a.DCO")) == USDTable["Pkey"], "left")
    .join(bknom_branch_df, col("a.AGE") == col("b1.CACC"), "left")
    .join(bknom_currency_df, col("a.DEV") == col("b2.CACC"), "left")
    .select(
        col("a.AGE").alias("BranchCode"),
        col("b1.LIB1").alias("Branch"),
        col("a.DEV").alias("CurrencyCode"),
        col("b2.LIB2").alias("CurrISOCode"),
        col("a.DCO").alias("ExchangeRateDate"),
        col("a.TAC").alias("PurchaseRate"),
        col("a.TVE").alias("SellingRate"),
        col("a.TIND").alias("IndicativeRateUSD"),
        spark_round(
            when(col("a.TAC") != 0, (1.0 / col("usdRate.TAC")) * col("a.TAC")).otherwise(0), 10
        ).alias("PurchaseRateUSD"),
        spark_round(
            when(col("a.TVE") > 0, (1.0 / col("usdRate.TVE")) * col("a.TVE")).otherwise(0), 10
        ).alias("SellingRateUSD"),
        col("a.BATCHID").alias("Batch_ID"),
        col("a.BATCHDATE").alias("Batch_Date"),
        to_date(col("a.BATCHDATE")).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        col("a.SYSTEMCODE").alias("System_Code"),
        lit(None).cast("binary").alias("ROWHASH"),
        lit("P_Dly_Cbs_Slv_Gld").alias("Workflow_Name")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4.2 Adding Values to RowHash Column

# CELL ********************

exchange_rate_df = exchange_rate_df.withColumn(
    "ROWHASH",
    sha2(
        concat_ws("",
            col("BranchCode"),
            col("Branch"),
            col("CurrencyCode"),
            col("CurrISOCode"),
            col("ExchangeRateDate"),
            col("PurchaseRate"),
            col("SellingRate"),
            col("IndicativeRateUSD"),
            col("PurchaseRateUSD"),
            col("SellingRateUSD")
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

# ### 4. Writing Data into "**SilverLakehouse.ExchangeRate**"

# CELL ********************

final_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsExchangeRate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
