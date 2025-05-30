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
cms_currency_df = spark.read.format("delta").table("BronzeLakehouse.CMS_Currency")
#Gold Tables
bknom_df = spark.read.format("delta").table("GoldLakehouse.CbsBknom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Currency Silver ETL

# MARKDOWN ********************

# #### 3.1 Creating Resultant Dataframe

# CELL ********************

# Alias dataframes
bknom_df = bknom_df.alias("b")
cms_currency_df = cms_currency_df.alias("c")

# Filter, join, and select required columns with transformations
result_df_currency = (
    bknom_df.filter(col("b.CTAB") == "005")
    .join(
        cms_currency_df,
        col("c.CUR_ALPH_CODE") == col("b.LIB2"),
        "left"
    )
    .select(
        col("b.CACC").alias("CurrencyCode"),
        col("b.LIB2").alias("CurrencyISO"),
        col("c.CUR_CODE").alias("CurrencyCodeCMS"),
        col("b.BatchID").alias("Batch_ID"),
        col("b.BatchDate").alias("Batch_Date"),
        to_date(col("b.BatchDate")).alias("Created_On"),
        lit(None).cast("date").alias("Updated_On"),
        col("b.SystemCode").alias("System_Code"),
        lit(None).cast("binary").alias("Row_Hash"),  # placeholder for now
        lit("P_Dly_Cbs_Slv_Gld").alias("Workflow_Name")
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

# ### 4. Writing Data into "**SilverLakehouse.Currency**"

# CELL ********************

final_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsCurrency")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
