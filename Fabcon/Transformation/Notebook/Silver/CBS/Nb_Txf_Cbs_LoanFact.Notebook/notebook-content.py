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
bkdosprt_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkdosprt")
#Gold Tables
loansk_df = spark.read.format("delta").table("GoldLakehouse.LoanSK")
loantype_df = spark.read.format("delta").table("GoldLakehouse.LoanType")
loanprocstatus_df = spark.read.format("delta").table("GoldLakehouse.LoanProcStatus")
datedim_df = spark.read.format("delta").table("GoldLakehouse.DateDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# trimmed columns from BKDOSPRT
bkdosprt_df = bkdosprt_df.withColumn("ETA", trim(col("ETA"))) \
    .withColumn("CTR", trim(col("CTR"))) \
    .withColumn("AGE", trim(col("AGE"))) \
    .withColumn("EVE", trim(col("EVE"))) \
    .withColumn("TYP", trim(col("TYP"))) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. LoanFact Silver ETL

# MARKDOWN ********************

# #### 3.1 Creating Resultant Dataframe

# CELL ********************

loanfact_df = bkdosprt_df.alias("B") \
    .join(loansk_df.alias("LS"), 
          (col("B.AGE") == col("LS.BranchCode")) & 
          (col("B.EVE") == col("LS.FileNumber")) & 
          (col("B.AVE") == col("LS.AmendmentNum")), 
          "left") \
    .join(loantype_df.alias("LT"), 
          col("B.TYP") == col("LT.TypeCode"), 
          "left") \
    .join(loanprocstatus_df.alias("LPS"), 
          (col("B.ETA") == col("LPS.STATUSCODE")) & 
          (col("B.CTR") == col("LPS.ProcessCode")), 
          "left") \
    .join(datedim_df.alias("DD"), 
          col("B.DIMP") == col("DD.DateValue"), 
          "left") \
    .select(
        col("LS.LoanID"),
        col("LT.LoanTypeID"),
        trim(col("B.ETA")).alias("StatusCode"),
        trim(col("B.CTR")).alias("ProcessCode"),
        col("LPS.LoanStatusID").alias("LoanProcStatusID"),
        lit(None).alias("PrevLoanProcStatusID"),
        col("DD.DateKey").alias("LastChangeDate"),
        col("DD.DateKey").alias("LastUnpaidAmtDT"),
        col("B.CUM_AMO").alias("TotalAmoAmt"),
        col("B.CUM_INT").alias("TotalInterestAmt"),
        col("B.CUM_FRA").alias("TotalCharges"),
        col("B.CUM_INI").alias("TotalIntUnpaidAmt"),
        col("B.CUM_TAX_INT").alias("TotalTaxInt"),
        col("B.CUM_TAX_FRA").alias("TotalTaxCharge"),
        col("B.CUM_TAX_INI").alias("TotalTaxUnpaidAmtInt"),
        col("B.CUM_CO1").alias("TotalComm1"),
        col("B.CUM_CO2").alias("TotalComm2"),
        col("B.CUM_CO3").alias("TotalComm3"),
        col("B.DARR_INI").alias("LastChargeIntCalcDate"),
        col("B.DARR_INT").alias("LastIntCalcDate"),
        col("B.DDBL").alias("LastFundsReleaseDate"),
        col("B.DDEC").alias("LastInstallmentDate"),
        col("B.DIMP").alias("LastUnpaidDate"),
        col("B.BatchID"),
        col("B.BatchDate"),
        col("B.BatchDate").cast("date").alias("Created_On"),
        lit(None).cast("timestamp").alias("Updated_On"),
        col("B.SystemCode"),
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

# Append Row_Hash column to your existing DataFrame
loanfact_df = loanfact_df.withColumn(
    "Row_Hash",
    sha2(concat_ws("",
        col("LoanTypeID"),
        col("StatusCode"),
        col("ProcessCode"),
        col("LoanProcStatusID"),
        col("PrevLoanProcStatusID"),
        col("LastChangeDate"),
        col("LastUnpaidAmtDT"),
        col("TotalAmoAmt"),
        col("TotalInterestAmt"),
        col("TotalCharges"),
        col("TotalIntUnpaidAmt"),
        col("TotalTaxInt"),
        col("TotalTaxCharge"),
        col("TotalTaxUnpaidAmtInt"),
        col("TotalComm1"),
        col("TotalComm2"),
        col("TotalComm3"),
        col("LastChargeIntCalcDate"),
        col("LastIntCalcDate"),
        col("LastFundsReleaseDate"),
        col("LastInstallmentDate"),
        col("LastUnpaidDate")
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

# ### 4. Writing Data into "**SilverLakehouse.LoanFact**"

# CELL ********************

loanfact_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsLoanFact")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
