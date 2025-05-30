# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5953e930-bc05-403f-8cad-ba3d204e63d3",
# META       "default_lakehouse_name": "SilverLakehouse",
# META       "default_lakehouse_workspace_id": "4f2aaff8-44cb-476d-805b-1c95e486af08",
# META       "known_lakehouses": [
# META         {
# META           "id": "b554114f-f6f1-42fa-b182-35c9b03d7bfd"
# META         },
# META         {
# META           "id": "7ba102aa-777a-4ad5-8f68-ce62922c90b3"
# META         },
# META         {
# META           "id": "f0cb0e22-356b-4bf1-81ce-5167c0a5a785"
# META         },
# META         {
# META           "id": "5953e930-bc05-403f-8cad-ba3d204e63d3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### **1. Libraries**

# CELL ********************

from pyspark.sql.functions import *
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **2. Reading Tables**

# CELL ********************

#Reading from Bronze Lakehouse
cl_bkcompens_av = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AV_V")
cl_bkcompens_rv = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_RV_V")

#Reading from Gold Lakehouse 
ClearingDim = spark.read.format("delta").table("GoldLakehouse.ClearingDim")

#Lookup Warehouse tables: Reading from Other workspace Lakehouse
Institution=spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Institution")
ClearingStatus = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingStatus")
OperationCode = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_OperationCode")
AccountSK = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_AccountSK")
Branch = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Branch")
Currency = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Currency")
DateDim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_DateDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3. Clearing Fact ETL**

# MARKDOWN ********************

# ##### **3.1 RV Dataframe**

# CELL ********************

#created a dataframe for BKCOMPENS_RV
df_rv = (cl_bkcompens_rv 
    .join(ClearingDim, (cl_bkcompens_rv.IDRV == ClearingDim.ClearingCode) & (ClearingDim.ClearingType == 'R'), "left") 
    .join(DateDim.alias("DateDim1"), cl_bkcompens_rv.DREG == col("DateDim1.DateValue"), "left") 
    .join(OperationCode, (cl_bkcompens_rv.TOPE == OperationCode.OperationCode) & (OperationCode.AgenceCode == '05100'), "left") 
    .join(Institution, (cl_bkcompens_rv.ETABE == Institution.BankCode) & (cl_bkcompens_rv.GUIBE == Institution.CounterCode) & (Institution.BranchCode == '05100') , "left") 
    .join(AccountSK, (cl_bkcompens_rv.AGED == AccountSK.BranchCode) & (cl_bkcompens_rv.DEVD == AccountSK.CurrencyCode) & (cl_bkcompens_rv.NCPD == AccountSK.AccountNumber), "left") 
    .join(Branch, cl_bkcompens_rv.AGED == Branch.BranchCode, "left") 
    .join(Currency.alias("Currency1"), cl_bkcompens_rv.DEVD == col("Currency1.CurrencyCode"), "left") 
    .join(Currency.alias("Currency2"), cl_bkcompens_rv.DEV == col("Currency2.CurrencyCode"), "left") 
    .join(DateDim.alias("DateDim2"), cl_bkcompens_rv.DCOM == col("DateDim2.DateValue"), "left") 
    .select(
        cl_bkcompens_rv.IDRV.alias("ClearingCode"),
        ClearingDim.ClearingID, 
        lit('R').alias("ClearingType"),
        col("DateDim1.DateKey").alias("SettleDate"),
        col("OperationID").cast("short").alias("OperationID"),
        Institution.BankID.cast("int").alias("InstitutionID"),
        trim(cl_bkcompens_rv.COME).alias("RemitAcc"),
        AccountSK.AccountID.alias("ReceiptAccID"),
        Branch.BranchID.alias("ReceiptBranchID"),
        col("Currency1.CurrencyID").cast("short").alias("RecipientCurrID"),
        col("Currency2.CurrencyID").cast("short").alias("OperationCurrID"),
        cl_bkcompens_rv.MON.cast("decimal(19,4)").alias("Amount"), 
        cl_bkcompens_rv.REF.alias("TranRef"), 
        col("DateDim2.DateKey").alias("ClearingDate"),
        cl_bkcompens_rv.BatchID,
        cl_bkcompens_rv.BatchDate,
        to_date(cl_bkcompens_rv.BatchDate).alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_rv.SystemCode,
        lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_rv.WorkflowName
    ))
display(df_rv)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cl_bkcompens_rv.count()
df_rv.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **3.2 AV Dataframe**

# CELL ********************

#Created a dataframe for BKCOMPENS_AV
df_av = (cl_bkcompens_av 
    .join(ClearingDim, (cl_bkcompens_av.IDAV == ClearingDim.ClearingCode) & (ClearingDim.ClearingType == 'A'), "left") 
    .join(DateDim.alias("DateDim1"), cl_bkcompens_av.DCOM == col("DateDim1.DateValue"), "left") 
    .join(OperationCode, (cl_bkcompens_av.TOPE == OperationCode.OperationCode) & (OperationCode.AgenceCode == '05100'), "left") 
   .join(Institution, (cl_bkcompens_av.ETABD == Institution.BankCode) & (cl_bkcompens_av.GUIBD == Institution.CounterCode) & (Institution.BranchCode == '05100') , "left") 
    .join(AccountSK, (cl_bkcompens_av.AGEE == AccountSK.BranchCode) & (cl_bkcompens_av.DEVE == AccountSK.CurrencyCode) & (cl_bkcompens_av.NCPE == AccountSK.AccountNumber), "left") 
    .join(Branch, cl_bkcompens_av.AGEE == Branch.BranchCode, "left") 
    .join(Currency.alias("Currency1"), cl_bkcompens_av.DEVE == col("Currency1.CurrencyCode"), "left") 
    .join(Currency.alias("Currency2"), cl_bkcompens_av.DEV == col("Currency2.CurrencyCode"), "left") 
    .join(DateDim.alias("DateDim2"), cl_bkcompens_av.DCOM == col("DateDim2.DateValue"), "left") 
    .select(
        cl_bkcompens_av.IDAV.alias("ClearingCode"),
        ClearingDim.ClearingID, 
        lit('A').alias("ClearingType"),
        col("DateDim1.DateKey").alias("SettleDate"), 
        col("OperationID").cast("short").alias("OperationID"),
        Institution.BankID.cast("int").alias("InstitutionID"),
        trim(cl_bkcompens_av.COMD).alias("RemitAcc"),
        AccountSK.AccountID.alias("ReceiptAccID"),
        Branch.BranchID.alias("ReceiptBranchID"),
         col("Currency1.CurrencyID").cast("short").alias("RecipientCurrID"),
        col("Currency2.CurrencyID").cast("short").alias("OperationCurrID"),
        cl_bkcompens_av.MON.cast("decimal(19,4)").alias("Amount"),
        cl_bkcompens_av.REF.alias("TranRef"),
        col("DateDim2.DateKey").alias("ClearingDate"),
        cl_bkcompens_av.BatchID,
        cl_bkcompens_av.BatchDate,
        to_date(cl_bkcompens_av.BatchDate).alias("CreatedOn"),
       lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_av.SystemCode,
        lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_av.WorkflowName
    ))
display(df_av)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cl_bkcompens_av.count()
df_av.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3.3 Joining staging_av and staging_rv**

# CELL ********************

# Union both DataFrames to create a consolidated dataset
total_staging = df_rv.union(df_av)
display(total_staging)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

total_staging.count()

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

# ### 4. Writing Data into "**SilverLakehouse.ClearingFact**"

# CELL ********************


total_staging.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.ClearingFact")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
