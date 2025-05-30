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
# META           "id": "e56ddaf5-2e15-4634-91cc-e2eb818afa61"
# META         },
# META         {
# META           "id": "ac470c1d-7c5d-4111-b733-488950a5aeb6"
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         },
# META         {
# META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
# META         },
# META         {
# META           "id": "23a1ae3b-a1ac-470c-a1d0-f3dcf1363bf3"
# META         }
# META       ]
# META     },
# META     "warehouse": {}
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

# CELL ********************

spark.version


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

#Reading from other workspaces lakehouse
ClearingStatus = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingStatus")
OperationCode = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_OperationCode")
Institution = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/23a1ae3b-a1ac-470c-a1d0-f3dcf1363bf3/Tables/Institution")
AccountSK = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_AccountSK")
Branch = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Branch")
Currency = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Currency")
DateDim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_DateDim")
Institution.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3. Clearing Fact ETL**

# MARKDOWN ********************

# ##### **3.1 Dataframe For RV**

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
        ClearingDim.ClearingID, lit('R').alias("ClearingType"),
        col("DateDim1.DateKey").alias("SettleDate"),
        col("OperationID").cast("short").alias("OperationID"),
        Institution.BankID.alias("InstitutionID"),
        trim(cl_bkcompens_rv.COME).alias("RemitAcc"),
        AccountSK.AccountID.alias("ReceiptAccID"),
        Branch.BranchID.alias("ReceiptBranchID"),
        col("Currency1.CurrencyID").cast("short").alias("ReceipientCurrID"),
        col("Currency2.CurrencyID").cast("short").alias("OperationCurrID"),
        cl_bkcompens_rv.MON.alias("Amount"), 
        cl_bkcompens_rv.REF.alias("TranRef"), 
        col("DateDim2.DateKey").alias("ClearingDate"),
        cl_bkcompens_rv.BatchID.alias("BatchID"),
        cl_bkcompens_rv.BatchDate.alias("BatchDate"),
        to_date(cl_bkcompens_rv.BatchDate).alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_rv.SystemCode.alias("SystemCode"),
        lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_rv.WorkflowName.alias("WorkflowName")
    ))
display(df_rv)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cl_bkcompens_rv.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_rv.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **3.2 Dataframe For AV**

# CELL ********************

#Created a dataframe for BKCOMPENS_AV
df_av = (cl_bkcompens_av 
    .join(ClearingDim, (cl_bkcompens_av.IDAV == ClearingDim.ClearingCode) & (ClearingDim.ClearingType == 'A'), "left") 
    .join(DateDim.alias("DateDim1"), cl_bkcompens_av.DCOM == col("DateDim1.DateValue"), "left") 
    .join(OperationCode, (cl_bkcompens_av.TOPE == OperationCode.OperationCode) & (OperationCode.AgenceCode == '05100'), "left") 
   # .join(Institution, (cl_bkcompens_av.ETABD == Institution.BankCode) & (cl_bkcompens_av.GUIBD == Institution.CounterCode), "left") 
    .join(AccountSK, (cl_bkcompens_av.AGEE == AccountSK.BranchCode) & (cl_bkcompens_av.DEVE == AccountSK.CurrencyCode) & (cl_bkcompens_av.NCPE == AccountSK.AccountNumber), "left") 
    .join(Branch, cl_bkcompens_av.AGEE == Branch.BranchCode, "left") 
    .join(Currency.alias("Currency1"), cl_bkcompens_av.DEVE == col("Currency1.CurrencyCode"), "left") 
    .join(Currency.alias("Currency2"), cl_bkcompens_av.DEV == col("Currency2.CurrencyCode"), "left") 
    .join(DateDim.alias("DateDim2"), cl_bkcompens_av.DCOM == col("DateDim2.DateValue"), "left") 
    .select(
        ClearingDim.ClearingID, lit('A').alias("ClearingType"),
        col("DateDim1.DateKey").alias("SettleDate"), 
        col("OperationID").cast("short").alias("OperationID"),
        lit(None).cast("short").alias("InstitutionID"),
        trim(cl_bkcompens_av.COMD).alias("RemitAcc"),
        AccountSK.AccountID.alias("ReceiptAccID"),
        Branch.BranchID.alias("ReceiptBranchID"),
         col("Currency1.CurrencyID").cast("short").alias("ReceipientCurrID"),
        col("Currency2.CurrencyID").cast("short").alias("OperationCurrID"),
        cl_bkcompens_av.MON.alias("Amount"),
        cl_bkcompens_av.REF.alias("TranRef"),
        col("DateDim2.DateKey").alias("ClearingDate"),
        cl_bkcompens_av.BatchID.alias("BatchID"),
        cl_bkcompens_av.BatchDate.alias("BatchDate"),
        to_date(cl_bkcompens_av.BatchDate).alias("CreatedOn"),
       lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_av.SystemCode.alias("SystemCode"),
        lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_av.WorkflowName.alias("WorkflowName")
    ))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SilverLakehouse.ClearingFact LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3.3 Joining df_av and df_rv**

# CELL ********************

# Union both DataFrames to create a consolidated dataset
total_staging = df_rv.union(df_av)
total_staging.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Writing Into ClearingFact**

# CELL ********************

# silver_lakehouse_path="abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/SilverLakehouse.Lakehouse/Tables/ClearingFact"
# total_staging.write.format("delta").mode("overwrite").save(silver_lakehouse_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
