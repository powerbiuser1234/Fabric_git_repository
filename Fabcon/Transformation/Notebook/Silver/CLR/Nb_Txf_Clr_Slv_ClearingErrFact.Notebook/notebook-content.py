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
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         },
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
# META         },
# META         {
# META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
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

#reading tables from bronze Lakehouse
cl_bkcompens_av = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AV_V")
cl_bkcompens_rv = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_RV_V")
cl_bkcompens_error=spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_ERROR_V")

#reading tables from GoldLakehouse
ClearingDim =spark.read.format("delta").table("GoldLakehouse.ClearingDim")


#Lookup Warehouse tables: Reading from Other workspace Lakehouse
Institution = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Institution")
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

# ### **3. ClearingErrFact ETL**

# MARKDOWN ********************

# #### **3.1 Dataframe for R**

# CELL ********************

df_r = (
    cl_bkcompens_error.filter("SENS = 'R'")
    .join(ClearingDim, (cl_bkcompens_error.ID == ClearingDim.ClearingCode) & (ClearingDim.ClearingType == 'R'), "left")
    .join(cl_bkcompens_rv, cl_bkcompens_error.ID == cl_bkcompens_rv.IDRV, "left")
    .join(DateDim.alias("DateDim1"), cl_bkcompens_rv.DREG == col("DateDim1.DateValue"), "left")
    .join(OperationCode, (cl_bkcompens_rv.TOPE == OperationCode.OperationCode) & (OperationCode.AgenceCode == '05100'), "left")
    .join(Institution, (cl_bkcompens_rv.ETABE == Institution.BankCode) & (cl_bkcompens_rv.GUIBE == Institution.CounterCode) & (Institution.BranchCode == '05100') , "left") 
    .join(AccountSK, (cl_bkcompens_rv.AGED == AccountSK.BranchCode) & (cl_bkcompens_rv.DEVD == AccountSK.CurrencyCode) & (cl_bkcompens_rv.NCPD == AccountSK.AccountNumber), "left")
    .join(Branch, cl_bkcompens_rv.AGED == Branch.BranchCode, "left")
    .join(Currency.alias("Currency1"), cl_bkcompens_rv.DEVD == col("Currency1.CurrencyCode"), "left")
    .join(Currency.alias("Currency2"), cl_bkcompens_rv.DEV == col("Currency2.CurrencyCode"), "left")
    .join(DateDim.alias("DateDim2"), cl_bkcompens_rv.DCOM == col("DateDim2.DateValue"), "left")
    .join(DateDim.alias("DateDim3"), cl_bkcompens_error.DCRE == col("DateDim3.DateValue"), "left")
    .filter(cl_bkcompens_rv.IDRV.isNotNull())
    .select(
        cl_bkcompens_error.IDEN.alias("ClearingErrCode"),
        ClearingDim.ClearingID, 
        lit('R').alias("ClearingType"),
        col("DateDim1.DateKey").alias("SettleDate"),
        col("OperationID").cast("short").alias("OperationID"),
        Institution.BankID.cast("int").alias("InstitutionID"),
        trim(cl_bkcompens_rv.COME).alias("RemitAcc"),
        AccountSK.AccountID.alias("ReceiptAccID"),
        Branch.BranchID.cast("int").alias("ReceiptBranchID"),
        col("Currency1.CurrencyID").cast("short").alias("RecipientCurrID"),
        col("Currency2.CurrencyID").cast("short").alias("OperationCurrID"),
        cl_bkcompens_rv.MON.cast("decimal(19,4)").alias("Amount"), 
        cl_bkcompens_rv.REF.alias("TranRef"), 
        col("DateDim2.DateKey").alias("ClearingDate"),
        cl_bkcompens_error.TYPE.alias("ErrorType"),
        cl_bkcompens_error.PROG.alias("Program"),
        cl_bkcompens_error.CERR.alias("ShortErrCode"),
        cl_bkcompens_error.LCERR.alias("LongErrCode"),
        cl_bkcompens_error.MESS.alias("ErrMessage"),
        cl_bkcompens_rv.ETA.alias("ErrorStatus"),
        col("DateDim3.DateKey").alias("ErrorDate"),
        cl_bkcompens_error.HCRE.alias("ErrorTime"),
        cl_bkcompens_error.BatchID,
        cl_bkcompens_error.BatchDate,
        to_date(cl_bkcompens_error.BatchDate).alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_error.SystemCode,
         lit(None).cast("binary").alias("RowHash"),
        lit("P_Dly_Clr_Slv_Gld").alias("WorkflowName")
    )
)
display(df_r)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cl_bkcompens_error.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

non_null_count = df_r.filter(col("ClearingID").isNotNull()).count()

print(f"Total non-null clearingID count: {non_null_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_r.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3.2 Dataframe for E**

# CELL ********************

df_e = (
    cl_bkcompens_error.filter("SENS = 'E'")
    .join(ClearingDim, (cl_bkcompens_error.ID == ClearingDim.ClearingCode) & (ClearingDim.ClearingType == 'A'), "left")
    .join(cl_bkcompens_av, cl_bkcompens_error.ID == cl_bkcompens_av.IDAV, "left")
    .join(DateDim.alias("DateDim1"), cl_bkcompens_av.DCOM == col("DateDim1.DateValue"), "left")
    .join(OperationCode, (cl_bkcompens_av.TOPE == OperationCode.OperationCode) & (OperationCode.AgenceCode == '05100'), "left")
    .join(Institution,(cl_bkcompens_av.ETABD == Institution.BankCode) & (cl_bkcompens_av.GUIBD == Institution.CounterCode) & (Institution.BranchCode == '05100'), "left")
    .join(AccountSK, (cl_bkcompens_av.AGEE == AccountSK.BranchCode) & (cl_bkcompens_av.DEVE == AccountSK.CurrencyCode) & (cl_bkcompens_av.NCPE == AccountSK.AccountNumber), "left")
    .join(Branch, cl_bkcompens_av.AGEE == Branch.BranchCode, "left")
    .join(Currency.alias("Currency1"), cl_bkcompens_av.DEVE == col("Currency1.CurrencyCode"), "left")
    .join(Currency.alias("Currency2"), cl_bkcompens_av.DEV == col("Currency2.CurrencyCode"), "left")
    .join(DateDim.alias("DateDim2"), cl_bkcompens_av.DCOM == col("DateDim2.DateValue"), "left")
    .join(DateDim.alias("DateDim3"), cl_bkcompens_error.DCRE == col("DateDim3.DateValue"), "left")
    .filter(cl_bkcompens_av.IDAV.isNotNull())
    .select(
        cl_bkcompens_error.IDEN.alias("ClearingErrCode"),
        ClearingDim.ClearingID, 
        lit('A').alias("ClearingType"),
        col("DateDim1.DateKey").alias("SettleDate"),
        col("OperationID").cast("short").alias("OperationID"),
        Institution.BankID.cast("int").alias("InstitutionID"),
        trim(cl_bkcompens_av.COMD).alias("RemitAcc"),
        AccountSK.AccountID.alias("ReceiptAccID"),
        Branch.BranchID.cast("int").alias("ReceiptBranchID"),
        col("Currency1.CurrencyID").cast("short").alias("RecipientCurrID "), 
        col("Currency2.CurrencyID").cast("short").alias("OperationCurrID"),
        cl_bkcompens_av.MON.cast("decimal(19,4)").alias("Amount"), 
        cl_bkcompens_av.REF.alias("TranRef"), 
        col("DateDim2.DateKey").alias("ClearingDate"),
        cl_bkcompens_error.TYPE.alias("ErrorType"),
        cl_bkcompens_error.PROG.alias("Program"),
        cl_bkcompens_error.CERR.alias("ShortErrCode"),
        cl_bkcompens_error.LCERR.alias("LongErrCode"),
        cl_bkcompens_error.MESS.alias("ErrMessage"),
        cl_bkcompens_av.ETA.alias("ErrorStatus"),
        col("DateDim3.DateKey").alias("ErrorDate"),
        cl_bkcompens_error.HCRE.alias("ErrorTime"),
       cl_bkcompens_error.BatchID,
        cl_bkcompens_error.BatchDate,
        to_date(cl_bkcompens_error.BatchDate).alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_error.SystemCode,
         lit(None).cast("binary").alias("RowHash"),
        lit("P_Dly_Clr_Slv_Gld").alias("WorkflowName")
    )
)
display(df_e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

non_null_count = df_e.filter(col("ClearingID").isNotNull()).count()

print(f"Total non-null ClearingID count: {non_null_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_e.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **3.3 Joining df_r and df_e**


# CELL ********************

# Union both DataFrames to create a consolidated dataset
result_df = df_r.union(df_e)
display(result_df)

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

# ### 4. Writing Data into "**SilverLakehouse.ClearingErrFact**"

# CELL ********************

result_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.ClearingErrFact")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SilverLakehouse.ClearingErrFact LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
