# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b554114f-f6f1-42fa-b182-35c9b03d7bfd",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "4f2aaff8-44cb-476d-805b-1c95e486af08",
# META       "known_lakehouses": [
# META         {
# META           "id": "5953e930-bc05-403f-8cad-ba3d204e63d3"
# META         },
# META         {
# META           "id": "7ba102aa-777a-4ad5-8f68-ce62922c90b3"
# META         },
# META         {
# META           "id": "b554114f-f6f1-42fa-b182-35c9b03d7bfd"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### 1. Libraries

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Reading Tables

# CELL ********************

#Bronze Tables
cl_bkcompens_rv_eve = spark.read.table("BronzeLakehouse.CL_BKCOMPENS_RV_EVE_V")
cl_bkcompens_rv = spark.read.table("BronzeLakehouse.CL_BKCOMPENS_RV_V")
cl_bkcompens_av = spark.read.table("BronzeLakehouse.CL_BKCOMPENS_AV_V")
cl_bkcompens_av_eve = spark.read.table("BronzeLakehouse.CL_BKCOMPENS_AV_EVE_V")


#Gold Tables
clearingdim = spark.read.format("delta").table("GoldLakehouse.ClearingDim")

#Lookup Warehouse tables: Reading from Other workspace Lakehouse
institution = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Institution")
clearingstatus = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingStatus")
datedim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_DateDim")
operationcode = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_OperationCode")
accountsk = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_AccountSK")
currency = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Currency")
trannature = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_TranNature")
branch = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Branch")

# #Creating Views
# datedim.createOrReplaceTempView("datedim_v")
# operationcode.createOrReplaceTempView("operationcode_v")
# institution.createOrReplaceTempView("institution_v")
# accountsk.createOrReplaceTempView("accountsk_v")
# currency.createOrReplaceTempView("currency_v")
# trannature.createOrReplaceTempView("trannature_v")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. ClearingEventFact Silver ETL

# MARKDOWN ********************

# #### 3.1 Creating RV Dataframe

# CELL ********************

result_df_rv = (
    cl_bkcompens_rv_eve
    .join(clearingdim, (cl_bkcompens_rv_eve.IDRV == clearingdim.ClearingCode) & (clearingdim.ClearingType == 'R'), "left")
    .join(cl_bkcompens_rv, cl_bkcompens_rv_eve.IDRV == cl_bkcompens_rv.IDRV, "left")
    .join(datedim.alias("DD"), cl_bkcompens_rv.DREG == col("DD.DateValue"), "left")
    .join(operationcode.alias("OPC1"), (cl_bkcompens_rv.TOPE == col("OPC1.OperationCode")) & (col("OPC1.AgenceCode") == "05100"), "left")
    .join(institution, (cl_bkcompens_rv.ETABE == institution.BankCode) & (cl_bkcompens_rv.GUIBE == institution.CounterCode) & (institution.BranchCode == '05100') , "left") 
    .join(accountsk, (cl_bkcompens_rv.AGED == accountsk.BranchCode) & (cl_bkcompens_rv.DEVD == accountsk.CurrencyCode) & (cl_bkcompens_rv.NCPD == accountsk.AccountNumber), "left")
    .join(branch, cl_bkcompens_rv.AGED == branch.BranchCode, "left")
    .join(currency.alias("CURD"), cl_bkcompens_rv.DEVD == col("CURD.CurrencyCode"), "left")
    .join(currency.alias("CUR"), cl_bkcompens_rv.DEV == col("CUR.CurrencyCode"), "left")
    .join(datedim.alias("DD1"), cl_bkcompens_rv.DCOM == col("DD1.DateValue"), "left")
    .join(trannature, cl_bkcompens_rv_eve.NAT == trannature.NatureCode, "left")
    .join(operationcode.alias("OPC2"), (cl_bkcompens_rv_eve.OPE == col("OPC2.OperationCode")) & (col("OPC2.AgenceCode") == "05100"), "left")
    .join(datedim.alias("DD2"), cl_bkcompens_rv_eve.DSAI == col("DD2.DateValue"), "left")
    .select(
        cl_bkcompens_rv_eve.IDRV.alias("ClearingCode"),
        cl_bkcompens_rv_eve.NORD.cast("smallint").alias("Sequence"),
        clearingdim.ClearingID,
        lit("R").alias("ClearingType"),
        col("DD.DateKey").alias("SettleDate"),
        col("OPC1.OperationID").cast("smallint").alias("TOperationID"),
        institution.BankID.cast("int").alias("InstitutionID"),
        trim(cl_bkcompens_rv.COME).alias("RemitAcc"),
        accountsk.AccountID.alias("ReceiptAccID"),     
        branch.BranchID.alias("ReceiptBranchID"),
        col("CURD.CurrencyID").cast("short").alias("RecipientCurrID"),
        col("CUR.CurrencyID").cast("short").alias("OperationCurrID"),
        cl_bkcompens_rv.MON.cast("decimal(19,4)").alias("Amount"),
        cl_bkcompens_rv.REF.alias("TranRef"),
        col("DD1.DateKey").alias("ClearingDate"),
        cl_bkcompens_rv_eve.EVE.alias("EventID"),
        trannature.TranNatureID,
        col("OPC2.OperationID").alias("OperationID"),
        col("DD2.DateKey").alias("EventDate"),
        cl_bkcompens_rv_eve.BatchID,
        cl_bkcompens_rv_eve.BatchDate,
        cl_bkcompens_rv_eve.BatchDate.alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_rv_eve.SystemCode,
        lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_rv_eve.WorkflowName
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(cl_bkcompens_rv_eve.count())
print(result_df_rv.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result_df_rv)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3.2 Creating AV Dataframe

# CELL ********************

result_df_av = (
    cl_bkcompens_av_eve
    .join(clearingdim, (cl_bkcompens_av_eve.IDAV == clearingdim.ClearingCode) & (clearingdim.ClearingType == 'A'), "left")
    .join(cl_bkcompens_av, cl_bkcompens_av_eve.IDAV == cl_bkcompens_av.IDAV, "left")
    .join(datedim.alias("DD"), cl_bkcompens_av.DCOM == col("DD.DateValue"), "left")
    .join(operationcode.alias("OPC1"), (cl_bkcompens_av.TOPE == col("OPC1.OperationCode")) & (col("OPC1.AgenceCode") == '05100'), "left")
    .join(institution,(cl_bkcompens_av.ETABD == institution.BankCode) & (cl_bkcompens_av.GUIBD == institution.CounterCode) & (institution.BranchCode == '05100'), "left")
    .join(accountsk, (cl_bkcompens_av.AGEE == accountsk.BranchCode) & (cl_bkcompens_av.DEVE == accountsk.CurrencyCode) & (cl_bkcompens_av.NCPE == accountsk.AccountNumber), "left")
    .join(branch, cl_bkcompens_av.AGEE == branch.BranchCode, "left")
    .join(currency.alias("CURD"), cl_bkcompens_av.DEVE == col("CURD.CurrencyCode"), "left")
    .join(currency.alias("CUR"), cl_bkcompens_av.DEVE == col("CUR.CurrencyCode"), "left")
    .join(datedim.alias("DD1"), cl_bkcompens_av.DCOM == col("DD1.DateValue"), "left")
    .join(trannature, cl_bkcompens_av_eve.NAT == trannature.NatureCode, "left")
    .join(operationcode.alias("OPC2"), (cl_bkcompens_av_eve.OPE == col("OPC2.OperationCode")) & (col("OPC2.AgenceCode") == '05100'), "left")
    .join(datedim.alias("DD2"), cl_bkcompens_av_eve.DSAI == col("DD2.DateValue"), "left")
    .select(
        cl_bkcompens_av_eve.IDAV.alias("ClearingCode"),
        cl_bkcompens_av_eve.NORD.cast("smallint").alias("Sequence"),
        clearingdim.ClearingID,
        lit("A").alias("ClearingType"),
        col("DD.DateKey").alias("SettleDate"),
        col("OPC1.OperationID").cast("smallint").alias("TOperationID"),
        institution.BankID.cast("int").alias("InstitutionID"),
        trim(cl_bkcompens_av.COMD).alias("RemitAcc"),
        accountsk.AccountID.alias("ReceiptAccID"),
        branch.BranchID.alias("ReceiptBranchID"),
        col("CURD.CurrencyID").cast("short").alias("RecipientCurrID"),
        col("CUR.CurrencyID").cast("short").alias("OperationCurrID"),
        cl_bkcompens_av.MON.cast("decimal(19,4)").alias("Amount"),
        cl_bkcompens_av.REF.alias("TranRef"),
        col("DD1.DateKey").alias("ClearingDate"),
        cl_bkcompens_av_eve.EVE.alias("EventID"),
        trannature.TranNatureID,
        col("OPC2.OperationID").alias("OperationID"),
        col("DD2.DateKey").alias("EventDate"),
        cl_bkcompens_av_eve.BatchID,
        cl_bkcompens_av_eve.BatchDate,
        cl_bkcompens_av_eve.BatchDate.alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_av_eve.SystemCode,
        lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_av_eve.WorkflowName
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(cl_bkcompens_av_eve.count())
print(result_df_av.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result_df_av)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3.4 Joining RV with AV Dataframe

# CELL ********************

df_union_rv_av = result_df_rv.union(result_df_av)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_union_rv_av)

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

# ### 4. Writing Data into **"SilverLakehouse.ClearingEventFact"**

# CELL ********************

df_union_rv_av.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.ClearingEventFact") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM SilverLakehouse.ClearingEventFact where ClearingType = 'R'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM SilverLakehouse.ClearingEventFact where ClearingType = 'A'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
