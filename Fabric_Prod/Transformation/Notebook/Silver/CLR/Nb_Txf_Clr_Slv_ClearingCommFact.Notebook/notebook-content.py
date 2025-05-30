# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9c04cb09-56e5-49d6-a2b0-5e2e578f481a",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "197819da-2cec-4c3c-a96a-0fe62ae2300b",
# META       "known_lakehouses": [
# META         {
# META           "id": "2e2c9b55-5b79-4ca1-880c-bf0b64fd0495"
# META         },
# META         {
# META           "id": "0bba3a6e-afd6-42a2-a721-dff287a67789"
# META         },
# META         {
# META           "id": "9c04cb09-56e5-49d6-a2b0-5e2e578f481a"
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Reading tables

# CELL ********************

#Bronze Tables
cl_bkcompens_av_evec = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AV_EVEC_V")
cl_bkcompens_av = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AV_V")

#Gold Tables
clearingdim = spark.read.format("delta").table("GoldLakehouse.ClearingDim")

#Lookup Warehouse tables: Reading from Other workspace Lakehouse
institution=spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Institution")
Branch=spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Branch")
datedim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_DateDim")
operationcode = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_OperationCode")
accountsk = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_AccountSK")
currency = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Currency")
trannature = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_TranNature")

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

# ### 3. ClearingCommFact Silver ETL

# MARKDOWN ********************

# #### 3.1 Creating Dataframe

# CELL ********************

result_df_av_evec = (
    cl_bkcompens_av_evec
    .join(clearingdim, (cl_bkcompens_av_evec.IDAV == clearingdim.ClearingCode) & (clearingdim.ClearingType == lit('A')), "left")
    .join(cl_bkcompens_av, cl_bkcompens_av_evec.IDAV == cl_bkcompens_av.IDAV, "left")
    .join(operationcode, (cl_bkcompens_av.TOPE == operationcode.OperationCode) & (operationcode.AgenceCode == lit('05100')), "left")
    .join(institution,(cl_bkcompens_av.ETABD == institution.BankCode) & (cl_bkcompens_av.GUIBD == institution.CounterCode) & (institution.BranchCode == '05100'), "left")
    .join(accountsk, (cl_bkcompens_av.AGEE == accountsk.BranchCode) & (cl_bkcompens_av.DEVE == accountsk.CurrencyCode) & (cl_bkcompens_av.NCPE == accountsk.AccountNumber), "left")
    .join(Branch, cl_bkcompens_av.AGEE == Branch.BranchCode, "left")
    .join(currency.alias("Curr1"), cl_bkcompens_av.DEVE == col("Curr1.CurrencyCode"), "left")
    .join(currency.alias("Curr2"), cl_bkcompens_av.DEV == col("Curr2.CurrencyCode"), "left")
    .join(datedim, cl_bkcompens_av.DCOM == datedim.DateValue, "left")
    .join(trannature, cl_bkcompens_av_evec.NAT == trannature.NatureCode, "left")
    .select(
        cl_bkcompens_av_evec.IDAV.alias("ClearingCode"),
        cl_bkcompens_av_evec.NORD.cast("smallint").alias("Sequence"),
        cl_bkcompens_av_evec.NAT.alias("ChargeType"),
        clearingdim.ClearingID.alias("ClearingID"),
        lit("A").alias("ClearingType"),
        operationcode.OperationID.cast("smallint").alias("OperationID"),
        institution.BankID.cast("int").alias("InstitutionID"),
        cl_bkcompens_av.COMD.alias("RemitAcc"),
        accountsk.AccountID.alias("ReceiptAccID"),
        Branch.BranchID.alias("ReceiptBranchID"),
        col("Curr1.CurrencyID").cast("smallint").alias("ReceipientCurrID"),
        col("Curr2.CurrencyID").cast("smallint").alias("OperationCurrID"),
        cl_bkcompens_av.MON.cast("decimal(19,4)").alias("Amount"),
        cl_bkcompens_av.REF.alias("TranRef"),
        datedim.DateKey.alias("ClearingDate"),
        trannature.TranNatureID.alias("TranNatureID"),
        cl_bkcompens_av_evec.IDEN.alias("ChargeIden"),
        cl_bkcompens_av_evec.TYPC.alias("CommType"),
        cl_bkcompens_av_evec.TAX.alias("IsTaxable"),
        cl_bkcompens_av_evec.TCOM.cast("decimal(15,8)").alias("TaxRate"),
        cl_bkcompens_av_evec.MCOMT.alias("AmtTranCurr"),
        cl_bkcompens_av_evec.MCOMN.alias("AmtLocalCurr"),
        cl_bkcompens_av_evec.MCOMC.alias("AmtAccCurr"),
        cl_bkcompens_av_evec.TXREF.cast("decimal(15,8)").alias("ExchRateBaseCurr"),
        cl_bkcompens_av_evec.MCOMR.alias("AmtBaseCurr"),
        cl_bkcompens_av_evec.BatchID,
        cl_bkcompens_av_evec.BatchDate,
        cl_bkcompens_av_evec.BatchDate.alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_av_evec.SystemCode,
        lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_av_evec.WorkflowName
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(cl_bkcompens_av_evec.count())
print(result_df_av_evec.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result_df_av_evec)

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

# ### 4. Writing Data into "**SilverLakehouse.ClearingCommFact**"

# CELL ********************

result_df_av_evec.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.ClearingCommFact")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from BronzeLakehouse.CL_BKCOMPENS_AV_EVEC_V

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
