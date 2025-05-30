# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ac470c1d-7c5d-4111-b733-488950a5aeb6",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "197819da-2cec-4c3c-a96a-0fe62ae2300b",
# META       "known_lakehouses": [
# META         {
# META           "id": "ac470c1d-7c5d-4111-b733-488950a5aeb6"
# META         },
# META         {
# META           "id": "e56ddaf5-2e15-4634-91cc-e2eb818afa61"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CL_BKCOMPENS_RV_EVE = spark.read.table("BronzeLakehouse.BKCOMPENS_RV_EVE")

ClearingDim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingDim")
ClearingDim.createOrReplaceTempView("clearing_dim")

CL_BKCOMPENS_RV = spark.read.table("BronzeLakehouse.BKCOMPENS_RV")

ClearingStatus = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingStatus")
ClearingStatus.createOrReplaceTempView("clearing_status")

DateDim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_DateDim")
DateDim.createOrReplaceTempView("date_dim")

OperationCode = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_OperationCode")
OperationCode.createOrReplaceTempView("operation_code")

Institution = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Institution")
Institution.createOrReplaceTempView("institution")

AccountSK = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_AccountSK")
AccountSK.createOrReplaceTempView("accountSK")

Branch = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Branch")
Branch.createOrReplaceTempView("branch")

Currency = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Currency")
Currency.createOrReplaceTempView("currency")

TranNature = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_TranNature")
TranNature.createOrReplaceTempView("trannature")

CL_BKCOMPENS_AV = spark.read.table("BronzeLakehouse.BKCOMPENS_AV")
CL_BKCOMPENS_AV_EVE = spark.read.table("BronzeLakehouse.BKCOMPENS_AV_EVE")

ClearingEventFactTable = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingEventFact")
ClearingEventFactTable.createOrReplaceTempView("clearingeventfact")

ClearingFact = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingFact")
ClearingFact.createOrReplaceTempView("clearingfact")

ClearingCommFact = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingCommFact")
ClearingCommFact.createOrReplaceTempView("clearingcommfact")

ClearingDim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingDim")
ClearingDim.createOrReplaceTempView("clearingdim")

ClearingErrFact = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingErrFact")
ClearingErrFact.createOrReplaceTempView("clearingerrfact")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CL_BKCOMPENS_RV_EVE.alias("bk")
CL_BKCOMPENS_RV.alias("bkrv")
ClearingStatus.alias("CLS")
DateDim.alias("DD")
OperationCode.alias("OPC1")
Institution.alias("INST")
AccountSK.alias("ASK")
Branch.alias("BR")
OperationCode.alias("OPC2")
CL_BKCOMPENS_AV_EVE.alias("bkav")
CL_BKCOMPENS_AV.alias("bka")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(CL_BKCOMPENS_RV_EVE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ClearingEventFactA = (
    CL_BKCOMPENS_RV_EVE.alias("bk")
    .join(
        ClearingDim.alias("CD"),
        (F.col("bk.IDRV") == F.col("CD.ClearingCode")) & (F.col("CD.ClearingType") == "R"),
        "left",
    )
    .join(
        CL_BKCOMPENS_RV.alias("bkrv"),
        F.col("bk.IDRV") == F.col("bkrv.IDRV"),
        "left",
    )
    .join(
        ClearingStatus.alias("CLS"),
        F.col("bkrv.ETA") == F.col("CLS.StatusCode"),
        "left",
    )
    .join(
        DateDim.alias("DD"),
        F.col("bkrv.DREG") == F.col("DD.DateValue"),
        "left",
    )
    .join(
        OperationCode.alias("OPC1"),
        (F.col("bkrv.TOPE") == F.col("OPC1.OperationCode")) & (F.col("OPC1.AgenceCode") == "05100"),
        "left",
    )
    .join(
        Institution.alias("INST"),
        (F.col("bkrv.ETABE") == F.col("INST.BankCode"))
        & (F.col("bkrv.GUIBE") == F.col("INST.CounterCode")),
        "left",
    )
    .join(
        AccountSK.alias("ASK"),
        (F.col("bkrv.AGED") == F.col("ASK.BranchCode"))
        & (F.col("bkrv.DEVD") == F.col("ASK.CurrencyCode"))
        & (F.col("bkrv.NCPD") == F.col("ASK.AccountNumber")),
        "left",
    )
    .join(
        Branch.alias("BR"),
        F.col("bkrv.AGED") == F.col("BR.BranchCode"),
        "left",
    )
    .join(
        Currency.alias("CURD"),
        F.col("bkrv.DEVD") == F.col("CURD.CurrencyCode"),
        "left",
    )
    .join(
        Currency.alias("CUR"),
        F.col("bkrv.DEV") == F.col("CUR.CurrencyCode"),
        "left",
    )
    .join(
        DateDim.alias("DD1"),
        F.col("bkrv.DCOM") == F.col("DD1.DateValue"),
        "left",
    )
    .join(
        TranNature.alias("TranNature"),
        F.col("bk.NAT") == F.col("TranNature.NatureCode"),
        "left",
    )
    .join(
        OperationCode.alias("OPC2"),
        (F.col("bk.OPE") == F.col("OPC2.OperationCode")) & (F.col("OPC2.AgenceCode") == "05100"),
        "left",
    )
    .join(
        DateDim.alias("DD2"),
        F.col("bk.DSAI") == F.col("DD2.DateValue"),
        "left",
    )
    .select(
        F.col("CD.ClearingID").alias("ClearingID"),
        F.lit("R").alias("ClearingType"),
        F.col("CLS.ClearingStatusID").alias("StatusID"),
        F.col("DD.DateKey").alias("SettleDate"),
        F.col("OPC1.OperationID").alias("TOperationID"),
        F.col("INST.BankID").alias("InstitutionID"),
        F.trim(F.col("bkrv.COME")).alias("RemitAcc"),
        F.col("ASK.AccountID").alias("ReceiptAccID"),
        F.col("BR.BranchID").alias("ReceiptBranchID"),
        F.col("CURD.CurrencyID").alias("ReceipientCurrID"),
        F.col("CUR.CurrencyID").alias("OperationCurrID"),
        F.col("bkrv.MON").alias("Amount"),
        F.col("bkrv.REF").alias("TranRef"),
        F.col("DD1.DateKey").alias("ClearingDate"),
        F.col("bk.EVE").alias("EventID"),
        F.col("TranNature.TranNatureID").alias("TranNatureID"),
        F.col("OPC2.OperationID").alias("OperationID"),
        F.col("DD2.DateKey").alias("EventDate"),
        F.col("bk.NORD").alias("Sequence"),
        F.lit(1).alias("Batch_ID"),
        F.lit("2025-01-22").alias("Batch_Date"),
        F.lit("2025-01-22 10:00:00").alias("Created_On"),
        F.lit(2).alias("System_Code"),
        F.lit("WorkflowName").alias("Workflow_Name"),
    )
)

display(ClearingEventFactA)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ClearingEventFactB = (CL_BKCOMPENS_AV_EVE.alias("bkav")
    .join(ClearingDim.alias("CD"), 
          (F.col("bkav.IDAV") == F.col("CD.ClearingCode")) & 
          (F.col("CD.ClearingType") == 'A'), 
          "left")
    .join(CL_BKCOMPENS_AV.alias("bka"), (F.col("bkav.IDAV") == F.col("bka.IDAV")), "left") 
    .join(ClearingStatus.alias("CLS"), (F.col("bka.ETA") == F.col("CLS.StatusCode")), "left") 
    .join(DateDim.alias("DD"), (F.col("bka.DCOM") == F.col("DD.DateValue")), "left")
    .join(OperationCode.alias("OPC1"), (F.col("bka.TOPE") == F.col("OPC1.OperationCode")) & (F.col("OPC1.AgenceCode") == '05100'), "left")
    .join(Institution.alias("INST"), (F.col("bka.ETABD") == F.col("INST.BankCode")) & (F.col("bka.GUIBD") == F.col("INST.CounterCode")), "left") 
    .join(AccountSK.alias("ASK"), (F.col("bka.AGEE") == F.col("ASK.BranchCode")) & (F.col("bka.DEVE") == F.col("ASK.CurrencyCode")) & (F.col("bka.NCPE") == F.col("ASK.AccountNumber")), "left") 
    .join(Branch.alias("BR"), (F.col("bka.AGEE") == F.col("BR.BranchCode")), "left") 
    .join(Currency.alias("CURD"), (F.col("bka.DEVE") == F.col("CURD.CurrencyCode")), "left")  
    .join(Currency.alias("CUR"), (F.col("bka.DEVE") == F.col("CUR.CurrencyCode")), "left") 
    .join(DateDim.alias("DD1"), (F.col("bka.DCOM") == F.col("DD1.DateValue")), "left") 
    .join(TranNature.alias("CURD"), (F.col("bkav.NAT") == F.col("CURD.NatureCode")), "left") 
    .join(OperationCode.alias("OPC2"), (F.col("bkav.OPE") == F.col("OPC2.OperationCode")) & (F.col("OPC2.AgenceCode") == '05100'), "left")
    .join(DateDim.alias("DD2"), (F.col("bkav.DSAI") == F.col("DD2.DateValue")), "left") 
    .select(
        F.col("CD.ClearingID").alias("ClearingID"),
        F.lit("A").alias("ClearingType"),
        F.col("CLS.ClearingStatusID").alias("StatusID"),
        F.col("DD.DateKey").alias("SettleDate"),
        F.col("OPC1.OperationID").alias("TOperationID"),
        F.col("INST.BankID").alias("InstitutionID"),
        F.trim(F.col("bka.COMD")).alias("RemitAcc"),
        F.col("ASK.AccountID").alias("ReceiptAccID"),
        F.col("BR.BranchID").alias("ReceiptBranchID"),
        F.col("CURD.CurrencyID").alias("ReceipientCurrID"),
        F.col("CUR.CurrencyID").alias("OperationCurrID"),
        F.col("bka.MON").alias("Amount"),
        F.col("bka.REF").alias("TranRef"),
        F.col("DD1.DateKey").alias("ClearingDate"),
        F.col("bkav.EVE").alias("EventID"),
        F.col("CURD.TranNatureID").alias("TranNatureID"),
        F.col("OPC2.OperationID").alias("OperationID"),
        F.col("DD2.DateKey").alias("EventDate"),
        F.col("bkav.NORD").alias("Sequence"),
        F.lit(1).alias("Batch_ID"),
        F.lit("2025-01-22").alias("Batch_Date"),
        F.lit("2025-01-22 10:00:00").alias("Created_On"),
        F.lit(2).alias("System_Code"),
        F.lit("WorkflowName").alias("Workflow_Name"))
)
display(ClearingEventFactB)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Performing the union of both dataframes
ClearingEventFact = ClearingEventFactA.union(ClearingEventFactB)

display(ClearingEventFact)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(ClearingEventFactTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Staging_df = ClearingEventFact.alias("clearing_event_fact") \
    .join(ClearingEventFactTable.alias("staging"), 
          F.col("clearing_event_fact.ClearingID") == F.col("staging.ClearingID"), "left") \
    .filter(F.col("staging.ClearingID").isNull()) \
    .select(
        F.col("staging.ClearingID"),
        F.col("staging.ClearingType"),
        F.col("staging.StatusID"),
        F.col("staging.SettleDate"),
        F.col("staging.TOperationID"),
        F.col("staging.InstitutionID"),
        F.col("staging.RemitAcc"),
        F.col("staging.ReceiptAccID"),
        F.col("staging.ReceiptBranchID"),
        F.col("staging.ReceipientCurrID"),
        F.col("staging.OperationCurrID"),
        F.col("staging.Amount"),
        F.col("staging.TranRef"),
        F.col("staging.ClearingDate"),
        F.col("staging.EventID"),
        F.col("staging.TranNatureID"),
        F.col("staging.OperationID"),
        F.col("staging.EventDate"),
        F.col("staging.Sequence"),
        F.col("staging.Batch_ID"),
        F.col("staging.Batch_Date"),
        F.col("staging.CreatedOn"),
        F.col("staging.SystemCode"),
        F.col("staging.WorkflowName")
        )
display(Staging_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --FIRST part of the query (dataframe A)
# MAGIC SELECT 
# MAGIC 	 CD.ClearingID as ClearingID
# MAGIC 	,'R' as ClearingType
# MAGIC 	,CLS.ClearingStatusID as StatusID
# MAGIC 	,DD.DateKey as SettleDate
# MAGIC 	,OPC1.OperationID as TOperationID
# MAGIC 	,INST.BankID as InstitutionID
# MAGIC 	,TRIM(COMRV.COME) as RemitAcc
# MAGIC 	,ASK.AccountID as ReceiptAccID
# MAGIC 	,BR.BranchID as ReceiptBranchID
# MAGIC 	,CURD.CurrencyID as ReceipientCurrID
# MAGIC 	,CUR.CurrencyID as OperationCurrID
# MAGIC 	,COMRV.MON as Amount
# MAGIC 	,COMRV.REF as TranRef
# MAGIC 	,DD1.DateKey as ClearingDate
# MAGIC 	,COMRVEVE.EVE as EventID
# MAGIC 	,TranNature.TranNatureID as TranNatureID
# MAGIC 	,OPC2.OperationID as OperationID
# MAGIC 	,DD2.DateKey as EventDate
# MAGIC 	,COMRVEVE.NORD as Sequence
# MAGIC 	--,COMRVEVE.Batch_ID  as Batch_ID
# MAGIC     ,'12345' AS Batch_ID
# MAGIC 	--,COMRVEVE.Batch_Date  as Batch_Date
# MAGIC     ,'2025-01-01' AS Batch_Date
# MAGIC 	--,COMRVEVE.CreatedOn  as Created_On
# MAGIC     ,'2025-02-02' AS Created_On
# MAGIC 	--,COMRVEVE.SystemCode  as System_Code
# MAGIC     ,'SYS001' AS System_Code
# MAGIC 	--,COMRVEVE.WorkflowName  as Workflow_Name
# MAGIC     ,'SampleWorkflow' AS Workflow_Name
# MAGIC FROM
# MAGIC BronzeLakehouse.BKCOMPENS_RV_EVE COMRVEVE 
# MAGIC left join clearing_dim CD
# MAGIC 	on COMRVEVE.IDRV=CD.ClearingCode 
# MAGIC 	AND CD.ClearingType='R' 
# MAGIC left join BronzeLakehouse.BKCOMPENS_RV COMRV  
# MAGIC 	ON COMRVEVE.IDRV=COMRV.IDRV
# MAGIC left join clearing_status CLS 
# MAGIC on COMRV.ETA=CLS.StatusCode 
# MAGIC left join date_dim DD 
# MAGIC     on COMRV.DREG=DD.DateValue 
# MAGIC left join operation_code OPC1 
# MAGIC     on COMRV.TOPE=OPC1.OperationCode 
# MAGIC     AND OPC1.AgenceCode='05100' 
# MAGIC left join institution INST 
# MAGIC 	on COMRV.ETABE=INST.BankCode 
# MAGIC     AND COMRV.GUIBE=INST.CounterCode 
# MAGIC left join accountSK ASK 
# MAGIC 	on COMRV.AGED=ASK.BranchCode 
# MAGIC     AND COMRV.DEVD=ASK.CurrencyCode 
# MAGIC 	AND COMRV.NCPD=ASK.AccountNumber  
# MAGIC left join branch BR 
# MAGIC 	on COMRV.AGED=BR.BranchCode 
# MAGIC left join currency CURD 
# MAGIC 	on COMRV.DEVD=CURD.CurrencyCode 
# MAGIC left join currency CUR 
# MAGIC 	on COMRV.DEV=CUR.CurrencyCode 
# MAGIC left join date_dim DD1 
# MAGIC 	on COMRV.DCOM=DD1.DateValue 
# MAGIC left join trannature TranNature 
# MAGIC 	on COMRVEVE.NAT=TranNature.NatureCode 
# MAGIC left join operation_code OPC2 
# MAGIC 	on COMRVEVE.OPE=OPC2.OperationCode 
# MAGIC 	AND OPC2.AgenceCode='05100' 
# MAGIC left join date_dim DD2 
# MAGIC     on COMRVEVE.DSAI=DD2.DateValue

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- SECOND part of the select query (Dataframe B)
# MAGIC SELECT 
# MAGIC 	 CD.ClearingID as ClearingID
# MAGIC 	,'A' as ClearingType
# MAGIC 	,CLS.ClearingStatusID as StatusID
# MAGIC 	,DD.DateKey as SettleDate
# MAGIC 	,OPC1.OperationID as TOperationID
# MAGIC 	,INST.BankID as InstitutionID
# MAGIC 	,TRIM(COMAV.COMD) as RemitAcc
# MAGIC 	,ASK.AccountID as ReceiptAccID
# MAGIC 	,BR.BranchID as ReceiptBranchID
# MAGIC 	,CURD.CurrencyID as ReceipientCurrID
# MAGIC 	,CUR.CurrencyID as OperationCurrID
# MAGIC 	,COMAV.MON as Amount
# MAGIC 	,COMAV.REF as TranRef
# MAGIC 	,DD1.DateKey as ClearingDate
# MAGIC 	,COMAVEVE.EVE as EventID
# MAGIC 	,TranNature.TranNatureID as TranNatureID
# MAGIC 	,OPC2.OperationID as OperationID
# MAGIC 	,DD2.DateKey as EventDate
# MAGIC 	,COMAVEVE.NORD as Sequence
# MAGIC    ,'12345' AS Batch_ID
# MAGIC 	--,COMRVEVE.Batch_Date  as Batch_Date
# MAGIC     ,'2025-01-01' AS Batch_Date
# MAGIC 	--,COMRVEVE.CreatedOn  as Created_On
# MAGIC     ,'2025-02-02' AS Created_On
# MAGIC 	--,COMRVEVE.SystemCode  as System_Code
# MAGIC     ,'SYS001' AS System_Code
# MAGIC 	--,COMRVEVE.WorkflowName  as Workflow_Name
# MAGIC     ,'SampleWorkflow' AS Workflow_Name
# MAGIC FROM
# MAGIC BronzeLakehouse.BKCOMPENS_AV_EVE COMAVEVE 
# MAGIC left join clearing_dim CD
# MAGIC 	on COMAVEVE.IDAV=CD.ClearingCode 
# MAGIC 	AND CD.ClearingType='A' 
# MAGIC left join BronzeLakehouse.BKCOMPENS_AV COMAV  
# MAGIC 	ON COMAVEVE.IDAV=COMAV.IDAV
# MAGIC left join clearing_status CLS 
# MAGIC on COMAV.ETA=CLS.StatusCode 
# MAGIC left join date_dim DD 
# MAGIC     on COMAV.DCOM=DD.DateValue 
# MAGIC left join operation_code OPC1 
# MAGIC     on COMAV.TOPE=OPC1.OperationCode 
# MAGIC     AND OPC1.AgenceCode='05100' 
# MAGIC left join institution INST 
# MAGIC 	on COMAV.ETABD=INST.BankCode 
# MAGIC     AND COMAV.GUIBD=INST.CounterCode 
# MAGIC left join accountSK ASK 
# MAGIC 	on COMAV.AGEE=ASK.BranchCode 
# MAGIC     AND COMAV.DEVE=ASK.CurrencyCode 
# MAGIC 	AND COMAV.NCPE=ASK.AccountNumber  
# MAGIC left join branch BR 
# MAGIC 	on COMAV.AGEE=BR.BranchCode 
# MAGIC left join currency CURD 
# MAGIC 	on COMAV.DEVE=CURD.CurrencyCode 
# MAGIC left join currency CUR 
# MAGIC 	on COMAV.DEV=CUR.CurrencyCode 
# MAGIC left join date_dim DD1 
# MAGIC 	on COMAV.DCOM=DD1.DateValue 
# MAGIC left join trannature TranNature 
# MAGIC 	on COMAVEVE.NAT=TranNature.NatureCode 
# MAGIC left join operation_code OPC2 
# MAGIC 	on COMAVEVE.OPE=OPC2.OperationCode 
# MAGIC 	AND OPC2.AgenceCode='05100' 
# MAGIC left join date_dim DD2 
# MAGIC     on COMAVEVE.DSAI=DD2.DateValue

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --FIRST part of the query (dataframe A)
# MAGIC SELECT 
# MAGIC 	 CD.ClearingID as ClearingID
# MAGIC 	,'R' as ClearingType
# MAGIC 	,CLS.ClearingStatusID as StatusID
# MAGIC 	,DD.DateKey as SettleDate
# MAGIC 	,OPC1.OperationID as TOperationID
# MAGIC 	,INST.BankID as InstitutionID
# MAGIC 	,TRIM(COMRV.COME) as RemitAcc
# MAGIC 	,ASK.AccountID as ReceiptAccID
# MAGIC 	,BR.BranchID as ReceiptBranchID
# MAGIC 	,CURD.CurrencyID as ReceipientCurrID
# MAGIC 	,CUR.CurrencyID as OperationCurrID
# MAGIC 	,COMRV.MON as Amount
# MAGIC 	,COMRV.REF as TranRef
# MAGIC 	,DD1.DateKey as ClearingDate
# MAGIC 	,COMRVEVE.EVE as EventID
# MAGIC 	,TranNature.TranNatureID as TranNatureID
# MAGIC 	,OPC2.OperationID as OperationID
# MAGIC 	,DD2.DateKey as EventDate
# MAGIC 	,COMRVEVE.NORD as Sequence
# MAGIC 	--,COMRVEVE.Batch_ID  as Batch_ID
# MAGIC    ,'12345' AS Batch_ID
# MAGIC 	--,COMRVEVE.Batch_Date  as Batch_Date
# MAGIC     ,'2025-01-01' AS Batch_Date
# MAGIC 	--,COMRVEVE.CreatedOn  as Created_On
# MAGIC     ,'2025-02-02' AS Created_On
# MAGIC 	--,COMRVEVE.SystemCode  as System_Code
# MAGIC     ,'SYS001' AS System_Code
# MAGIC 	--,COMRVEVE.WorkflowName  as Workflow_Name
# MAGIC     ,'SampleWorkflow' AS Workflow_Name
# MAGIC FROM
# MAGIC BronzeLakehouse.BKCOMPENS_RV_EVE COMRVEVE 
# MAGIC left join clearing_dim CD
# MAGIC 	on COMRVEVE.IDRV=CD.ClearingCode 
# MAGIC 	AND CD.ClearingType='R' 
# MAGIC left join BronzeLakehouse.BKCOMPENS_RV COMRV  
# MAGIC 	ON COMRVEVE.IDRV=COMRV.IDRV
# MAGIC left join clearing_status CLS 
# MAGIC on COMRV.ETA=CLS.StatusCode 
# MAGIC left join date_dim DD 
# MAGIC     on COMRV.DREG=DD.DateValue 
# MAGIC left join operation_code OPC1 
# MAGIC     on COMRV.TOPE=OPC1.OperationCode 
# MAGIC     AND OPC1.AgenceCode='05100' 
# MAGIC left join institution INST 
# MAGIC 	on COMRV.ETABE=INST.BankCode 
# MAGIC     AND COMRV.GUIBE=INST.CounterCode 
# MAGIC left join accountSK ASK 
# MAGIC 	on COMRV.AGED=ASK.BranchCode 
# MAGIC     AND COMRV.DEVD=ASK.CurrencyCode 
# MAGIC 	AND COMRV.NCPD=ASK.AccountNumber  
# MAGIC left join branch BR 
# MAGIC 	on COMRV.AGED=BR.BranchCode 
# MAGIC left join currency CURD 
# MAGIC 	on COMRV.DEVD=CURD.CurrencyCode 
# MAGIC left join currency CUR 
# MAGIC 	on COMRV.DEV=CUR.CurrencyCode 
# MAGIC left join date_dim DD1 
# MAGIC 	on COMRV.DCOM=DD1.DateValue 
# MAGIC left join trannature TranNature 
# MAGIC 	on COMRVEVE.NAT=TranNature.NatureCode 
# MAGIC left join operation_code OPC2 
# MAGIC 	on COMRVEVE.OPE=OPC2.OperationCode 
# MAGIC 	AND OPC2.AgenceCode='05100' 
# MAGIC left join date_dim DD2 
# MAGIC     on COMRVEVE.DSAI=DD2.DateValue
# MAGIC 
# MAGIC union
# MAGIC 
# MAGIC --second part (dataframe 2)
# MAGIC SELECT 
# MAGIC 	 CD.ClearingID as ClearingID
# MAGIC 	,'A' as ClearingType
# MAGIC 	,CLS.ClearingStatusID as StatusID
# MAGIC 	,DD.DateKey as SettleDate
# MAGIC 	,OPC1.OperationID as TOperationID
# MAGIC 	,INST.BankID as InstitutionID
# MAGIC 	,TRIM(COMAV.COMD) as RemitAcc
# MAGIC 	,ASK.AccountID as ReceiptAccID
# MAGIC 	,BR.BranchID as ReceiptBranchID
# MAGIC 	,CURD.CurrencyID as ReceipientCurrID
# MAGIC 	,CUR.CurrencyID as OperationCurrID
# MAGIC 	,COMAV.MON as Amount
# MAGIC 	,COMAV.REF as TranRef
# MAGIC 	,DD1.DateKey as ClearingDate
# MAGIC 	,COMAVEVE.EVE as EventID
# MAGIC 	,TranNature.TranNatureID as TranNatureID
# MAGIC 	,OPC2.OperationID as OperationID
# MAGIC 	,DD2.DateKey as EventDate
# MAGIC 	,COMAVEVE.NORD as Sequence
# MAGIC    ,'12345' AS Batch_ID
# MAGIC 	--,COMRVEVE.Batch_Date  as Batch_Date
# MAGIC     ,'2025-01-01' AS Batch_Date
# MAGIC    ,'2025-02-02' AS Created_On
# MAGIC 	--,COMRVEVE.SystemCode  as System_Code
# MAGIC     ,'SYS001' AS System_Code
# MAGIC 	--,COMRVEVE.WorkflowName  as Workflow_Name
# MAGIC     ,'SampleWorkflow' AS Workflow_Name
# MAGIC FROM
# MAGIC BronzeLakehouse.BKCOMPENS_AV_EVE COMAVEVE 
# MAGIC left join clearing_dim CD
# MAGIC 	on COMAVEVE.IDAV=CD.ClearingCode 
# MAGIC 	AND CD.ClearingType='A' 
# MAGIC left join BronzeLakehouse.BKCOMPENS_AV COMAV  
# MAGIC 	ON COMAVEVE.IDAV=COMAV.IDAV
# MAGIC left join clearing_status CLS 
# MAGIC on COMAV.ETA=CLS.StatusCode 
# MAGIC left join date_dim DD 
# MAGIC     on COMAV.DCOM=DD.DateValue 
# MAGIC left join operation_code OPC1 
# MAGIC     on COMAV.TOPE=OPC1.OperationCode 
# MAGIC     AND OPC1.AgenceCode='05100' 
# MAGIC left join institution INST 
# MAGIC 	on COMAV.ETABD=INST.BankCode 
# MAGIC     AND COMAV.GUIBD=INST.CounterCode 
# MAGIC left join accountSK ASK 
# MAGIC 	on COMAV.AGEE=ASK.BranchCode 
# MAGIC     AND COMAV.DEVE=ASK.CurrencyCode 
# MAGIC 	AND COMAV.NCPE=ASK.AccountNumber  
# MAGIC left join branch BR 
# MAGIC 	on COMAV.AGEE=BR.BranchCode 
# MAGIC left join currency CURD 
# MAGIC 	on COMAV.DEVE=CURD.CurrencyCode 
# MAGIC left join currency CUR 
# MAGIC 	on COMAV.DEV=CUR.CurrencyCode 
# MAGIC left join date_dim DD1 
# MAGIC 	on COMAV.DCOM=DD1.DateValue 
# MAGIC left join trannature TranNature 
# MAGIC 	on COMAVEVE.NAT=TranNature.NatureCode 
# MAGIC left join operation_code OPC2 
# MAGIC 	on COMAVEVE.OPE=OPC2.OperationCode 
# MAGIC 	AND OPC2.AgenceCode='05100' 
# MAGIC left join date_dim DD2 
# MAGIC     on COMAVEVE.DSAI=DD2.DateValue


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --FIRST part of the query (dataframe A)
# MAGIC SELECT 
# MAGIC 	 CD.ClearingID as ClearingID
# MAGIC 	,'R' as ClearingType
# MAGIC 	,CLS.ClearingStatusID as StatusID
# MAGIC 	,DD.DateKey as SettleDate
# MAGIC 	,OPC1.OperationID as TOperationID
# MAGIC 	,INST.BankID as InstitutionID
# MAGIC 	,TRIM(COMRV.COME) as RemitAcc
# MAGIC 	,ASK.AccountID as ReceiptAccID
# MAGIC 	,BR.BranchID as ReceiptBranchID
# MAGIC 	,CURD.CurrencyID as ReceipientCurrID
# MAGIC 	,CUR.CurrencyID as OperationCurrID
# MAGIC 	,COMRV.MON as Amount
# MAGIC 	,COMRV.REF as TranRef
# MAGIC 	,DD1.DateKey as ClearingDate
# MAGIC 	,COMRVEVE.EVE as EventID
# MAGIC 	,TranNature.TranNatureID as TranNatureID
# MAGIC 	,OPC2.OperationID as OperationID
# MAGIC 	,DD2.DateKey as EventDate
# MAGIC 	,COMRVEVE.NORD as Sequence
# MAGIC 	--,COMRVEVE.Batch_ID  as Batch_ID
# MAGIC    ,'12345' AS Batch_ID
# MAGIC    	--,COMRVEVE.Batch_Date  as Batch_Date
# MAGIC     ,'2025-01-01' AS Batch_Date
# MAGIC 	--,COMRVEVE.CreatedOn  as Created_On
# MAGIC     ,'2025-02-02' AS Created_On
# MAGIC 	--,COMRVEVE.SystemCode  as System_Code
# MAGIC     ,'SYS001' AS System_Code
# MAGIC 	--,COMRVEVE.WorkflowName  as Workflow_Name
# MAGIC     ,'SampleWorkflow' AS Workflow_Name
# MAGIC FROM
# MAGIC BronzeLakehouse.BKCOMPENS_RV_EVE COMRVEVE 
# MAGIC left join clearing_dim CD
# MAGIC 	on COMRVEVE.IDRV=CD.ClearingCode 
# MAGIC 	AND CD.ClearingType='R' 
# MAGIC left join BronzeLakehouse.BKCOMPENS_RV COMRV  
# MAGIC 	ON COMRVEVE.IDRV=COMRV.IDRV
# MAGIC left join clearing_status CLS 
# MAGIC on COMRV.ETA=CLS.StatusCode 
# MAGIC left join date_dim DD 
# MAGIC     on COMRV.DREG=DD.DateValue 
# MAGIC left join operation_code OPC1 
# MAGIC     on COMRV.TOPE=OPC1.OperationCode 
# MAGIC     AND OPC1.AgenceCode='05100' 
# MAGIC left join institution INST 
# MAGIC 	on COMRV.ETABE=INST.BankCode 
# MAGIC     AND COMRV.GUIBE=INST.CounterCode 
# MAGIC left join accountSK ASK 
# MAGIC 	on COMRV.AGED=ASK.BranchCode 
# MAGIC     AND COMRV.DEVD=ASK.CurrencyCode 
# MAGIC 	AND COMRV.NCPD=ASK.AccountNumber  
# MAGIC left join branch BR 
# MAGIC 	on COMRV.AGED=BR.BranchCode 
# MAGIC left join currency CURD 
# MAGIC 	on COMRV.DEVD=CURD.CurrencyCode 
# MAGIC left join currency CUR 
# MAGIC 	on COMRV.DEV=CUR.CurrencyCode 
# MAGIC left join date_dim DD1 
# MAGIC 	on COMRV.DCOM=DD1.DateValue 
# MAGIC left join trannature TranNature 
# MAGIC 	on COMRVEVE.NAT=TranNature.NatureCode 
# MAGIC left join operation_code OPC2 
# MAGIC 	on COMRVEVE.OPE=OPC2.OperationCode 
# MAGIC 	AND OPC2.AgenceCode='05100' 
# MAGIC left join date_dim DD2 
# MAGIC     on COMRVEVE.DSAI=DD2.DateValue
# MAGIC 
# MAGIC union
# MAGIC 
# MAGIC --second part (dataframe 2)
# MAGIC SELECT 
# MAGIC 	 CD.ClearingID as ClearingID
# MAGIC 	,'A' as ClearingType
# MAGIC 	,CLS.ClearingStatusID as StatusID
# MAGIC 	,DD.DateKey as SettleDate
# MAGIC 	,OPC1.OperationID as TOperationID
# MAGIC 	,INST.BankID as InstitutionID
# MAGIC 	,TRIM(COMAV.COMD) as RemitAcc
# MAGIC 	,ASK.AccountID as ReceiptAccID
# MAGIC 	,BR.BranchID as ReceiptBranchID
# MAGIC 	,CURD.CurrencyID as ReceipientCurrID
# MAGIC 	,CUR.CurrencyID as OperationCurrID
# MAGIC 	,COMAV.MON as Amount
# MAGIC 	,COMAV.REF as TranRef
# MAGIC 	,DD1.DateKey as ClearingDate
# MAGIC 	,COMAVEVE.EVE as EventID
# MAGIC 	,TranNature.TranNatureID as TranNatureID
# MAGIC 	,OPC2.OperationID as OperationID
# MAGIC 	,DD2.DateKey as EventDate
# MAGIC 	,COMAVEVE.NORD as Sequence
# MAGIC    ,'12345' AS Batch_ID
# MAGIC 	--,COMRVEVE.Batch_Date  as Batch_Date
# MAGIC     ,'2025-01-01' AS Batch_Date
# MAGIC    ,'2025-02-02' AS Created_On
# MAGIC 	--,COMRVEVE.SystemCode  as System_Code
# MAGIC     ,'SYS001' AS System_Code
# MAGIC 	--,COMRVEVE.WorkflowName  as Workflow_Name
# MAGIC     ,'SampleWorkflow' AS Workflow_Name
# MAGIC FROM
# MAGIC BronzeLakehouse.BKCOMPENS_AV_EVE COMAVEVE 
# MAGIC left join clearing_dim CD
# MAGIC 	on COMAVEVE.IDAV=CD.ClearingCode 
# MAGIC 	AND CD.ClearingType='A' 
# MAGIC left join BronzeLakehouse.BKCOMPENS_AV COMAV  
# MAGIC 	ON COMAVEVE.IDAV=COMAV.IDAV
# MAGIC left join clearing_status CLS 
# MAGIC on COMAV.ETA=CLS.StatusCode 
# MAGIC left join date_dim DD 
# MAGIC     on COMAV.DCOM=DD.DateValue 
# MAGIC left join operation_code OPC1 
# MAGIC     on COMAV.TOPE=OPC1.OperationCode 
# MAGIC     AND OPC1.AgenceCode='05100' 
# MAGIC left join institution INST 
# MAGIC 	on COMAV.ETABD=INST.BankCode 
# MAGIC     AND COMAV.GUIBD=INST.CounterCode 
# MAGIC left join accountSK ASK 
# MAGIC 	on COMAV.AGEE=ASK.BranchCode 
# MAGIC     AND COMAV.DEVE=ASK.CurrencyCode 
# MAGIC 	AND COMAV.NCPE=ASK.AccountNumber  
# MAGIC left join branch BR 
# MAGIC 	on COMAV.AGEE=BR.BranchCode 
# MAGIC left join currency CURD 
# MAGIC 	on COMAV.DEVE=CURD.CurrencyCode 
# MAGIC left join currency CUR 
# MAGIC 	on COMAV.DEV=CUR.CurrencyCode 
# MAGIC left join date_dim DD1 
# MAGIC 	on COMAV.DCOM=DD1.DateValue 
# MAGIC left join trannature TranNature 
# MAGIC 	on COMAVEVE.NAT=TranNature.NatureCode 
# MAGIC left join operation_code OPC2 
# MAGIC 	on COMAVEVE.OPE=OPC2.OperationCode 
# MAGIC 	AND OPC2.AgenceCode='05100' 
# MAGIC left join date_dim DD2 
# MAGIC     on COMAVEVE.DSAI=DD2.DateValue
# MAGIC 
# MAGIC SELECT 
# MAGIC 	 staging.ClearingID
# MAGIC 	,staging.ClearingType
# MAGIC 	,staging.StatusiD
# MAGIC 	,staging.SettleDate
# MAGIC 	,staging.TOperationID
# MAGIC 	,staging.InstitutionID
# MAGIC 	,staging.RemitAcc
# MAGIC 	,staging.ReceiptAccID
# MAGIC 	,staging.ReceiptBranchID
# MAGIC 	,staging.ReceipientCurrID
# MAGIC 	,staging.OperationCurrID
# MAGIC 	,staging.Amount
# MAGIC 	,staging.TranRef
# MAGIC 	,staging.ClearingDate
# MAGIC 	,staging.EventID
# MAGIC 	,staging.TranNatureID
# MAGIC 	,staging.OperationID
# MAGIC 	,staging.EventDate
# MAGIC 	,staging.Sequence
# MAGIC 	,staging.Batch_ID
# MAGIC 	,staging.Batch_Date
# MAGIC 	,staging.CreatedOn
# MAGIC 	---,staging.UpdatedOn
# MAGIC 	,staging.SystemCode
# MAGIC 	---,staging.RowHash
# MAGIC 	,staging.WorkflowName
# MAGIC 
# MAGIC FROM clearingeventfact staging
# MAGIC LEFT JOIN clearingeventfact DWH ON staging.ClearingID = DWH.ClearingID
# MAGIC WHERE DWH.ClearingID IS NULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.
df = spark.sql("SELECT * FROM Lakehouse.dbo_ClearingDim LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.
df = spark.sql("SELECT * FROM clearing_dim")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.
df = spark.sql("SELECT * FROM Lakehouse.dbo_ClearingStatus LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW DATABASES").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE BronzeLakehouse.Table_List
# MAGIC (
# MAGIC     Schema_Name Varchar(100),
# MAGIC     Table_Name Varchar(100)
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO BronzeLakehouse.Table_List
# MAGIC VALUES
# MAGIC     ('dbo', 'BKCOMPENS_AV'),
# MAGIC     ('dbo', 'BKCOMPENS_AV_EVEC'),
# MAGIC     ('dbo', 'BKCOMPENS_RV_EVE'),
# MAGIC     ('dbo', 'BKCOMPENS_RV_CHQ'),
# MAGIC     ('dbo', 'BKCOMPENS_RV'),
# MAGIC     ('dbo', 'BKCOMPENS_RV_ETA'),
# MAGIC     ('dbo', 'BKCOMPENS_RF_ETA');


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM BronzeLakehouse.Table_List;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --SELECT * FROM Lakehouse.dbo_ClearingEventFact
# MAGIC 
# MAGIC CREATE TABLE BronzeLakehouse.dbo_ClearingEventFact AS 
# MAGIC SELECT * FROM clearingeventfact


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM Lakehouse.dbo_ClearingEventFact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --SELECT * FROM Lakehouse.dbo_ClearingEventFact
# MAGIC 
# MAGIC CREATE TABLE BronzeLakehouse.dbo_ClearingFact AS 
# MAGIC SELECT * FROM clearingfact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM Lakehouse.dbo_ClearingFact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE BronzeLakehouse.dbo_ClearingCommFact AS 
# MAGIC SELECT * FROM clearingcommfact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE BronzeLakehouse.dbo_ClearingDim AS 
# MAGIC SELECT * FROM clearingdim

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM Lakehouse.dbo_ClearingDim

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE BronzeLakehouse.dbo_ClearingErrFact AS 
# MAGIC SELECT * FROM clearingerrfact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM Lakehouse.dbo_ClearingErrFact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bkcompens_av_schema = StructType(
    [
        StructField("IDAV", StringType(), False),
        StructField("IDFIC", StringType(), True),
        StructField("TOPE", StringType(), True),
        StructField("ETABD", StringType(), True),
        StructField("GUIBD", StringType(), True),
        StructField("COMD", StringType(), True),
        StructField("CLEB", StringType(), True),
        StructField("NOMD", StringType(), True),
        StructField("AGEE", StringType(), False),
        StructField("NCPE", StringType(), False),
        StructField("SUFE", StringType(), False),
        StructField("DEVE", StringType(), False),
        StructField("DEV", StringType(), False),
        StructField("MON", DecimalType(19, 4), False),
        StructField("REF", StringType(), True),
        StructField("DCOM", TimestampType(), True),
        StructField("ID_ORIG_SENS", StringType(), True),
        StructField("ID_ORIG", StringType(), True),
        StructField("ETA", StringType(), False),
        StructField("SYST", StringType(), True),
        StructField("NATOPE", StringType(), True),
        StructField("Batch_ID", IntegerType(), True),
        StructField("Batch_Date", TimestampType(), True),
        StructField("SYSTEMCODE", StringType(), True),
        StructField("WORKFLOWNAME", StringType(), True),
        StructField("createdon", TimestampType(), True)
    ]
)

bkcompens_av = spark.createDataFrame([], bkcompens_av_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bkcompens_av_evec_schema = StructType(
    [
        StructField("IDAV", StringType(), False),
        StructField("NORD", IntegerType(), False),
        StructField("NAT", StringType(), False),
        StructField("IDEN", StringType(), False),
        StructField("TYPC", StringType(), True),
        StructField("DEVR", StringType(), True),
        StructField("MCOMR", DecimalType(19, 4), True),
        StructField("TXREF", DecimalType(18, 10), True),
        StructField("MCOMC", DecimalType(19, 4), True),
        StructField("MCOMN", DecimalType(19, 4), True),
        StructField("MCOMT", DecimalType(19, 4), True),
        StructField("TAX", DecimalType(19, 4), True),
        StructField("TCOM", DecimalType(15, 7), True),
        StructField("Batch_ID", IntegerType(), True),
        StructField("Batch_Date", TimestampType(), True),
        StructField("SYSTEMCODE", StringType(), True),
        StructField("WORKFLOWNAME", StringType(), True),
        StructField("MCOMC", DecimalType(19, 4), True),
        StructField("Createdon", TimestampType(), True),
        StructField("RowHash", StringType(), True)
    ]
)

bkcompens_av_evec = spark.createDataFrame([], bkcompens_av_evec_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bkcompens_av_eve_schema = StructType(
    [
        StructField("IDAV", StringType(), False),
        StructField("NORD", DecimalType(5, 0), False),
        StructField("AGE", StringType(), False),
        StructField("OPE", StringType(), False),
        StructField("EVE", StringType(), False),
        StructField("NAT", StringType(), False),
        StructField("DESA1", StringType(), True),
        StructField("DESA2", StringType(), True),
        StructField("DESA3", StringType(), True),
        StructField("DESA4", StringType(), True),
        StructField("DESA5", StringType(), True),
        StructField("DESC1", StringType(), True),
        StructField("DESC2", StringType(), True),
        StructField("DESC3", StringType(), True),
        StructField("DESC4", StringType(), True),
        StructField("DESC5", StringType(), True),
        StructField("DESC5", StringType(), True),
        StructField("FORC", StringType(), True),
        StructField("DSAI", TimestampType(), True),
        StructField("HSAI", StringType(), True),
        StructField("BATCH_ID", IntegerType(), True),
        StructField("BATCH_DATE", TimestampType(), True),
        StructField("SYSTEMCODE", StringType(), True),
        StructField("WORKFLOWNAME", StringType(), True),
        StructField("createdon", TimestampType(), True)
    ]
)

bkcompens_av_eve = spark.createDataFrame([], bkcompens_av_eve_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bkcompens_rv_eve_schema = StructType(
    [
        StructField("IDRV", StringType(), False),
        StructField("NORD", DecimalType(5, 0), False),
        StructField("AGE", StringType(), False),
        StructField("OPE", StringType(), False),
        StructField("EVE", StringType(), False),
        StructField("NAT", StringType(), False),
        StructField("DESA1", StringType(), True),
        StructField("DESA2", StringType(), True),
        StructField("DESA3", StringType(), True),
        StructField("DESA4", StringType(), True),
        StructField("DESA5", StringType(), True),
        StructField("DESC1", StringType(), True),
        StructField("DESC2", StringType(), True),
        StructField("DESC3", StringType(), True),
        StructField("DESC4", StringType(), True),
        StructField("DESC5", StringType(), True),
        StructField("DSAI", TimestampType(), True),
        StructField("HSAI", StringType(), True),
        StructField("FORC", StringType(), True),
        StructField("BATCH_ID", IntegerType(), True),
        StructField("BATCH_DATE", TimestampType(), True),
        StructField("SYSTEMCODE", StringType(), True),
        StructField("WORKFLOWNAME", StringType(), True),
        StructField("createdon", TimestampType(), True)
    ]
)
bkcompens_rv_eve = spark.createDataFrame([], bkcompens_rv_eve_schema)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bkcompens_rv_chq_schema = StructType(
    [
        StructField("IDRVCHQ", DecimalType(10, 0), False),
        StructField("IDRV", StringType(), False),
        StructField("NCHQ", StringType(), True),
        StructField("CTRLVISU", StringType(), True),
        StructField("DESA1", StringType(), True),
        StructField("DESA2", StringType(), True),
        StructField("SERIAL", StringType(), True),
        StructField("DECH", TimestampType(), True),
        StructField("CM7", StringType(), True),
        StructField("Batch_ID", StringType(), True),
        StructField("Batch_Date", TimestampType(), True),
        StructField("CTRLVISU", StringType(), True),

    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
tables = spark.sql("SHOW TABLES")
tables.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
