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
# META           "id": "e56ddaf5-2e15-4634-91cc-e2eb818afa61"
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         },
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### **1. Libraries**

# CELL ********************

# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.
df = spark.sql("SELECT * FROM BronzeLakehouse.TEMP_BKCOMPENS_AF LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM BronzeLakehouse.TEMP_BKCOMPENS_AF LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) from BronzeLakehouse.TEMP_BKCOMPENS_RV_CHQ


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lakehouse name
lakehouse = "BronzeLakehouse"  # Replace with your actual schema/database if needed

# List of table names
tables = [
    "TEMP_BKCOMPENS_AV",
    "TEMP_BKCOMPENS_AV_EVEC",
    "TEMP_BKCOMPENS_AV_EVE",
    "TEMP_BKCOMPENS_RV_EVE",
    "TEMP_BKCOMPENS_RV_CHQ",
    "TEMP_BKCOMPENS_RV",
    "TEMP_BKCOMPENS_RV_ETA",
    "TEMP_BKCOMPENS_RF_ETA",
    "TEMP_BKCOMPENS_RF",
    "TEMP_BKCOMPENS_ERROR",
    "TEMP_BKCOMPENS_AV_CHQ",
    "TEMP_BKCOMPENS_AF",
    "TEMP_BKCOMPENS_AV_ETA",
    "TEMP_BKCOMPENS_AF_ETA",
    "TEMP_BKCOMPENS_RV_TRF",
    "TEMP_BKCOMPENS_AV_TRF"
]

# Collect table counts
for table in tables:
    full_name = f"{lakehouse}.{table}"
    try:
        count = spark.sql(f"SELECT COUNT(*) as row_count FROM {full_name}").collect()[0]["row_count"]
        print(f"{table}: {count} rows")
    except Exception as e:
        print(f"Error reading {table}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%%sql
describe BronzeLakehouse.CL_BKCOMPENS_AV_EVE

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
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
# cl_bkcompens_av = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AV_V")
# cl_bkcompens_rv = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_RV_V")
# cl_bkcompens_error=spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_ERROR_V")
cl_bkcompens_av = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV")
cl_bkcompens_rv = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV")
cl_bkcompens_error=spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_ERROR")

#reading tables from Lakehouse
ClearingDim = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/ClearingDim")

#reading tables from different workspaces lakehouse
ClearingStatus = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingStatus")
OperationCode = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_OperationCode")
Institution = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Institution")
AccountSK = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_AccountSK")
Branch = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Branch")
Currency = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Currency")
DateDim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_DateDim")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null_count = ClearingDim.filter(col("ClearingID").isNull()).count()
print(f"Number of null values in ClearingID: {null_count}")

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

null_count = df_r.filter(col("ClearingID").isNull()).count()
not_null_count = df_r.filter(col("ClearingID").isNotNull()).count()

print(f"NULL ClearingID count: {null_count}")
print(f"NOT NULL ClearingID count: {not_null_count}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_r = (
    cl_bkcompens_error.filter("SENS = 'R'")
    .join(ClearingDim, (cl_bkcompens_error.ID == ClearingDim.ClearingCode) & (ClearingDim.ClearingType == 'R'), "left")
    .join(cl_bkcompens_rv, cl_bkcompens_error.ID == cl_bkcompens_rv.IDRV, "left")
    .join(DateDim.alias("DateDim1"), cl_bkcompens_rv.DREG == col("DateDim1.DateValue"), "left")
    .join(OperationCode, (cl_bkcompens_rv.TOPE == OperationCode.OperationCode) & (OperationCode.AgenceCode == '05100'), "left")
    #.join(Institution, (cl_bkcompens_rv.ETABE == Institution.BankCode) & (cl_bkcompens_rv.GUIBE == Institution.CounterCode), "left")
    .join(AccountSK, (cl_bkcompens_rv.AGED == AccountSK.BranchCode) & (cl_bkcompens_rv.DEVD == AccountSK.CurrencyCode) & (cl_bkcompens_rv.NCPD == AccountSK.AccountNumber), "left")
    .join(Branch, cl_bkcompens_rv.AGED == Branch.BranchCode, "left")
    .join(Currency.alias("Currency1"), cl_bkcompens_rv.DEVD == col("Currency1.CurrencyCode"), "left")
    .join(Currency.alias("Currency2"), cl_bkcompens_rv.DEV == col("Currency2.CurrencyCode"), "left")
    .join(DateDim.alias("DateDim2"), cl_bkcompens_rv.DCOM == col("DateDim2.DateValue"), "left")
    .join(DateDim.alias("DateDim3"), cl_bkcompens_error.DCRE == col("DateDim3.DateValue"), "left")
    .select(
        ClearingDim.ClearingID, lit('R').alias("ClearingType"),
        col("DateDim1.DateKey").alias("SettleDate"),
        OperationCode.OperationID,
       lit(None).alias("InstitutionID"),
        trim(cl_bkcompens_rv.COME).alias("RemitAcc"),
        AccountSK.AccountID.alias("ReceiptAccID"),
        Branch.BranchID.alias("ReceiptBranchID"),
        col("Currency1.CurrencyID").alias("ReceipientCurrID"), 
        col("Currency2.CurrencyID").alias("OperationCurrID"),
        cl_bkcompens_rv.MON.alias("Amount"), 
        cl_bkcompens_rv.REF.alias("TranRef"), 
        col("DateDim2.DateKey").alias("ClearingDate"),
        cl_bkcompens_error.IDEN,
        cl_bkcompens_error.TYPE,
        cl_bkcompens_error.PROG,
        cl_bkcompens_error.CERR,
        cl_bkcompens_error.LCERR,
        cl_bkcompens_error.MESS,
        cl_bkcompens_rv.ETA,
        col("DateDim3.DateKey").alias("DATEKEY3"),
        cl_bkcompens_error.HCRE,
        cl_bkcompens_error.BATCHID,
        cl_bkcompens_error.BATCHDATE,
        to_date(cl_bkcompens_error.BATCHDATE).alias("CREATEDON"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_error.SYSTEMCODE,
         lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_error.WORKFLOWNAME
    )
)
display(df_r)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_r_filtered = df_r.filter(df_r["ReceiptAccID"].isNotNull())
display(df_r_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1 = cl_bkcompens_error.filter("SENS = 'E'")
print("After filtering cl_bkcompens_error:", df_1.count())

df_2 = df_1.join(ClearingDim, (df_1.ID == ClearingDim.ClearingCode) & (ClearingDim.ClearingType == 'A'), "left")
print("After joining ClearingDim:", df_2.count())

df_3 = df_2.join(cl_bkcompens_av, df_2.ID == cl_bkcompens_av.IDAV, "left")
print("After joining cl_bkcompens_av:", df_3.count())

df_4 = df_3.join(DateDim.alias("DateDim1"), df_3.DCOM == col("DateDim1.DateValue"), "left")
print("After joining DateDim1:", df_4.count())

df_5 = df_4.join(OperationCode, (df_4.TOPE == OperationCode.OperationCode) & (OperationCode.AgenceCode == '05100'), "left")
print("After joining OperationCode:", df_5.count())

df_6 = df_5.join(AccountSK, (df_5.AGEE == AccountSK.BranchCode) & (df_5.DEVE == AccountSK.CurrencyCode) & (df_5.NCPE == AccountSK.AccountNumber), "left")
print("After joining AccountSK:", df_6.count())

df_7 = df_6.join(Branch, df_6.AGEE == Branch.BranchCode, "left")
print("After joining Branch:", df_7.count())

df_8 = df_7.join(Currency.alias("Currency1"), df_7.DEVE == col("Currency1.CurrencyCode"), "left")
print("After joining Currency1:", df_8.count())

df_9 = df_8.join(Currency.alias("Currency2"), df_8.DEV == col("Currency2.CurrencyCode"), "left")
print("After joining Currency2:", df_9.count())

df_10 = df_9.join(DateDim.alias("DateDim2"), df_9.DCOM == col("DateDim2.DateValue"), "left")
print("After joining DateDim2:", df_10.count())

df_11 = df_10.join(DateDim.alias("DateDim3"), df_10.DCRE == col("DateDim3.DateValue"), "left")
print("After joining DateDim3:", df_11.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cl_bkcompens_error.select("SENS").distinct().show()


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
    #.join(Institution, (cl_bkcompens_av.ETABD == Institution.BankCode) & (cl_bkcompens_av.GUIBD == Institution.CounterCode), "left")
    .join(AccountSK, (cl_bkcompens_av.AGEE == AccountSK.BranchCode) & (cl_bkcompens_av.DEVE == AccountSK.CurrencyCode) & (cl_bkcompens_av.NCPE == AccountSK.AccountNumber), "left")
    .join(Branch, cl_bkcompens_av.AGEE == Branch.BranchCode, "left")
    .join(Currency.alias("Currency1"), cl_bkcompens_av.DEVE == col("Currency1.CurrencyCode"), "left")
    .join(Currency.alias("Currency2"), cl_bkcompens_av.DEV == col("Currency2.CurrencyCode"), "left")
    .join(DateDim.alias("DateDim2"), cl_bkcompens_av.DCOM == col("DateDim2.DateValue"), "left")
    .join(DateDim.alias("DateDim3"), cl_bkcompens_error.DCRE == col("DateDim3.DateValue"), "left")
    # .filter(cl_bkcompens_av.IDAV.isNotNull())
    .select(
        ClearingDim.ClearingID, lit('A').alias("ClearingType"),
        col("DateDim1.DateKey").alias("SettleDate"),
        OperationCode.OperationID,
       lit(None).alias("InstitutionID"),
        trim(cl_bkcompens_av.COMD).alias("RemitAcc"),
        AccountSK.AccountID.alias("ReceiptAccID"),
        Branch.BranchID.alias("ReceiptBranchID"),
        col("Currency1.CurrencyID").alias("ReceipientCurrID"), 
        col("Currency2.CurrencyID").alias("OperationCurrID"),
        cl_bkcompens_av.MON.alias("Amount"), 
        cl_bkcompens_av.REF.alias("TranRef"), 
        col("DateDim2.DateKey").alias("ClearingDate"),
        cl_bkcompens_error.IDEN,
        cl_bkcompens_error.TYPE,
        cl_bkcompens_error.PROG,
        cl_bkcompens_error.CERR,
        cl_bkcompens_error.LCERR,
        cl_bkcompens_error.MESS,
        cl_bkcompens_av.ETA,
        col("DateDim3.DateKey").alias("DATEKEY3"),
        cl_bkcompens_error.HCRE,
       cl_bkcompens_error.BATCHID,
        cl_bkcompens_error.BATCHDATE,
        to_date(cl_bkcompens_error.BATCHDATE).alias("CREATEDON"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_error.SYSTEMCODE,
         lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_error.WORKFLOWNAME
    )
)
display(df_e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null_count = df_e.filter(col("ClearingID").isNull()).count()
not_null_count = df_e.filter(col("ClearingID").isNotNull()).count()

print(f"NULL ClearingID count: {null_count}")
print(f"NOT NULL ClearingID count: {not_null_count}")



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


# result_df.write.format("delta").mode("append").saveAsTable("CLEARINGERRFACT")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''created the view of the bronzelakhouse tables for sql (the default lakehouse is "Lakehouse" so we have to create views of the other lakehouse
if we want to use them in sql )'''
cl_bkcompens_av.createOrReplaceTempView("view_BKCOMPENS_AV")
cl_bkcompens_error.createOrReplaceTempView("view_BKCOMPENS_ERROR")
cl_bkcompens_rv.createOrReplaceTempView("view_BLCOMPENS_RV")
ClearingDim.createOrReplaceTempView("view_Clearingdim")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT  
# MAGIC C2.CLEARINGID,
# MAGIC 'R' AS CLEARINGTYPE,
# MAGIC C3.CLEARINGSTATUSID,
# MAGIC D.DATEKEY,
# MAGIC O.OPERATIONID,
# MAGIC INSTITUTION.BANKID,
# MAGIC C4.COME,
# MAGIC dbo_AccountSK.ACCOUNTID,
# MAGIC B.BranchID,
# MAGIC CUR1.CURRENCYID AS ReceipientCurrID,
# MAGIC CUR2.CURRENCYID AS OperationCurrID,
# MAGIC C4.MON,
# MAGIC C4.REF,
# MAGIC D2.DATEKEY,
# MAGIC C1.IDEN,
# MAGIC C1.TYPE,
# MAGIC C1.PROG,
# MAGIC C1.CERR,
# MAGIC C1.LCERR,
# MAGIC C1.MESS,
# MAGIC C1.ETA,
# MAGIC D3.DateKey,
# MAGIC C1.HCRE,
# MAGIC C1.BATCHID,
# MAGIC C1.BATCHDATE,
# MAGIC C1.SYSTEMCODE,
# MAGIC C1.WORKFLOWNAME   
# MAGIC FROM  view_BKCOMPENS_ERROR C1
# MAGIC 
# MAGIC LEFT JOIN  view_Clearingdim C2
# MAGIC 	ON C1.ID=C2.CLEARINGCODE 
# MAGIC 	AND C2.CLEARINGTYPE='R'
# MAGIC 
# MAGIC LEFT JOIN view_BLCOMPENS_RV C4  
# MAGIC 	ON C1.ID=C4.IDRV 
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_ClearingStatus C3 
# MAGIC 	ON C4.ETA=C3.STATUSCODE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_DateDim D 
# MAGIC 	ON C4.DREG=D.DATEVALUE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_OperationCode O 
# MAGIC 	ON C4.TOPE=O.OPERATIONCODE 
# MAGIC 	AND O.AGENCECODE='05100'
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_Institution INSTITUTION
# MAGIC 	ON C4.ETABE=INSTITUTION.BANKCODE 
# MAGIC 	AND C4.GUIBE=INSTITUTION.COUNTERCODE
# MAGIC 
# MAGIC LEFT JOIN  Lakehouse.dbo_AccountSk  
# MAGIC 	ON C4.AGED=dbo_AccountSK.BRANCHCODE 
# MAGIC 	AND C4.DEVD=dbo_AccountSK.CURRENCYCODE 
# MAGIC 	AND C4.NCPD=dbo_AccountSK.ACCOUNTNUMBER
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_Branch  B 
# MAGIC 	ON C4.AGED=B.BRANCHCODE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_Currency CUR1 
# MAGIC 	ON C4.DEVD=CUR1.CURRENCYCODE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_Currency CUR2 
# MAGIC 	ON C4.DEV=CUR2.CURRENCYCODE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_DateDim D2 
# MAGIC 	ON C4.DCOM=D2.DATEVALUE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_DateDim D3  
# MAGIC 	ON  C1.DCRE=D3.DATEVALUE
# MAGIC 
# MAGIC WHERE C1.SENS='R'


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT LENGTH(C1.ID) AS Length_BK, LENGTH(C2.CLEARINGCODE) AS Length_DIM  
# MAGIC FROM view_BKCOMPENS_ERROR C1  
# MAGIC LEFT JOIN view_Clearingdim C2  
# MAGIC     ON C1.ID = C2.CLEARINGCODE  
# MAGIC WHERE C1.ID IN (  
# MAGIC     '19110811394800000000', '19111113191300000000', '20031113354500000000'  
# MAGIC )  
# MAGIC AND C2.CLEARINGCODE IS NOT NULL;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### for A


# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC C2.CLEARINGID,
# MAGIC 'A' AS CLEARINGTYPE,
# MAGIC D.DATEKEY,
# MAGIC O.OPERATIONID,
# MAGIC INSTITUTION.BANKID,
# MAGIC TRIM(C4.COMD) AS COMD,
# MAGIC dbo_AccountSK.ACCOUNTID,
# MAGIC B.BranchID,
# MAGIC CUR1.CURRENCYID AS ReceipientCurrID,
# MAGIC CUR2.CURRENCYID AS OperationCurrID,
# MAGIC C4.MON,
# MAGIC C4.REF,
# MAGIC D2.DATEKEY,
# MAGIC C1.IDEN,
# MAGIC C1.TYPE,
# MAGIC C1.PROG,
# MAGIC C1.CERR,
# MAGIC C1.LCERR,
# MAGIC C1.MESS,
# MAGIC C1.ETA,
# MAGIC D3.DATEKEY,
# MAGIC C1.HCRE,
# MAGIC C1.BATCHID,
# MAGIC C1.BATCHDATE,
# MAGIC C1.SYSTEMCODE,
# MAGIC C1.WORKFLOWNAME 
# MAGIC 
# MAGIC FROM view_BKCOMPENS_ERROR C1 
# MAGIC 
# MAGIC LEFT JOIN  view_Clearingdim C2 
# MAGIC 	ON C1.ID=C2.CLEARINGCODE
# MAGIC 	AND C2.CLEARINGTYPE='A'
# MAGIC 
# MAGIC LEFT JOIN view_BKCOMPENS_AV C4
# MAGIC 	ON C1.ID=C4.IDAV 
# MAGIC 
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_DateDim D 
# MAGIC 	ON C4.DCOM=D.DATEVALUE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_OperationCode O 
# MAGIC 	ON C4.TOPE=O.OPERATIONCODE 
# MAGIC 	AND O.AGENCECODE='05100'
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_Institution INSTITUTION  
# MAGIC 	ON C4.ETABD=INSTITUTION.BANKCODE 
# MAGIC 	AND C4.GUIBD=INSTITUTION.COUNTERCODE
# MAGIC 
# MAGIC LEFT JOIN  Lakehouse.dbo_AccountSK  
# MAGIC 	ON C4.AGEE=dbo_AccountSK.BRANCHCODE 
# MAGIC 	AND C4.DEVE=dbo_AccountSK.CURRENCYCODE 
# MAGIC 	AND C4.NCPE=dbo_AccountSK.ACCOUNTNUMBER
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_Branch  B
# MAGIC 	ON C4.AGEE=B.BRANCHCODE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_Currency CUR1 
# MAGIC 	ON C4.DEVE=CUR1.CURRENCYCODE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_Currency CUR2 
# MAGIC 	ON C4.DEV=CUR2.CURRENCYCODE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_DateDim D2 
# MAGIC 	ON C4.DCOM=D2.DATEVALUE
# MAGIC 
# MAGIC LEFT JOIN Lakehouse.dbo_DateDim D3  
# MAGIC 	ON  C1.DCRE=D3.DATEVALUE
# MAGIC 
# MAGIC WHERE C1.SENS='E'
# MAGIC -- AND C4.IDAV IS NOT NULL;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS TotalRecords,
# MAGIC     SUM(CASE WHEN CLEARINGID IS NULL THEN 1 ELSE 0 END) AS NullClearingID
# MAGIC FROM (
# MAGIC     -- Your main query
# MAGIC     SELECT 
# MAGIC         C2.CLEARINGID
# MAGIC     FROM view_BKCOMPENS_ERROR C1 
# MAGIC     LEFT JOIN view_Clearingdim C2 
# MAGIC         ON C1.ID = C2.CLEARINGCODE
# MAGIC         AND C2.CLEARINGTYPE = 'A'
# MAGIC     WHERE C1.SENS = 'E'
# MAGIC ) SubQuery;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT CLEARINGCODE, CLEARINGTYPE 
# MAGIC FROM view_Clearingdim 
# MAGIC WHERE CLEARINGCODE IN (SELECT ID FROM view_BKCOMPENS_ERROR WHERE SENS = 'E');


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT ID 
# MAGIC FROM view_BKCOMPENS_ERROR C1
# MAGIC WHERE ID NOT IN (SELECT DISTINCT CLEARINGCODE FROM view_Clearingdim WHERE CLEARINGTYPE = 'A');
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT CLEARINGCODE, CLEARINGTYPE 
# MAGIC FROM view_Clearingdim 
# MAGIC WHERE CLEARINGCODE IN (
# MAGIC     '19110811394800000000',
# MAGIC     '19111113191300000000',
# MAGIC     '24112716254400400000',
# MAGIC     '20013115560300000000',
# MAGIC     '24100816263500400000',
# MAGIC     '21082414031000100000',
# MAGIC     '21082414031300100000',
# MAGIC     '21091514025200100000',
# MAGIC     '21090114474500100000'
# MAGIC     
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
