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
cl_bkcompens_rf_eta = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_RF_ETA_V")
cl_bkcompens_rv_eta = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_RV_ETA_V")
cl_bkcompens_rv_chq = spark.read.format("delta").table("BronzeLakehouse.cl_bkcompens_rv_chq_V")
cl_bkcompens_rv_trf = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_RV_TRF_V")
cl_bkcompens_av = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AV_V")
cl_bkcompens_af = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AF_V")
cl_bkcompens_af_eta = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AF_ETA_V")
cl_bkcompens_av_chq = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AV_CHQ_V")
cl_bkcompens_rv = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_RV_V")
cl_bkcompens_av_eta = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AV_ETA_V")
cl_bkcompens_av_trf = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_AV_TRF_V")
cl_bkcompens_rf = spark.read.format("delta").table("BronzeLakehouse.CL_BKCOMPENS_RF_V")

#Staging Tables
stg_clearingdim = spark.read.format("delta").table("SilverLakehouse.ClearingDim")

#Gold Tables
clearingsk = spark.read.format("delta").table("GoldLakehouse.ClearingSK")

#Lookup Warehouse tables: Reading from Other workspace Lakehouse
branch = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Branch")
clearingstatus = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingStatus")


#Creating View
#clearingStatus.createOrReplaceTempView("ClearingStatus")
# branch.createOrReplaceTempView("Branch")
# clearingsk.createOrReplaceTempView("clearingsk_v")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. ClearingSK ETL

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from GoldLakehouse.ClearingSK;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#To apply Row_Number to generate unique number just like Identity column in SQL Server
MaxClearingID = clearingsk.select(max('ClearingID').alias('ClearingID')).collect()[0]['ClearingID']
window_spec = Window.orderBy(['ClearingType', 'ClearingCode'])

temp_RV = (cl_bkcompens_rv.select(lit('R').alias("Hardcoded"), col("IDRV")).distinct())
temp_AV = (cl_bkcompens_av.select(lit('A').alias("Hardcoded"), col("IDAV")).distinct()) 

RV_Union_AV = temp_RV.union(temp_AV)

new_sk = (RV_Union_AV.alias("RV_Union_AV")
             .join(clearingsk.alias("DW"),
                   (col("RV_Union_AV.Hardcoded") == col("DW.ClearingType")) & 
                   (col("RV_Union_AV.IDRV") == col("DW.ClearingCode")),
                   "left")
             .filter(col("DW.ClearingType").isNull() & col("DW.ClearingCode").isNull())
             .select("ClearingID", col("RV_Union_AV.Hardcoded").alias("ClearingType"), col("RV_Union_AV.IDRV").alias("ClearingCode")))

new_sk_final = new_sk.withColumn("ClearingID", (row_number().over(window_spec) + MaxClearingID).cast(LongType())) \
                              .select('ClearingID', 'ClearingType', 'ClearingCode')
display(new_sk_final)

#Writing the final result back to the ClearingSK table
new_sk_final.write.mode("append").saveAsTable("GoldLakehouse.ClearingSK")
print(f'Successfully Appended new records in ClearingSK')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from GoldLakehouse.ClearingSK;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. ClearingDim Silver ETL

# MARKDOWN ********************

# #### 4.1 Creating RV Dataframe

# CELL ********************

# Aggregate eta_1_rv
eta_1_rv = cl_bkcompens_rf_eta.groupBy("IDFIC").agg(max("DCRE").alias("DCRE"))

# Aggregate eta_2_rv
eta_2_rv = cl_bkcompens_rv_eta.alias("a").join(
    cl_bkcompens_rv_eta.groupBy("IDRV", "MOT").agg(max("DCOM").alias("DCOM")).alias("b"),
    (col("a.DCOM") == col("b.DCOM")) & (col("a.IDRV") == col("b.IDRV")) & (col("a.MOT") == col("b.MOT")),
    "inner"
).groupBy("a.IDRV").agg(max("a.MOT").alias("MOT"))

result_df_rv = (
    cl_bkcompens_rv
    .join(clearingsk, cl_bkcompens_rv.IDRV == clearingsk.ClearingCode, "left")
    .join(cl_bkcompens_rf, cl_bkcompens_rv.IDFIC == cl_bkcompens_rf.IDFIC, "left")
    .join(branch, cl_bkcompens_rf.AGE == branch.BranchCode, "left")
    .join(clearingstatus, cl_bkcompens_rf.ETA == clearingstatus.StatusCode, "left")
    .join(eta_1_rv, cl_bkcompens_rf.IDFIC == eta_1_rv.IDFIC, "left")
    .join(eta_2_rv, cl_bkcompens_rv.IDRV == eta_2_rv.IDRV, "left")
    .join(cl_bkcompens_rv_chq, cl_bkcompens_rv.IDRV == cl_bkcompens_rv_chq.IDRV, "left")
    .join(cl_bkcompens_rv_trf, cl_bkcompens_rv.IDRV == cl_bkcompens_rv_trf.IDRV, "left")
    .filter(clearingsk.ClearingType == lit('R'))
    .select(
        clearingsk.ClearingID.alias("ClearingID"),
        clearingsk.ClearingType.alias("ClearingType"),
        cl_bkcompens_rv.IDRV.alias("ClearingCode"),
        cl_bkcompens_rf.AGE.alias("FileBranchCode"),
        branch.BranchName.alias("FileBranch"),
        cl_bkcompens_rf.IDFIC.alias("FileCode"),
        cl_bkcompens_rf.ETA.alias("FileStatus"),
        clearingstatus.StatusDesc.alias("FileStatusDesc"),
        cl_bkcompens_rf.REF_COM.alias("FileReference"),
        cl_bkcompens_rf.IDFIC.alias("FileName"),
        eta_1_rv.DCRE.alias("FileStatusDate"),
        cl_bkcompens_rv.ETA.alias("ClearingStatus"),
        clearingstatus.StatusDesc.alias("ClearingStatusDesc"),
        cl_bkcompens_rv.ID_ORIG.alias("InstrumentRef"),
        cl_bkcompens_rv.EMETTEUR.alias("SenderInfo"),
        cl_bkcompens_rv.DESTINATAIRE.alias("ReceiverInformation"),
        cl_bkcompens_rv.CLEE.alias("CheckDigit"),
        cl_bkcompens_rv.NOMD.alias("ReceiverName"),
        cl_bkcompens_rv.REF.alias("TranRef"),
        cl_bkcompens_rv.DCOM.alias("ClearingDate"),
        eta_2_rv.MOT.alias("ClearingStatusReason"),
        cl_bkcompens_rv_chq.IDRVCHQ.cast("long").alias("ChequeID"),
        cl_bkcompens_rv_chq.CTRLVISU.alias("ChqVisStatus"),
        cl_bkcompens_rv_chq.NCHQ.alias("ChqNumber"),
        concat(cl_bkcompens_rv_trf.MOT1, cl_bkcompens_rv_trf.MOT2, cl_bkcompens_rv_trf.MOT3, cl_bkcompens_rv_trf.MOT4).alias("Narration"),
        cl_bkcompens_rv.BatchID,
        cl_bkcompens_rv.BatchDate,
        cl_bkcompens_rv.BatchDate.alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_rv.SystemCode,
        lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_rv.WorkflowName
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(cl_bkcompens_rv.count())
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

# #### 4.2 Creating AV Dataframe

# CELL ********************

# Aggregate eta_1_av
eta_1_av = cl_bkcompens_af_eta.groupBy("IDFIC").agg(max("DCRE").alias("DCRE"))

# Aggregate eta_2_av
eta_2_av = cl_bkcompens_av_eta.alias("a").join(
    cl_bkcompens_av_eta.groupBy("IDAV", "MOT").agg(max("DCOM").alias("DCOM")).alias("b"),
    (col("a.DCOM") == col("b.DCOM")) & (col("a.IDAV") == col("b.IDAV")) & (col("a.MOT") == col("b.MOT")),
    "inner"
).groupBy("a.IDAV").agg(max("a.MOT").alias("MOT"))

# Final DataFrame
result_df_av = (
    cl_bkcompens_av
    .join(clearingsk, cl_bkcompens_av.IDAV == clearingsk.ClearingCode, "left")
    .join(cl_bkcompens_af, cl_bkcompens_av.IDFIC == cl_bkcompens_af.IDFIC, "left")
    .join(branch, cl_bkcompens_af.AGE == branch.BranchCode, "left")
    .join(clearingstatus, cl_bkcompens_af.ETA == clearingstatus.StatusCode, "left")
    .join(eta_1_av, cl_bkcompens_af.IDFIC == eta_1_av.IDFIC, "left")
    .join(eta_2_av, cl_bkcompens_av.IDAV == eta_2_av.IDAV, "left")
    .join(cl_bkcompens_av_chq, cl_bkcompens_av.IDAV == cl_bkcompens_av_chq.IDAV, "left")
    .join(cl_bkcompens_av_trf, cl_bkcompens_av.IDAV == cl_bkcompens_av_trf.IDAV, "left")
    .filter(clearingsk.ClearingType == lit('A'))
    .select(
        clearingsk.ClearingID.alias("ClearingID"),
        clearingsk.ClearingType.alias("ClearingType"),
        cl_bkcompens_av.IDAV.alias("ClearingCode"),
        cl_bkcompens_af.AGE.alias("FileBranchCode"),
        branch.BranchName.alias("FileBranch"),
        cl_bkcompens_af.IDFIC.alias("FileCode"),
        cl_bkcompens_af.ETA.alias("FileStatus"),
        clearingstatus.StatusDesc.alias("FileStatusDesc"),
        cl_bkcompens_af.REF_COM.alias("FileReference"),
        cl_bkcompens_af.NFIC.alias("FileName"),
        eta_1_av.DCRE.alias("FileStatusDate"),
        cl_bkcompens_av.ETA.alias("ClearingStatus"),
        clearingstatus.StatusDesc.alias("ClearingStatusDesc"),
        cl_bkcompens_av.ID_ORIG.alias("InstrumentRef"),
        lit(None).alias("SenderInfo"),
        lit(None).alias("ReceiverInformation"),
        cl_bkcompens_av.CLEB.alias("CheckDigit"),
        cl_bkcompens_av.NOMD.alias("ReceiverName"),
        cl_bkcompens_av.REF.alias("TranRef"),
        cl_bkcompens_av.DCOM.alias("ClearingDate"),
        eta_2_av.MOT.alias("ClearingStatusReason"),
        cl_bkcompens_av_chq.IDAVCHQ.cast("long").alias("ChequeID"),
        lit(None).alias("ChqVisStatus"),
        cl_bkcompens_av_chq.NCHQ.alias("ChqNumber"),
        concat(
            cl_bkcompens_av_trf.MOT1, cl_bkcompens_av_trf.MOT2, 
            cl_bkcompens_av_trf.MOT3, cl_bkcompens_av_trf.MOT4
        ).alias("Narration"),
        cl_bkcompens_av.BatchID,
        cl_bkcompens_av.BatchDate,
        cl_bkcompens_av.BatchDate.alias("CreatedOn"),
        lit(None).cast("date").alias("UpdatedOn"),
        cl_bkcompens_av.SystemCode,
        lit(None).cast("binary").alias("RowHash"),
        cl_bkcompens_av.WorkflowName
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(cl_bkcompens_av.count())
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

# #### 4.3 Joining RV with AV Dataframe 

# CELL ********************

df_union_rv_av = result_df_rv.union(result_df_av)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4.4 Adding Values to RowHash Column

# CELL ********************

#creating rowhash

df_union_rv_av = df_union_rv_av.withColumn("RowHash", sha2(concat_ws("", "ClearingID", "ClearingType", "ClearingCode", "FileBranchCode", "FileBranch", "FileCode", "FileStatus", "FileStatusDesc", "FileReference", "FileName", "FileStatusDate", "ClearingStatus", "ClearingStatusDesc", "InstrumentRef", \
                                                "SenderInfo", "ReceiverInformation", "CheckDigit", "ReceiverName", "TranRef", "ClearingDate", "ClearingStatusReason", "ChequeID", "ChqVisStatus", "ChqNumber", "Narration"), \
                                                256).cast("binary")) #empty string in start is a seperator

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

# ### 5. Writing Data into "**SilverLakehouse.ClearingDim**"

# CELL ********************

df_union_rv_av.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.ClearingDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from SilverLakehouse.ClearingDim where ClearingType = 'R';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from SilverLakehouse.ClearingDim where ClearingType = 'A';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
