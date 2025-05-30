# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e56ddaf5-2e15-4634-91cc-e2eb818afa61",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "f1bde452-2399-41bc-9ba5-b3c078a190fc",
# META       "known_lakehouses": [
# META         {
# META           "id": "ac470c1d-7c5d-4111-b733-488950a5aeb6"
# META         },
# META         {
# META           "id": "e56ddaf5-2e15-4634-91cc-e2eb818afa61"
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, concat_ws, sha2

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ClearingDim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingDim")
ClearingSK = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingSK")
ClearingSK.createOrReplaceTempView("clearingSK")

CL_BKCOMPENS_RF = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/BKCOMPENS_RF")
Branch = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Branch")
ClearingStatus = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingStatus")
CL_BKCOMPENS_RF_ETA = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RF_ETA")
CL_BKCOMPENS_RV_ETA = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV_ETA")
CL_BKCOMPENS_RV_CHQ = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV_CHQ")
CL_BKCOMPENS_RV_TRF = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV_TRF")
CL_BKCOMPENS_AV = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV")
CL_BKCOMPENS_AF = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AF")
CL_BKCOMPENS_AF_ETA = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AF_ETA")
CL_BKCOMPENS_AV_CHQ = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV_CHQ")
CL_BKCOMPENS_AF_ETA = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AF_ETA")
CL_BKCOMPENS_RV = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV")
CL_BKCOMPENS_AV_ETA = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV_ETA")
CL_BKCOMPENS_AV_TRF = spark.read.format("delta").load("abfss://Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV_TRF")
ClearingDim_H = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingDim_H")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC 	 temp.Hardcoded
# MAGIC 	,temp.IDRV
# MAGIC FROM (select 
# MAGIC distinct 
# MAGIC 'R' Hardcoded
# MAGIC ,IDRV
# MAGIC from BronzeLakehouse.CL_BKCOMPENS_RV
# MAGIC  
# MAGIC union all
# MAGIC  
# MAGIC select  distinct 
# MAGIC 'A' Hardcoded
# MAGIC ,IDAV
# MAGIC from BronzeLakehouse.CL_BKCOMPENS_AV) temp
# MAGIC LEFT JOIN clearingSK DW ON temp.Hardcoded = DW.ClearingType and temp.IDRV = DW.ClearingCode
# MAGIC WHERE DW.ClearingType IS NULL and DW.ClearingCode is null
# MAGIC  

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select 
# MAGIC distinct 
# MAGIC 'R' Hardcoded
# MAGIC ,IDRV
# MAGIC from BronzeLakehouse.CL_BKCOMPENS_RV
# MAGIC  
# MAGIC union all
# MAGIC  
# MAGIC select  distinct 
# MAGIC 'A' Hardcoded
# MAGIC ,IDAV
# MAGIC from BronzeLakehouse.CL_BKCOMPENS_AV

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE GoldLakehouse.dbo_ClearingSK AS 
# MAGIC SELECT * FROM clearingSK

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check if ClearingDim has records
if ClearingDim.count() > 0:
    empty_df = spark.createDataFrame([], spark.read.table("Lakehouse.dbo_ClearingDim").schema)
    #empty_df.write.format("delta").mode("overwrite").save("Lakehouse.dbo_ClearingDim")

#aggregations
eta_1 = (CL_BKCOMPENS_RF_ETA.groupBy("IDFIC").agg(F.max("DCRE").alias("DCRE")))
eta_2 = (CL_BKCOMPENS_RV_ETA.alias("a")
        .join(
            CL_BKCOMPENS_RV_ETA
            .groupBy("IDRV", "MOT")
            .agg(F.max("DCOM").alias("DCOM"))
            .alias("b"),
            (F.col("a.DCOM") == F.col("b.DCOM")) &
            (F.col("a.IDRV") == F.col("b.IDRV")) &
            (F.col("a.MOT") == F.col("b.MOT")),
            "inner"
            )
            .groupBy("a.IDRV")
            .agg(F.max("b.MOT").alias("MOT"))
            )
DF_A = (CL_BKCOMPENS_RV.alias("rv")
        .join(ClearingSK, CL_BKCOMPENS_RV.IDRV == ClearingSK.ClearingCode, "left")
        .join(CL_BKCOMPENS_RF, CL_BKCOMPENS_RV.IDFIC == CL_BKCOMPENS_RF.IDFIC, "left")
        .join(Branch, CL_BKCOMPENS_RF.AGE == Branch.BranchCode, "left")
        .join(ClearingStatus, CL_BKCOMPENS_RF.ETA == ClearingStatus.StatusCode, "left")
        .join(CL_BKCOMPENS_RF.alias("rf"), F.col("rv.IDFIC") == F.col("rf.IDFIC"), "left") 
        .join(Branch.alias("Branch"), F.col("rf.AGE") == F.col("Branch.BranchCode"), "left")
        .join(ClearingStatus.alias("CS"), F.col("rf.ETA") == F.col("CS.StatusCode"), "left")
        .join(eta_1, F.col("rf.IDFIC") == eta_1.IDFIC, "left")
        .join(eta_2, CL_BKCOMPENS_RV.IDRV == eta_2.IDRV, "left")
        .join(CL_BKCOMPENS_RV_CHQ, CL_BKCOMPENS_RV.IDRV == CL_BKCOMPENS_RV_CHQ.IDRV, "left")
        .join(CL_BKCOMPENS_RV_TRF, CL_BKCOMPENS_RV.IDRV == CL_BKCOMPENS_RV_TRF.IDRV, "left")
        .filter(ClearingSK.ClearingType == 'R')
        .select(
            (ClearingSK.ClearingID).alias("ClearingID"),
            (ClearingSK.ClearingType).alias("ClearingType"),
            (CL_BKCOMPENS_RV.IDRV).alias("IDAV"),
            F.col("rf.AGE").alias("AGE"),
            F.col("Branch.BranchName").alias("BranchName"),
            F.col("rf.IDFIC").alias("IDFIC"),
            F.col("rf.ETA").alias("ETA"),
            F.col("CS.StatusDesc").alias("StatusDesc"),
            F.col("rf.REF_COM").alias("REF_COM"),
            F.col("rf.IDFIC").alias("NFIC"),
            (eta_1.DCRE).alias("DCRE"),
           (CL_BKCOMPENS_RV.ETA).alias("ETA"),
            F.col("CS.StatusDesc").alias("StatusDesc"),
            (CL_BKCOMPENS_RV.ID_ORIG).alias("ID_ORIG"),
            (CL_BKCOMPENS_RV.EMETTEUR).alias("EMETTEUR"),
            (CL_BKCOMPENS_RV.DESTINATAIRE).alias("DESTINATAIRE"),
            (CL_BKCOMPENS_RV.CLEE).alias("CLEE"),
            (CL_BKCOMPENS_RV.NOMD).alias("NOMD"),
            (CL_BKCOMPENS_RV.REF).alias("REF"),
            (CL_BKCOMPENS_RV.DCOM).alias("DCOM"),
            (eta_2.MOT).alias("MOT"),
            (CL_BKCOMPENS_RV_CHQ.IDRVCHQ).alias("IDRVCHQ"),
            (CL_BKCOMPENS_RV_CHQ.CTRLVISU).alias("CTRLVISU"),
            (CL_BKCOMPENS_RV_CHQ.NCHQ).alias("NCHQ"),
            F.concat(CL_BKCOMPENS_RV_TRF.MOT1, CL_BKCOMPENS_RV_TRF.MOT2, CL_BKCOMPENS_RV_TRF.MOT3, CL_BKCOMPENS_RV_TRF.MOT4),
            F.col("rv.Batch_ID").alias("Batch_ID"),
            F.col("rv.Batch_Date").alias("Batch_Date"),
            F.col("rv.Batch_Date").cast("date").alias("CreatedOn"),
            F.col("rv.SYSTEMCODE").alias("SYSTEMCODE"),
            F.col("rv.WORKFLOWNAME").alias("WORKFLOWNAME"),
        )
        )

DF_B = (
    CL_BKCOMPENS_AV.alias("CL_BKCOMPENS_AV")
    .join(ClearingSK.alias("ClearingSK"), 
        F.col("ClearingSK.ClearingCode") == F.col("CL_BKCOMPENS_AV.IDAV"), "left")
    .join(CL_BKCOMPENS_AF.alias("CL_BKCOMPENS_AF"), 
          F.col("CL_BKCOMPENS_AV.IDFIC") == F.col("CL_BKCOMPENS_AF.IDFIC"), 
          "left")
    .join(Branch.alias("Branch"), 
          F.col("CL_BKCOMPENS_AF.AGE") == F.col("Branch.BranchCode"), 
          "left")
    .join(ClearingStatus.alias("ClearingStatus"), 
          F.col("CL_BKCOMPENS_AF.ETA") == F.col("ClearingStatus.StatusCode"), 
          "left")
    .join(
        CL_BKCOMPENS_AF_ETA.groupBy("IDFIC").agg(F.max("DCRE").alias("DCRE")).alias("eta_1"),
        F.col("CL_BKCOMPENS_AF.IDFIC") == F.col("eta_1.IDFIC"),
        "left")
    .join(
        CL_BKCOMPENS_AV_ETA.alias("eta_2")
        .join(
            CL_BKCOMPENS_AV_ETA.groupBy("IDAV", "MOT").agg(F.max("DCOM").alias("DCOM")).alias("b"),
            (F.col("eta_2.DCOM") == F.col("b.DCOM")) & 
            (F.col("eta_2.IDAV") == F.col("b.IDAV")) & 
            (F.col("eta_2.MOT") == F.col("b.MOT")),
            "inner"
        )
       .groupBy("eta_2.IDAV")
        .agg(F.max("b.MOT").alias("MOT")).alias("eta_2"),
        F.col("CL_BKCOMPENS_AV.IDAV") == F.col("eta_2.IDAV"),
        "left"
    )
    .join(CL_BKCOMPENS_AV_CHQ.alias("CL_BKCOMPENS_AV_CHQ"), 
          F.col("CL_BKCOMPENS_AV.IDAV") == F.col("CL_BKCOMPENS_AV_CHQ.IDAV"), 
          "left")
    .join(CL_BKCOMPENS_AV_TRF.alias("CL_BKCOMPENS_AV_TRF"), 
          F.col("CL_BKCOMPENS_AV.IDAV") == F.col("CL_BKCOMPENS_AV_TRF.IDAV"), 
          "left")
    .filter(F.col("ClearingSK.ClearingType") == "A")
    .select(
        F.col("ClearingSK.ClearingID"),
        F.col("ClearingSK.ClearingType"),
        F.col("CL_BKCOMPENS_AV.IDAV"),
        F.col("CL_BKCOMPENS_AF.AGE"),
        F.col("Branch.BranchName"),
        F.col("CL_BKCOMPENS_AF.IDFIC"),
        F.col("CL_BKCOMPENS_AF.ETA"),
        F.col("ClearingStatus.StatusDesc"),
        F.col("CL_BKCOMPENS_AF.REF_COM"),
        F.col("CL_BKCOMPENS_AF.NFIC"),
        F.col("eta_1.DCRE"),
        F.col("CL_BKCOMPENS_AV.ETA"),
        F.col("ClearingStatus.StatusDesc"),
        F.col("CL_BKCOMPENS_AV.ID_ORIG"),
        F.lit(None).alias("SenderInfo"),
        F.lit(None).alias("ReceiverInfo"),
        F.col("CL_BKCOMPENS_AV.CLEB"),
        F.col("CL_BKCOMPENS_AV.NOMD"),
        F.col("CL_BKCOMPENS_AV.REF"),
        F.col("CL_BKCOMPENS_AV.DCOM"),
        F.col("eta_2.MOT"),
        F.col("CL_BKCOMPENS_AV_CHQ.IDAVCHQ"),
        F.lit(None).alias("ChqVisStatus"),
        F.col("CL_BKCOMPENS_AV_CHQ.NCHQ"),
        F.concat(
            F.col("CL_BKCOMPENS_AV_TRF.MOT1"), 
            F.col("CL_BKCOMPENS_AV_TRF.MOT2"), 
            F.col("CL_BKCOMPENS_AV_TRF.MOT3"), 
            F.col("CL_BKCOMPENS_AV_TRF.MOT4")
        ).alias("narration"),
        F.col("CL_BKCOMPENS_AV.Batch_ID").alias("BatchID"),
        F.col("CL_BKCOMPENS_AV.Batch_Date").alias("BatchDate"),
        F.col("CL_BKCOMPENS_AV.Batch_Date").cast("date").alias("CreatedOn"),
        F.col("CL_BKCOMPENS_AV.SYSTEMCODE").alias("SystemCode"),
        F.col("CL_BKCOMPENS_AV.WORKFLOWNAME").alias("WorkFlowName")
    )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Combined_DF = DF_A.union(DF_B)

display(Combined_DF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#row_hash
columns_to_hash = [
    "ClearingID", "ClearingType", "ClearingCode", "FileBranchCode", "FileBranch",
    "FileCode", "FileStatus", "FileStatusDesc", "FileReference", "FileName",
    "FileStatusDate", "ClearingStatus", "ClearingStatusDesc", "InstrumentRef",
    "SenderInfo", "Receiver_Information", "CheckDigit", "ReceiverName",
    "TranRef", "ClearingDate", "ClearingStatusReason", "ChequeID",
    "ChqVisStatus", "ChqNumber", "Narration"
]

ClearingDim = ClearingDim.withColumn(
    "RowHash",
    sha2(concat_ws("", *[col(c) for c in columns_to_hash]), 256)
)

display(ClearingDim)

#ClearingDim_updated.write.format("delta").mode("overwrite").save("Lakehouse.dbo_ClearingDim")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#spark.sql("DROP TABLE IF EXISTS bronzelakehouse.dbo_clearingdim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#DW = ClearingDim (already existing)
#STG = new dataframe

DF_C = ClearingDim.alias("DW").join(Combined_DF.alias("STG"),
    (col("DW.ClearingType") == col("STG.ClearingType")) &
    (col("DW.ClearingCode") == col("STG.ClearingCode")) &
    (col("DW.RowHash") != col("STG.RowHash")), "inner"
).select(
    col("STG.ClearingID"), col("STG.ClearingType"), col("STG.ClearingCode"),
    col("STG.FileBranchCode"), col("STG.FileBranch"), col("STG.FileCode"),
    col("STG.FileStatus"), col("STG.FileStatusDesc"), col("STG.FileReference"),
    col("STG.FileName"), col("STG.FileStatusDate"), col("STG.ClearingStatus"),
    col("STG.ClearingStatusDesc"), col("STG.InstrumentRef"), col("STG.SenderInfo"),
    col("STG.Receiver_Information"), col("STG.CheckDigit"), col("STG.ReceiverName"),
    col("STG.TranRef"), col("STG.ClearingDate"), col("STG.ClearingStatusReason"),
    col("STG.ChequeID"), col("STG.ChqVisStatus"), col("STG.ChqNumber"),
    col("STG.Narration"), col("STG.BatchID"), col("STG.BatchDate"),
    col("STG.BatchDate").cast("date").alias("Updated_On"),
    col("STG.CreatedOn"), col("STG.SystemCode"), col("STG.RowHash"),
    col("STG.WorkflowName")
)

#DF_C.write.format("delta").mode("overwrite").save("Lakehouse.dbo_ClearingDim")

new_records = Combined_DF.alias("STG").join(ClearingDim.alias("DW"), col("STG.ClearingID"), "left_anti")\
                .select(
                col("STG.ClearingID"), col("STG.ClearingType"), col("STG.ClearingCode"),
                col("STG.FileBranchCode"), col("STG.FileBranch"), col("STG.FileCode"),
                col("STG.FileStatus"), col("STG.FileStatusDesc"), col("STG.FileReference"),
                col("STG.FileName"), col("STG.FileStatusDate"), col("STG.ClearingStatus"),
                col("STG.ClearingStatusDesc"), col("STG.InstrumentRef"), col("STG.SenderInfo"),
                col("STG.Receiver_Information"), col("STG.CheckDigit"), col("STG.ReceiverName"),
                col("STG.TranRef"), col("STG.ClearingDate"), col("STG.ClearingStatusReason"),
                col("STG.ChequeID"), col("STG.ChqVisStatus"), col("STG.ChqNumber"),
                col("STG.Narration"), col("STG.BatchID"), col("STG.BatchDate"),
                col("STG.CreatedOn"), col("STG.SystemCode"), col("STG.RowHash"), col("STG.WorkFlowName")
                )

#new_records.write.format("delta").mode("overwrite").save("Lakehouse.dbo_ClearingDim")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


updated_history_df = ClearingDim_H.alias("H").join(ClearingDim.alias("DW"), 
                        (col("H.ClearingID") == col("DW.ClearingID")) &
                        (col("H.SCD_RowHash") != col("DW.RowHash")), "inner") \
                        .filter(col("H.SCD_EndDate") == lit("9999-12-31")) \
                        .select(
                            col("H.ClearingID"), 
                            (date_add(col("DW.BatchDate"), -1)).alias("SCD_EndDate")
                        )


#updated_history_df.write.format("delta").mode("overwrite").save("Lakehouse.dbo_ClearingDim_H")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
