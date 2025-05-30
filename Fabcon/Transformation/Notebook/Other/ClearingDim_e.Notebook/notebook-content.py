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

# ### ****Libraries****

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType, BinaryType, DateType, TimestampType
from pyspark.sql.functions import col, concat_ws, sha2, to_date, lit

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Reading Table**

# CELL ********************

CL_BKCOMPENS_RF = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RF")
CL_BKCOMPENS_RF_ETA = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RF_ETA")
CL_BKCOMPENS_RV_ETA = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV_ETA")
CL_BKCOMPENS_RV_CHQ = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV_CHQ")
CL_BKCOMPENS_RV_TRF = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV_TRF")
CL_BKCOMPENS_AV = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV")
CL_BKCOMPENS_AF = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AF")
CL_BKCOMPENS_AF_ETA = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AF_ETA")
CL_BKCOMPENS_AV_CHQ = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV_CHQ")
CL_BKCOMPENS_AF_ETA = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AF_ETA")
CL_BKCOMPENS_RV = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_RV")
CL_BKCOMPENS_AV_ETA = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV_ETA")
CL_BKCOMPENS_AV_TRF = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CL_BKCOMPENS_AV_TRF")


ClearingDim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingDim")
ClearingSK = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingSK")
ClearingSK.createOrReplaceTempView("clearingSK")

Branch = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Branch")
Branch.createOrReplaceTempView("branch")

ClearingStatus = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingStatus")
ClearingStatus.createOrReplaceTempView("clearingstatus")

ClearingDim_H = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingDim_H")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp_RV = (CL_BKCOMPENS_RV
           .select(F.lit('R').alias("Hardcoded"), F.col("IDRV"))
           .distinct())

temp_AV = (CL_BKCOMPENS_AV
           .select(F.lit('A').alias("Hardcoded"), F.col("IDAV"))
           .distinct()) 

temp = temp_RV.union(temp_AV)

joined_df = (temp.alias("temp")
             .join(ClearingSK.alias("DW"),
                   (F.col("temp.Hardcoded") == F.col("DW.ClearingType")) & 
                   (F.col("temp.IDRV") == F.col("DW.ClearingCode")),
                   "left")
             .filter(F.col("DW.ClearingType").isNull() & F.col("DW.ClearingCode").isNull())
             .select(F.col("temp.Hardcoded").alias("ClearingType"), F.col("temp.IDRV").alias("ClearingCode")))

display(joined_df)

# Writing the final result back to the ClearingSK table
#joined_df.write.mode("append").saveAsTable("GoldLakehouse.dbo_clearingsk")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check if ClearingDim has records
#if ClearingDim.count() > 0:
    #empty_df = spark.createDataFrame([], spark.read.table("Lakehouse.dbo_ClearingDim").schema)
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
        .join(CL_BKCOMPENS_RV_TRF.alias("CL_BKCOMPENS_RV_TRF"),
    (col("rv.IDRV") == col("CL_BKCOMPENS_RV_TRF.IDRV")) & 
    (to_date(col("CL_BKCOMPENS_RV_TRF.bacth_Date"), "yyyy-MM-dd") == to_date(lit("2025-02-12"), "yyyy-MM-dd")),
    "left")
        .filter(ClearingSK.ClearingType == 'R')
        .select(
            (ClearingSK.ClearingID).alias("ClearingID"),
            (ClearingSK.ClearingType).alias("ClearingType"),
            (CL_BKCOMPENS_RV.IDRV),
            F.col("rf.AGE").alias("AGE"),
            F.col("Branch.BranchName").alias("BranchName"),
            F.col("rf.IDFIC").alias("IDFIC"),
            F.col("rf.ETA").alias("ETA_RF"),
            F.col("CS.StatusDesc").alias("StatusDesc_RF"),
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
            F.concat(CL_BKCOMPENS_RV_TRF.MOT1, CL_BKCOMPENS_RV_TRF.MOT2, CL_BKCOMPENS_RV_TRF.MOT3, CL_BKCOMPENS_RV_TRF.MOT4).alias("Narration"),
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
    (col("CL_BKCOMPENS_AV.IDAV") == col("CL_BKCOMPENS_AV_TRF.IDAV")) & 
    (to_date(col("CL_BKCOMPENS_AV_TRF.Batch_Date"), "yyyy-MM-dd") == to_date(lit("2025-02-12"), "yyyy-MM-dd")),
    "left")
    .filter(F.col("ClearingSK.ClearingType") == "A")
    .select(
        F.col("ClearingSK.ClearingID"),
        F.col("ClearingSK.ClearingType"),
        F.col("CL_BKCOMPENS_AV.IDAV").alias("IDRV"),
        F.col("CL_BKCOMPENS_AF.AGE"),
        F.col("Branch.BranchName"),
        F.col("CL_BKCOMPENS_AF.IDFIC"),
        F.col("CL_BKCOMPENS_AF.ETA"),
        F.col("ClearingStatus.StatusDesc"),
        F.col("CL_BKCOMPENS_AF.REF_COM"),
        F.col("CL_BKCOMPENS_AF.NFIC"),
        F.col("eta_1.DCRE"),
        F.col("CL_BKCOMPENS_AV.ETA").alias("ETA_AV"),
        F.col("ClearingStatus.StatusDesc").alias("StatusDesc_AV"),
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
        ).alias("Narration"),
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

clearing_dim_schema = StructType([
    StructField("ClearingID", LongType(), True),
    StructField("ClearingType", StringType(), True),
    StructField("ClearingCode", StringType(), True),
    StructField("FileBranchCode", StringType(), True),
    StructField("FileBranch", StringType(), True),
    StructField("FileCode", StringType(), True),
    StructField("FileStatus", StringType(), True),
    StructField("FileStatusDesc", StringType(), True),
    StructField("FileReference", StringType(), True),
    StructField("FileName", StringType(), True),
    StructField("FileStatusDate", TimestampType(), True),
    StructField("ClearingStatus", StringType(), True),
    StructField("ClearingStatusDesc", StringType(), True),
    StructField("InstrumentRef", StringType(), True),
    StructField("SenderInfo", StringType(), True),
    StructField("Receiver_Information", StringType(), True),
    StructField("CheckDigit", StringType(), True),
    StructField("ReceiverName", StringType(), True),
    StructField("TranRef", StringType(), True),
    StructField("ClearingDate", TimestampType(), True),
    StructField("ClearingStatusReason", StringType(), True),
    StructField("ChequeID", LongType(), True),
    StructField("ChqVisStatus", StringType(), True),
    StructField("ChqNumber", StringType(), True),
    StructField("Narration", StringType(), True),
    StructField("BatchID", IntegerType(), True),
    StructField("BatchDate", TimestampType(), True),
    StructField("CreatedOn", DateType(), True),
    StructField("Updated_On", TimestampType(), True),
    StructField("SystemCode", StringType(), True),
    StructField("RowHash", BinaryType(), True),
    StructField("WorkFlowName", StringType(), True)
])

clearing_dim_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), clearing_dim_schema)
display(clearing_dim_df)
clearing_dim_df.printSchema()

silver_lakehouse_path = "abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/SilverLakehouse.Lakehouse/Tables/Staging_ClearingDim"

clearing_dim_df.write.format("delta").mode("overwrite").save(silver_lakehouse_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Combined_DF.write.format("delta").mode("overwriteSchema").save(silver_lakehouse_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

column_mapping = {
    "ClearingID" : "ClearingID",
    "ClearingType" : "ClearingType",
    "IDRV" : "ClearingCode",
    "AGE" : "FileBranchCode",
    "BranchName" : "FileBranch",
    "IDFIC" : "FileCode",
    "ETA_RF" : "FileStatus",
    "StatusDesc_RF" : "FileStatusDesc",
    "REF_COM" : "FileReference",
    "NFIC" : "FileName",
    "DCRE" : "FileStatusDate",
    "ETA" : "ClearingStatus",
    "StatusDesc" : "ClearingStatusDesc",
    "ID_ORIG" : "InstrumentRef",
    "EMETTEUR" : "SenderInfo",
    "DESTINATAIRE" : "Receiver_Information",
    "CLEE" : "CheckDigit",
    "NOMD" : "ReceiverName",
    "REF" : "TranRef",
    "DCOM" : "ClearingDate",
    "MOT" : "ClearingStatusReason",
    "IDRVCHQ" : "ChequeID",
    "CTRLVISU" : "ChqVisStatus",
    "NCHQ" : "ChqNumber",
    "Narration" : "Narration",
    "Batch_ID" : "BatchID",
    "Batch_Date" : "BatchDate",
    "CreatedOn" : "CreatedOn",
    "SYSTEMCODE" : "SystemCode",
    "WORKFLOWNAME" : "WorkFlowName"
}

Combined_DF = Combined_DF.selectExpr(
    *[f"{src} as {dest}" for src, dest in column_mapping.items()]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_lakehouse_path = "abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/SilverLakehouse.Lakehouse/Tables/Staging_ClearingDim"

# Cast ChequeID from decimal to long
Combined_DF = Combined_DF.withColumn("ChequeID", col("ChequeID").cast("long"))


target_table_df = spark.read.format("delta").load(silver_lakehouse_path)
target_table_df.printSchema()
Combined_DF.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_lakehouse_path = "abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/SilverLakehouse.Lakehouse/Tables/Staging_ClearingDim"

Combined_DF.write.format("delta") \
    .mode("overwrite") \
    .save(silver_lakehouse_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.sql("SELECT * FROM SilverLakehouse.Staging_ClearingDim LIMIT 1000")
# display(df)

# print("combined df")
# display(Combined_DF)
df = spark.sql("SELECT * FROM SilverLakehouse.Staging_ClearingDim").filter(col("ClearingType") == "A").orderBy("ClearingID")
display(df)

print("combined df")

# Filter and sort Combined_DF
Combined_DF = Combined_DF.filter(col("ClearingType") == "A").orderBy("ClearingID")
display(Combined_DF)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, concat_ws, sha2
from pyspark.sql.types import BinaryType

# Define columns to hash
columns_to_hash = [
    "ClearingID", "ClearingType", "ClearingCode", "FileBranchCode", "FileBranch",
    "FileCode", "FileStatus", "FileStatusDesc", "FileReference", "FileName",
    "FileStatusDate", "ClearingStatus", "ClearingStatusDesc", "InstrumentRef",
    "SenderInfo", "Receiver_Information", "CheckDigit", "ReceiverName",
    "TranRef", "ClearingDate", "ClearingStatusReason", "ChequeID",
    "ChqVisStatus", "ChqNumber", "Narration"
]

# Load the existing table
staging_clearing_dim_df = spark.read.format("delta").load("abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/SilverLakehouse.Lakehouse/Tables/Staging_ClearingDim")

# Compute RowHash and CAST it to BinaryType
staging_clearing_dim_hashed = staging_clearing_dim_df.withColumn(
    "RowHash",
    sha2(concat_ws("", *[col(c) for c in columns_to_hash]), 256).cast(BinaryType())  
)

# Display updated data
display(staging_clearing_dim_hashed)

# Merge logic to update only RowHash
from delta.tables import DeltaTable

staging_clearing_dim_table = DeltaTable.forPath(spark, "abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/SilverLakehouse.Lakehouse/Tables/Staging_ClearingDim")

staging_clearing_dim_table.alias("target").merge(
    staging_clearing_dim_hashed.alias("source"),
    "target.ClearingID = source.ClearingID"  # Use primary key
).whenMatchedUpdate(
    set={"RowHash": col("source.RowHash")}
).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM BronzeLakehouse.Mtd_Clr_Brn_Tbl_Lst

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
