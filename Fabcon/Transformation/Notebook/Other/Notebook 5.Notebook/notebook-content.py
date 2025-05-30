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
# META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
# META         },
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
# META         },
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

ClearingDim = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingDim")
ClearingDim.createOrReplaceTempView("ClearingDim")

ClearingSK = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_ClearingSK")
ClearingSK.createOrReplaceTempView("clearingSK")

Branch = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/dbo_Branch")
Branch.createOrReplaceTempView("Branch")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC describe extended ClearingDim

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GoldLakehouse.dbo_clearingsk LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC Create table GoldLakehouse.ClearingSK as select * from clearingSK;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC Create table GoldLakehouse.Branch as select * from Branch;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE SilverLakehouse.ClearingDim (
# MAGIC     ClearingID BIGINT,
# MAGIC     ClearingType STRING,
# MAGIC     ClearingCode STRING,
# MAGIC     FileBranchCode STRING,
# MAGIC     FileBranch STRING,
# MAGIC     FileCode STRING,
# MAGIC     FileStatus STRING,
# MAGIC     FileStatusDesc STRING,
# MAGIC     FileReference STRING,
# MAGIC     FileName STRING,
# MAGIC     FileStatusDate TIMESTAMP,
# MAGIC     ClearingStatus STRING,
# MAGIC     ClearingStatusDesc STRING,
# MAGIC     InstrumentRef STRING,
# MAGIC     SenderInfo STRING,
# MAGIC     Receiver_Information STRING,
# MAGIC     CheckDigit STRING,
# MAGIC     ReceiverName STRING,
# MAGIC     TranRef STRING,
# MAGIC     ClearingDate TIMESTAMP,
# MAGIC     ClearingStatusReason STRING,
# MAGIC     ChequeID BIGINT,
# MAGIC     ChqVisStatus STRING,
# MAGIC     ChqNumber STRING,
# MAGIC     Narration STRING,
# MAGIC     BatchID INT,
# MAGIC     BatchDate TIMESTAMP,
# MAGIC     CreatedOn DATE,
# MAGIC     Updated_On TIMESTAMP,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC ) using Delta;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC WITH TableCounts AS (
# MAGIC     SELECT 'CL_BKCOMPENS_AV' AS TABLENAME, COUNT(*) AS RowCount FROM BronzeLakehouse.CL_BKCOMPENS_AV_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_AV_EVEC', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_EVEC_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_AV_EVE', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_EVE_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_RV_EVE', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_EVE_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_RV_CHQ', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_CHQ_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_RV_ETA', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_ETA_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_RF_ETA', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RF_ETA_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_AV_CHQ', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_CHQ_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_AF', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AF_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_AF_ETA', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AF_ETA_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_RV_TRF', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_TRF_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_ERROR', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_ERROR_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_AV_TRF', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_TRF_20250330 
# MAGIC     UNION ALL
# MAGIC     SELECT 'CL_BKCOMPENS_RF', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RF_20250330
# MAGIC )
# MAGIC SELECT TABLENAME, RowCount FROM TableCounts
# MAGIC UNION ALL
# MAGIC SELECT 'TOTAL' AS TABLENAME, SUM(RowCount) FROM TableCounts;
# MAGIC 
# MAGIC 
# MAGIC -- UNION ALL
# MAGIC -- SELECT 'CL_BKCOMPENS_AV_ETA', COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_ETA_20250330 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM BronzeLakehouse.cl_bkcompens_av_eta_20250330 LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.sql("SHOW TABLES IN BronzeLakehouse").show(1000)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE BronzeLakehouse.bkcompens_rv_TEST
# MAGIC AS
# MAGIC SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_RV;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
