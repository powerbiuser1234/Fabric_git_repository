# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ef516724-eca9-40d5-92bc-40212bb6944e",
# META       "default_lakehouse_name": "GoldLakehouse",
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

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
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

spark.conf.set("spark.sql.caseSensitive", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM SilverLakehouse.ClearingEventFact
# MAGIC -- where ClearingID = '39598'
# MAGIC --group by ClearingID having count(*) > 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT idav, nord, nat, count(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_EVEC 
# MAGIC group by idav, nord, nat having count(*) > 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT ClearingID, Sequence, count(*) FROM SilverLakehouse.ClearingCommFact 
# MAGIC group by ClearingID, Sequence having count(*) > 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DESCRIBE FORMATTED SilverLakehouse.Clearingdim").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE FORMATTED SilverLakehouse.ClearingFact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM GoldLakehouse.ClearingSK LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from GoldLakehouse.ClearingSk

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(distinct(IDRV)) from BronzeLakehouse.CL_BKCOMPENS_RV_V;
# MAGIC 227342 --total
# MAGIC 224310 --after joining
# MAGIC 224310 --after joining and filter

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(distinct(IDRV)) from BronzeLakehouse.CL_BKCOMPENS_RV_V A
# MAGIC inner join GoldLakehouse.Institution on Trim(A.ETABE)=Institution.BankCode AND Trim(A.GUIBE)=Institution.CounterCode and Institution.BranchCode = '05100'; --224310

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(distinct(IDAV)) from BronzeLakehouse.CL_BKCOMPENS_AV_V;
# MAGIC 45612 --total
# MAGIC 224310 --after joining
# MAGIC 224310 --after joining and filter

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(distinct(IDAV)) from BronzeLakehouse.CL_BKCOMPENS_AV_V A
# MAGIC inner join GoldLakehouse.Institution on Trim(A.ETABD)=Institution.BankCode AND Trim(A.GUIBD)=Institution.CounterCode and Institution.BranchCode = '05100';;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from BronzeLakehouse.CL_BKCOMPENS_AV_V A where IDFIC <> '                    '

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM BronzeLakehouse.view1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW BronzeLakehouse.CL_BKCOMPENS_AV_V AS 
# MAGIC SELECT 
# MAGIC     TRIM(IDAV) AS IDAV, TRIM(IDFIC) AS IDFIC, TRIM(TOPE) AS TOPE, TRIM(ETABD) AS ETABD, TRIM(GUIBD) AS GUIBD, TRIM(COMD) AS COMD, TRIM(CLEB) AS CLEB, TRIM(NOMD) AS NOMD, TRIM(AGEE) AS AGEE, TRIM(NCPE) AS NCPE, TRIM(SUFE) AS SUFE, TRIM(DEVE) AS DEVE, TRIM(DEV) AS DEV, TRIM(MON) AS MON, TRIM(REF) AS REF, TRIM(DCOM) AS DCOM, TRIM(ID_ORIG_SENS) AS ID_ORIG_SENS, TRIM(ID_ORIG) AS ID_ORIG, TRIM(ZONE) AS ZONE, TRIM(ETA) AS ETA, TRIM(SYST) AS SYST, TRIM(NATOPE) AS NATOPE 
# MAGIC FROM BronzeLakehouse.CL_BKCOMPENS_AV;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC describe extended BronzeLakehouse.sample_datatypes

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from SilverLakehouse.ClearingErrFact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SilverLakehouse.clearingfact LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC describe extended BronzeLakehouse.CL_BKCOMPENS_AV

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select TXREF from BronzeLakehouse.CL_BKCOMPENS_AV_EVEC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC select * from SilverLakehouse.ClearingCommFact_20250329

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC Create or replace table GoldLakehouse.ClearingCommFact
# MAGIC (
# MAGIC ClearingCode	string,
# MAGIC Sequence	smallint,
# MAGIC ChargeType	string,
# MAGIC ClearingID	bigint,
# MAGIC ClearingType	string,
# MAGIC OperationID	smallint,
# MAGIC InstitutionID	int,
# MAGIC RemitAcc	string,
# MAGIC ReceiptAccID	bigint,
# MAGIC ReceiptBranchID	smallint,
# MAGIC ReceipientCurrID	smallint,
# MAGIC OperationCurrID	smallint,
# MAGIC Amount	decimal(19,4),
# MAGIC TranRef	string,
# MAGIC ClearingDate	int,
# MAGIC TranNatureID	smallint,
# MAGIC ChargeIden	string,
# MAGIC CommType	string,
# MAGIC IsTaxable	string,
# MAGIC TaxRate	decimal(15,8),
# MAGIC AmtTranCurr	decimal(19,4),
# MAGIC AmtLocalCurr	decimal(19,4),
# MAGIC AmtAccCurr	decimal(19,4),
# MAGIC ExchRateBaseCurr	decimal(15,8),
# MAGIC AmtBaseCurr	decimal(19,4),
# MAGIC BatchID	int,
# MAGIC BatchDate	date,
# MAGIC CreatedOn	date,
# MAGIC UpdatedOn	date,
# MAGIC SystemCode	string,
# MAGIC RowHash	binary,
# MAGIC WorkFlowName	string
# MAGIC )


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT ClearingCode, Sequence, count(*) FROM SilverLakehouse.ClearingEventFact group by ClearingCode, Sequence having count(*) > 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Institution = spark.read.format("delta").load("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/23a1ae3b-a1ac-470c-a1d0-f3dcf1363bf3/Tables/Institution")

display(Institution.count())

display(Institution)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from GoldLakehouse.Institution

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from GoldLakehouse.ClearingFact where ClearingType = 'A'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from GoldLakehouse.ClearingFact where ClearingType = 'R'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from GoldLakehouse.ClearingCommFact where ClearingType = 'A';
# MAGIC select ClearingCode, Sequence, ChargeType, count(*) from GoldLakehouse.ClearingCommFact group by ClearingCode, Sequence, ChargeType having count(*) > 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SHOW CREATE TABLE BronzeLakehouse.cl_bkcompens_rv_chq_v;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from GoldLakehouse.Institution

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_AV_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_AV;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_AV_EVEC_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_AV_EVEC;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_AV_EVE_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_AV_EVE;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_RV_EVE_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_RV_EVE;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_RV_CHQ_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_RV_CHQ;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_RV_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_RV;
# MAGIC 
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_RV_ETA_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_RV_ETA; --spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_RF_ETA_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_RF_ETA;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_AV_CHQ_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_AV_CHQ;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_AF_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_AF;
# MAGIC 
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_AV_ETA_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_AV_ETA; --spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_AF_ETA_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_AF_ETA;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_RV_TRF_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_RV_TRF;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_ERROR_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_ERROR;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_AV_TRF_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_AV_TRF;
# MAGIC -- CREATE TABLE BronzeLakehouse.CL_BKCOMPENS_RF_20250330 AS SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_RF;
# MAGIC -- CREATE TABLE SilverLakehouse.ClearingDim_20250330 AS SELECT * FROM SilverLakehouse.ClearingDim;
# MAGIC -- CREATE TABLE SilverLakehouse.ClearingFact_20250330 AS SELECT * FROM SilverLakehouse.ClearingFact;
# MAGIC -- CREATE TABLE SilverLakehouse.ClearingErrFact_20250330 AS SELECT * FROM SilverLakehouse.ClearingErrFact;
# MAGIC -- CREATE TABLE SilverLakehouse.ClearingEventFact_20250330 AS SELECT * FROM SilverLakehouse.ClearingEventFact;
# MAGIC -- CREATE TABLE SilverLakehouse.ClearingCommFact_20250330 AS SELECT * FROM SilverLakehouse.ClearingCommFact;
# MAGIC -- CREATE TABLE GoldLakehouse.ClearingSK_20250330 AS SELECT * FROM GoldLakehouse.ClearingSK;
# MAGIC -- CREATE TABLE GoldLakehouse.ClearingDim_20250330 AS SELECT * FROM GoldLakehouse.ClearingDim;
# MAGIC -- CREATE TABLE GoldLakehouse.ClearingDim_H_20250330 AS SELECT * FROM GoldLakehouse.ClearingDim_H;
# MAGIC -- CREATE TABLE GoldLakehouse.ClearingErrFact_20250330 AS SELECT * FROM GoldLakehouse.ClearingErrFact;
# MAGIC -- CREATE TABLE GoldLakehouse.ClearingEventFact_20250330 AS SELECT * FROM GoldLakehouse.ClearingEventFact;
# MAGIC -- CREATE TABLE GoldLakehouse.ClearingCommFact_20250330 AS SELECT * FROM GoldLakehouse.ClearingCommFact;
# MAGIC -- CREATE TABLE GoldLakehouse.ClearingFact_20250330 AS SELECT * FROM GoldLakehouse.ClearingFact;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SHOW TABLEs in GoldLakehouse;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 'CL_BKCOMPENS_AV' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_AV_EVEC' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_EVEC_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_AV_EVE' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_EVE_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_RV_EVE' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_EVE_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_RV_CHQ' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_CHQ_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_RV' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_RV_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_ETA_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_RF_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RF_ETA_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_AV_CHQ' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_CHQ_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_AF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AF_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_AV_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_ETA_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_AF_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AF_ETA_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_RV_TRF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_TRF_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_ERROR' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_ERROR_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_AV_TRF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_TRF_20250330 union all
# MAGIC SELECT 'CL_BKCOMPENS_RF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RF_20250330;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_AV;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_AV_EVEC;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_AV_EVE;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_RV_EVE;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_RV_CHQ;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_RV;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_RV_ETA;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_RF_ETA;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_AV_CHQ;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_AF;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_AV_ETA;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_AF_ETA;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_RV_TRF;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_ERROR;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_AV_TRF;
# MAGIC select distinct(BatchDate) from BronzeLakehouse.CL_BKCOMPENS_RF;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC describe extended BronzeLakehouse.cl_bkcompens_rv_20250330;

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
# MAGIC 
# MAGIC select * from SilverLakehouse.ClearingDim where ClearingID  = '42852'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select MIN(BatchDate), MAX(BatchDate), count(*) from SilverLakehouse.ClearingCommFact;
# MAGIC select MIN(BatchDate), MAX(BatchDate), count(*) from SilverLakehouse.ClearingDim;
# MAGIC select MIN(BatchDate), MAX(BatchDate), count(*) from SilverLakehouse.ClearingErrFact;
# MAGIC select MIN(BatchDate), MAX(BatchDate), count(*) from SilverLakehouse.ClearingEventFact;
# MAGIC select MIN(BatchDate), MAX(BatchDate), count(*) from SilverLakehouse.ClearingFact;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC VACUUM bronzelakehouse.cl_bkcompens_rv RETAIN 168 HOURS;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from BronzeLakehouse.cl_bkcompens_rv_trf_20250330;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- select distinct(batchDate) from BronzeLakehouse.cl_bkcompens_rv_trf;
# MAGIC DESCRIBE HISTORY BronzeLakehouse.cl_bkcompens_av_trf;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) from GoldLakehouse.ClearingFact_20250330;
# MAGIC select count(*) from GoldLakehouse.ClearingFact;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select 'Slv_ClearingCommFact' as table, count(*) from SilverLakehouse.ClearingCommFact_20250330 union all
# MAGIC select 'Slv_ClearingDim' as table, count(*) from SilverLakehouse.ClearingDim_20250330 union all
# MAGIC select 'Slv_ClearingErrFact' as table, count(*) from SilverLakehouse.ClearingErrFact_20250330 union all
# MAGIC select 'Slv_ClearingEventFact' as table, count(*) from SilverLakehouse.ClearingEventFact_20250330 union all
# MAGIC select 'Slv_ClearingFact' as table, count(*) from SilverLakehouse.ClearingFact_20250330 union all
# MAGIC select 'Gld_ClearingDim' as table, count(*) from GoldLakehouse.ClearingDim_20250330 union all
# MAGIC select 'Gld_ClearingDim_H' as table, count(*) from GoldLakehouse.ClearingDim_H_20250330 union all
# MAGIC select 'Gld_ClearingCommFact' as table, count(*) from GoldLakehouse.ClearingCommFact_20250330 union all
# MAGIC select 'Gld_ClearingErrFact' as table, count(*) from GoldLakehouse.ClearingErrFact_20250330 union all
# MAGIC select 'Gld_ClearingEventFact' as table, count(*) from GoldLakehouse.ClearingEventFact_20250330 union all
# MAGIC select 'Gld_ClearingFact' as table, count(*) from GoldLakehouse.ClearingFact_20250330;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 'CL_BKCOMPENS_AV' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV union all
# MAGIC SELECT 'CL_BKCOMPENS_AV_EVEC' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_EVEC union all
# MAGIC SELECT 'CL_BKCOMPENS_AV_EVE' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_EVE union all
# MAGIC SELECT 'CL_BKCOMPENS_RV_EVE' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_EVE union all
# MAGIC SELECT 'CL_BKCOMPENS_RV_CHQ' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_CHQ union all
# MAGIC SELECT 'CL_BKCOMPENS_RV_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_ETA union all
# MAGIC SELECT 'CL_BKCOMPENS_RF_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RF_ETA union all
# MAGIC SELECT 'CL_BKCOMPENS_AV_CHQ' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_CHQ union all
# MAGIC SELECT 'CL_BKCOMPENS_AF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AF union all
# MAGIC SELECT 'CL_BKCOMPENS_AF_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AF_ETA union all
# MAGIC SELECT 'CL_BKCOMPENS_RV_TRF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV_TRF union all
# MAGIC SELECT 'CL_BKCOMPENS_ERROR' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_ERROR union all
# MAGIC SELECT 'CL_BKCOMPENS_AV_TRF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_TRF union all
# MAGIC SELECT 'CL_BKCOMPENS_RF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RF union all
# MAGIC SELECT 'CL_BKCOMPENS_RV' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_RV union all
# MAGIC SELECT 'CL_BKCOMPENS_AV_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.CL_BKCOMPENS_AV_ETA;
# MAGIC  

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select ClearingType, BatchDate, count(*) from GoldLakehouse.ClearingCommFact group by ClearingType, BatchDate order by ClearingType, BatchDate;
# MAGIC select * from ClearingCommFact where ClearingType = 'A' and BatchDate = '2025-03-29';
# MAGIC --select * from ClearingCommFact where ClearingType = 'R' and BatchDate = '2025-03-29';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select ClearingType, BatchDate, count(*) from GoldLakehouse.ClearingErrFact group by ClearingType, BatchDate order by ClearingType, BatchDate;
# MAGIC --select * from ClearingErrFact where ClearingType = 'A' and BatchDate = '2025-03-29';
# MAGIC select * from ClearingErrFact where ClearingType = 'R' and BatchDate = '2025-03-29';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select ClearingType, BatchDate, count(*) from GoldLakehouse.ClearingEventFact group by ClearingType, BatchDate order by ClearingType, BatchDate;
# MAGIC select * from ClearingEventFact where ClearingType = 'A' and BatchDate = '2025-03-29';
# MAGIC select * from ClearingEventFact where ClearingType = 'R' and BatchDate = '2025-03-29';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select ClearingType, BatchDate, count(*) from GoldLakehouse.ClearingFact group by ClearingType, BatchDate order by ClearingType, BatchDate;
# MAGIC select * from ClearingFact where ClearingType = 'A' and BatchDate = '2025-03-29';
# MAGIC select * from ClearingFact where ClearingType = 'R' and BatchDate = '2025-03-29';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select ClearingType, CreatedOn, UpdatedOn, count(*) from GoldLakehouse.ClearingDim group by ClearingType, CreatedOn, UpdatedOn order by ClearingType, CreatedOn, UpdatedOn;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Create table SilverLakehouse.ClearingDim using delta as select * from SilverLakehouse.ClearingDim_20250330;
# MAGIC -- Create table GoldLakehouse.ClearingDim using delta as select * from GoldLakehouse.ClearingDim_20250330;
# MAGIC -- Create table GoldLakehouse.ClearingDim_H using delta as select * from GoldLakehouse.ClearingDim_H_20250330;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC describe extended GoldLakehouse.ClearingDim_20250330;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SHOW CREATE TABLE BronzeLakehouse.cl_bkcompens_av_v

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC UPDATE GoldLakehouse.ClearingDim_H
# MAGIC SET 
# MAGIC     ClearingCode = TRIM(ClearingCode),
# MAGIC     ClearingStatus = TRIM(ClearingStatus),
# MAGIC     InstrumentRef = TRIM(InstrumentRef),
# MAGIC     CheckDigit = TRIM(CheckDigit),
# MAGIC     ReceiverName = TRIM(ReceiverName),
# MAGIC     TranRef = TRIM(TranRef)
# MAGIC WHERE ClearingType = 'A' and ScdEndDate = '9999-12-31'


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM GoldLakehouse.ClearingDim
# MAGIC WHERE ClearingType = 'A'
# MAGIC AND (
# MAGIC      ClearingCode LIKE ' %' OR ClearingCode LIKE '% ' 
# MAGIC     OR ClearingStatus LIKE ' %' OR ClearingStatus LIKE '% ' 
# MAGIC     OR InstrumentRef LIKE ' %' OR InstrumentRef LIKE '% ' --yes
# MAGIC    OR CheckDigit LIKE ' %' OR CheckDigit LIKE '% '  --yes
# MAGIC     OR ReceiverName LIKE ' %' OR ReceiverName LIKE '% ' --yes
# MAGIC     OR TranRef LIKE ' %' OR TranRef LIKE '% ' --yes
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC     UPDATE GoldLakehouse.ClearingDim_H 
# MAGIC     SET RowHash = sha2(
# MAGIC         concat_ws("", ClearingID, ClearingType, ClearingCode, 
# MAGIC                   FileBranchCode, FileBranch, FileCode, FileStatus, 
# MAGIC                   FileStatusDesc, FileReference, FileName, FileStatusDate, 
# MAGIC                   ClearingStatus, ClearingStatusDesc, InstrumentRef, 
# MAGIC                   SenderInfo, ReceiverInformation, CheckDigit, 
# MAGIC                   ReceiverName, TranRef, ClearingDate, 
# MAGIC                   ClearingStatusReason, ChequeID, ChqVisStatus, 
# MAGIC                   ChqNumber, Narration), 256)
# MAGIC 
# MAGIC     where ClearingType = 'A' and ScdEndDate = '9999-12-31'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- select BatchDate, ScdEndDate, count(*) from GoldLakehouse.ClearingDim_H group by BatchDate, ScdEndDate order by BatchDate, ScdEndDate
# MAGIC select * from GoldLakehouse.ClearingDim_H where ClearingType = 'A' and ScdEndDate = '9999-12-31'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from SilverLakehouse.ClearingDim A
# MAGIC Inner Join GoldLakehouse.ClearingDim B on A.ClearingID = B.ClearingID and A.Rowhash = B.Rowhash
# MAGIC where A.Clearingtype = 'A'


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from GoldLakehouse.ClearingDim A
# MAGIC inner Join GoldLakehouse.ClearingDim_H B on A.ClearingID = B.ClearingID and A.Rowhash = B.Rowhash
# MAGIC where A.ClearingType = 'A' and B.ScdEndDate = '9999-12-31'


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select BatchDate, ClearingType, count(*) from SilverLakehouse.ClearingDim group by Batchdate, ClearingType

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC create table GoldLakehouse.ClearingDim_20250330_02 as select * from GoldLakehouse.ClearingDim;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- select * from GoldLakehouse.ClearingDim_20250330_02 where clearingtype = 'A'; --273101
# MAGIC -- select * from GoldLakehouse.ClearingDim_20250330_02 where clearingtype = 'R'; --265799
# MAGIC 
# MAGIC update GoldLakehouse.ClearingDim_20250330_02
# MAGIC set rowhash = '[100,101,54,54,57,53,51,51,56,56,102,48,49,50,54,56,98,99,51,100,48,102,101,49,102,56,99,48,51,99,99,53,50,55,51,49,99,102,54,53,57,98,50,97,53,50,102,48,102,52,101,99,54,55,48,56,56,99,55,53,49,56,54,100]'
# MAGIC where clearingid in ('273101', '265799')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from GoldLakehouse.ClearingDim_H where updatedon is not null

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- create table BronzeLakehouse.CL_BKCOMPENS_RV_Delete as select * from BronzeLakehouse.CL_BKCOMPENS_RV;
# MAGIC select distinct scdstartdate from clearingdim_h

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- create table BronzeLakehouse.CL_BKCOMPENS_RV_Delete as select * from BronzeLakehouse.CL_BKCOMPENS_RV;
# MAGIC DESCRIBE FORMATTED clearingerrfact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from GoldLakehouse.ClearingDim_20250330 union all
# MAGIC select count(*) from GoldLakehouse.ClearingDim_H_20250330 union all
# MAGIC select count(*) from GoldLakehouse.ClearingCommFact_20250330 union all
# MAGIC select count(*) from GoldLakehouse.ClearingErrFact_20250330 union all
# MAGIC select count(*) from GoldLakehouse.ClearingEventFact_20250330 union all
# MAGIC select count(*) from GoldLakehouse.ClearingFact_20250330;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
