-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "2e2c9b55-5b79-4ca1-880c-bf0b64fd0495",
-- META       "default_lakehouse_name": "GoldLakehouse",
-- META       "default_lakehouse_workspace_id": "197819da-2cec-4c3c-a96a-0fe62ae2300b",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "9c04cb09-56e5-49d6-a2b0-5e2e578f481a"
-- META         },
-- META         {
-- META           "id": "0bba3a6e-afd6-42a2-a721-dff287a67789"
-- META         },
-- META         {
-- META           "id": "2e2c9b55-5b79-4ca1-880c-bf0b64fd0495"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC spark.conf.set("spark.sql.caseSensitive", "true")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE VIEW BronzeLakehouse.Cl_Bkcompens_Av_V as
SELECT TRIM(IDAV) AS IDAV,     TRIM(IDFIC) AS IDFIC,     TRIM(TOPE) AS TOPE,     TRIM(ETABD) AS ETABD,     TRIM(GUIBD) AS GUIBD,     TRIM(COMD) AS COMD,     TRIM(CLEB) AS CLEB,     TRIM(NOMD) AS NOMD,     TRIM(AGEE) AS AGEE,     TRIM(NCPE) AS NCPE,     TRIM(SUFE) AS SUFE,     TRIM(DEVE) AS DEVE,     TRIM(DEV) AS DEV,     MON,     TRIM(REF) AS REF,     DCOM,     TRIM(ID_ORIG_SENS) AS ID_ORIG_SENS,     TRIM(ID_ORIG) AS ID_ORIG,     TRIM(ZONE) AS ZONE,     TRIM(ETA) AS ETA,     TRIM(SYST) AS SYST,     TRIM(NATOPE) AS NATOPE,     BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM BronzeLakehouse.Cl_Bkcompens_Av;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC -- DROP VIEW BronzeLakehouse.Cl_Bkcompens_Af_Eta_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Rv_Trf_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Av_Eve_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Rv_Eve_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Rv_Chq_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Rv_Eta_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Rf_Eta_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Rf_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Error_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Av_Chq_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Af_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Av_Eta_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Av_Evec_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Av_Trf_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Rv_V;
-- MAGIC --DROP VIEW BronzeLakehouse.Cl_Bkcompens_Av_V;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE OR REPLACE VIEW BronzeLakehouse.Cl_Bkcompens_Af_Eta_V as
-- MAGIC select TRIM(IDFIC) AS IDFIC,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE,  TRIM(HCRE) AS HCRE,DCO, DCOM, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM BronzeLakehouse.Cl_Bkcompens_Af_Eta;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC ALTER VIEW BronzeLakehouse.Cl_Bkcompens_Av_Eve_V as
-- MAGIC select TRIM(IDAV) AS IDAV, NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5,TRIM(FORC) AS FORC, DSAI, TRIM(HSAI) AS HSAI, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.Cl_Bkcompens_Av_Eve;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select count(*), BatchDate from SilverLakehouse.clearingFact
group by BatchDate
order by BatchDate desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select count(*), BatchDate from GoldLakehouse.clearingDim group by BatchDate union all
select count(*), BatchDate from GoldLakehouse.clearingDim_H group by BatchDate union all
select count(*), BatchDate from GoldLakehouse.clearingFact group by BatchDate union all
select count(*), BatchDate from GoldLakehouse.clearingCommFact group by BatchDate union all
select count(*), BatchDate from GoldLakehouse.clearingErrFact group by BatchDate union all
select count(*), BatchDate from GoldLakehouse.clearingEventFact group by BatchDate;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC df = spark.read.format("delta").load("abfss://Prod_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/ClearingDim")
-- MAGIC  
-- MAGIC # Write to Test lakehouse path
-- MAGIC df.write.format("delta").mode("overwrite").save("abfss://Dev_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/ClearingDim")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from GoldLakehouse.clearingEventFact
--where ClearingID is not null
limit 100

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--delete from GoldLakehouse.clearingDim;
--delete from GoldLakehouse.clearingDim_H;
--delete from GoldLakehouse.clearingFact;
--delete from GoldLakehouse.clearingCommFact;
--delete from GoldLakehouse.clearingErrFact;
--delete from GoldLakehouse.clearingEventFact;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT count(*), BatchDate FROM SilverLakehouse.ClearingDim
-- MAGIC group by BatchDate
-- MAGIC order by BatchDate desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT count(*), BatchDate FROM GoldLakehouse.ClearingDim
-- MAGIC group by BatchDate
-- MAGIC order by BatchDate desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
