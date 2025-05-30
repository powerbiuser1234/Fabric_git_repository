-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "5953e930-bc05-403f-8cad-ba3d204e63d3",
-- META       "default_lakehouse_name": "SilverLakehouse",
-- META       "default_lakehouse_workspace_id": "4f2aaff8-44cb-476d-805b-1c95e486af08",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "7ba102aa-777a-4ad5-8f68-ce62922c90b3"
-- META         },
-- META         {
-- META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
-- META         },
-- META         {
-- META           "id": "5953e930-bc05-403f-8cad-ba3d204e63d3"
-- META         },
-- META         {
-- META           "id": "b554114f-f6f1-42fa-b182-35c9b03d7bfd"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select * from GoldLakehouse.clearingdim_h;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM GoldLakehouse.ClearingDim_H where clearingcode in ('25021114054600481833', '25032811151400490487', '25032810543000490471')
order by clearingcode, scdenddate

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- SHOW TABLES IN BronzeLakehouse
SHOW VIEWS IN BronzeLakehouse;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

# Load from Dev lakehouse path
df = spark.read.format("delta").load("abfss://Dev_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/clearingcommfact")

# Write to Test lakehouse path
df.write.format("delta").mode("append").save("abfss://Test_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/clearingcommfact")


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

show tables GoldLakehouse

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select count(*) from GoldLakehouse.ClearingDim_H

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

# Load from Dev lakehouse path
df = spark.read.format("delta").load("abfss://Dev_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/clearingdim_h")

# Write to Test lakehouse path
df.write.format("delta").mode("append").save("abfss://Test_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/clearingdim_h")


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select * from GoldLakehouse.clearingdim_h
-- MAGIC where ClearingID = 270172

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select count(*), ClearingID from GoldLakehouse.clearingdim_h
-- MAGIC GROUP by ClearingID
-- MAGIC HAVING count(*) > 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE GoldLakehouse.clearingdim_h_bkup (
-- MAGIC ClearingID bigint,
-- MAGIC ClearingType string,
-- MAGIC ClearingCode string,
-- MAGIC FileBranchCode string,
-- MAGIC FileBranch string,
-- MAGIC FileCode string,
-- MAGIC FileStatus string,
-- MAGIC FileStatusDesc string,
-- MAGIC FileReference string,
-- MAGIC FileName string,
-- MAGIC FileStatusDate timestamp,
-- MAGIC ClearingStatus string,
-- MAGIC ClearingStatusDesc string,
-- MAGIC InstrumentRef string,
-- MAGIC SenderInfo string,
-- MAGIC ReceiverInformation string,
-- MAGIC CheckDigit string,
-- MAGIC ReceiverName string,
-- MAGIC TranRef string,
-- MAGIC ClearingDate timestamp,
-- MAGIC ClearingStatusReason string,
-- MAGIC ChequeID bigint,
-- MAGIC ChqVisStatus string,
-- MAGIC ChqNumber string,
-- MAGIC Narration string,
-- MAGIC BatchID int,
-- MAGIC BatchDate date,
-- MAGIC CreatedOn date,
-- MAGIC UpdatedOn date,
-- MAGIC SystemCode string,
-- MAGIC RowHash binary,
-- MAGIC ScdStartDate date,
-- MAGIC ScdEndDate date,
-- MAGIC WorkFlowName string
-- MAGIC );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC truncate GoldLakehouse.clearingdim_h

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC INSERT OVERWRITE GoldLakehouse.clearingdim_h
-- MAGIC SELECT distinct * FROM GoldLakehouse.clearingdim_h_bkup

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select * from GoldLakehouse.clearingdim_h_bkup

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

# Load from Dev lakehouse path
df = spark.read.format("delta").load("abfss://Dev_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/clearingdim_h")

# Write to Test lakehouse path
df.write.format("delta").mode("append").save("abfss://Dev_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables/clearingdim_h")


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

spark.conf.set("spark.sql.caseSensitive", "true")


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--- DDL for cl_bkcompens_af_eta_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Af_Eta_V as
select TRIM(IDFIC) AS IDFIC,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE,  TRIM(HCRE) AS HCRE,DCO, DCOM, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM BronzeLakehouse.Cl_BkCOMPENS_AF_ETA;
--- DDL for cl_bkcompens_rv_trf_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Rv_Trf_V as
select IDRVTRF, TRIM(IDRV) AS IDRV, TRIM(MOT1) AS MOT1, TRIM(MOT2) AS MOT2, TRIM(MOT3) AS MOT3, TRIM(MOT4) AS MOT4, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_RV_TRF;
--- DDL for cl_bkcompens_av_eve_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Av_Eve_V as
select TRIM(IDAV) AS IDAV, NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5,TRIM(FORC) AS FORC, DSAI, TRIM(HSAI) AS HSAI, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_AV_EVE;
--- DDL for cl_bkcompens_rv_eve_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Rv_Eve_V as
select TRIM(IDRV) AS IDRV,NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5, DSAI,TRIM(HSAI) AS HSAI, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_RV_EVE;
--- DDL for cl_bkcompens_rv_chq_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Rv_Chq_V as
select IDRVCHQ,TRIM(IDRV) AS IDRV,TRIM(NCHQ) AS NCHQ,  TRIM(CTRLVISU) AS CTRLVISU, TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(SERIAL) AS SERIAL,  DECH, CMC7, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_RV_CHQ;
--- DDL for cl_bkcompens_rv_eta_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Rv_Eta_V as
select TRIM(IDRV) AS IDRV,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT,NORDEVE, TRIM(UTI) AS UTI,DCO, DCOM, DCRE,TRIM(HCRE) AS HCRE, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_RV_ETA;
--- DDL for cl_bkcompens_rf_eta_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Rf_Eta_V as
select TRIM(IDFIC) AS IDFIC, NORD,TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE, TRIM(HCRE) AS HCRE,DCO, DCOM, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_RF_ETA;
--- DDL for cl_bkcompens_rf_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Rf_V as
select TRIM(IDFIC) AS IDFIC, TRIM(NFIC) AS NFIC, TRIM(REF_COM) AS REF_COM,ZONE_ENTETE, ZONE_PIED,TRIM(ETA) AS ETA,TRIM(SYST) AS SYST, TRIM(CYCLE_PRES) AS CYCLE_PRES, TRIM(AGE) AS AGE, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_RF;
--- DDL for cl_bkcompens_error_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Error_V as
select TRIM(IDEN) AS IDEN, TRIM(SENS) AS SENS, TRIM(ID) AS ID, TRIM(TYPE) AS TYPE, TRIM(CERR) AS CERR, TRIM(LCERR) AS LCERR, TRIM(PROG) AS PROG, TRIM(MESS) AS MESS,DCRE,TRIM(HCRE) AS HCRE, TRIM(ETA) AS ETA, CONTENU, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_ERROR;
--- DDL for cl_bkcompens_av_chq_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Av_Chq_V as
select IDAVCHQ,TRIM(IDAV) AS IDAV, TRIM(IDCHQ) AS IDCHQ, TRIM(NCHQ) AS NCHQ, TRIM(SERIE) AS SERIE, DECH, CMC7, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_AV_CHQ;
--- DDL for cl_bkcompens_af_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Af_V as
select TRIM(IDFIC) AS IDFIC, TRIM(NFIC) AS NFIC, TRIM(REF_COM) AS REF_COM,ZONE_ENTETE, ZONE_PIED, TRIM(ETA) AS ETA,CYCLE_PRES,TRIM(AGE) AS AGE, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_AF;
--- DDL for cl_bkcompens_av_eta_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Av_Eta_V as
select TRIM(IDAV) AS IDAV, NORD,TRIM(ETA) AS ETA,TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, NORDEVE,TRIM(UTI) AS UTI, DCO, DCOM, DCRE,TRIM(HCRE) AS HCRE, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_AV_ETA;
--- DDL for cl_bkcompens_av_evec_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Av_Evec_V as
select TRIM(IDAV) AS IDAV, NORD,TRIM(NAT) AS NAT, TRIM(IDEN) AS IDEN, TRIM(TYPC) AS TYPC, TRIM(DEVR) AS DEVR, MCOMR,TXREF,MCOMC,MCOMN,MCOMT,TRIM(TAX) AS TAX,TCOM, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_AV_EVEC;
--- DDL for cl_bkcompens_av_trf_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Av_Trf_V as
select IDAVTRF,TRIM(IDAV) AS IDAV, TRIM(MOT1) AS MOT1, TRIM(MOT2) AS MOT2, TRIM(MOT3) AS MOT3, TRIM(MOT4) AS MOT4, TRIM(AGEF) AS AGEF, TRIM(NCPF) AS NCPF, TRIM(SUFF) AS SUFF, TRIM(DEVF) AS DEVF, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_AV_TRF;
--- DDL for cl_bkcompens_rv_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Rv_V as
select TRIM(IDRV) AS IDRV, TRIM(IDFIC) AS IDFIC,NLIGNE, TRIM(TOPE) AS TOPE, TRIM(EMETTEUR) AS EMETTEUR, TRIM(DESTINATAIRE) AS DESTINATAIRE, TRIM(ETABE) AS ETABE, TRIM(GUIBE) AS GUIBE, TRIM(COME) AS COME, TRIM(CLEE) AS CLEE, TRIM(AGED) AS AGED, TRIM(NCPD) AS NCPD, TRIM(SUFD) AS SUFD, TRIM(DEVD) AS DEVD, TRIM(NOMD) AS NOMD, TRIM(DEV) AS DEV,MON, TRIM(REF) AS REF, DCOM,TRIM(ID_ORIG_SENS) AS ID_ORIG_SENS, TRIM(ID_ORIG) AS ID_ORIG, TRIM(ETA) AS ETA,DREG, NATOPE, BATCHID, Cast(BATCHDATE as date) as BATCHDATE, SYSTEMCODE, WORKFLOWNAME FROM  BronzeLakehouse.Cl_BkCOMPENS_RV;
--- DDL for cl_bkcompens_av_v

CREATE VIEW BronzeLakehouse.Cl_Bkcompens_Av_V as
SELECT TRIM(IDAV) AS IDAV,     TRIM(IDFIC) AS IDFIC,     TRIM(TOPE) AS TOPE,     TRIM(ETABD) AS ETABD,     TRIM(GUIBD) AS GUIBD,     TRIM(COMD) AS COMD,     TRIM(CLEB) AS CLEB,     TRIM(NOMD) AS NOMD,     TRIM(AGEE) AS AGEE,     TRIM(NCPE) AS NCPE,     TRIM(SUFE) AS SUFE,     TRIM(DEVE) AS DEVE,     TRIM(DEV) AS DEV,     MON,     TRIM(REF) AS REF,     DCOM,     TRIM(ID_ORIG_SENS) AS ID_ORIG_SENS,     TRIM(ID_ORIG) AS ID_ORIG,     TRIM(ZONE) AS ZONE,     TRIM(ETA) AS ETA,     TRIM(SYST) AS SYST,     TRIM(NATOPE) AS NATOPE,     BATCHID,     CAST(BATCHDATE AS DATE) AS BATCHDATE,     SYSTEMCODE,     WORKFLOWNAME FROM BronzeLakehouse.Cl_BkCOMPENS_AV;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC show create table GoldLakehouse.clearingSK

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select count(*), BatchDate from GoldLakehouse.clearingFact
-- MAGIC group by BatchDate
-- MAGIC order by BatchDate desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
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

-- MAGIC %%sql
-- MAGIC CREATE TABLE BronzeLakehouse.Cl_Bkcompens_Av (
-- MAGIC     IDAV STRING,
-- MAGIC     IDFIC STRING,
-- MAGIC     TOPE STRING,
-- MAGIC     ETABD STRING,
-- MAGIC     GUIBD STRING,
-- MAGIC     COMD STRING,
-- MAGIC     CLEB STRING,
-- MAGIC     NOMD STRING,
-- MAGIC     AGEE STRING,
-- MAGIC     NCPE STRING,
-- MAGIC     SUFE STRING,
-- MAGIC     DEVE STRING,
-- MAGIC     DEV STRING,
-- MAGIC     MON DECIMAL(19,4),
-- MAGIC     REF STRING,
-- MAGIC     DCOM TIMESTAMP,
-- MAGIC     ID_ORIG_SENS STRING,
-- MAGIC     ID_ORIG STRING,
-- MAGIC     ZONE STRING,
-- MAGIC     ETA STRING,
-- MAGIC     SYST STRING,
-- MAGIC     NATOPE STRING,
-- MAGIC     BATCHID INT,
-- MAGIC     BATCHDATE TIMESTAMP,
-- MAGIC     SYSTEMCODE STRING,
-- MAGIC     WORKFLOWNAME STRING
-- MAGIC )

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
