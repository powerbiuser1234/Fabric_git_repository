-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "a28baf80-5d91-4995-9acc-3a8bfa39eed9",
-- META       "default_lakehouse_name": "BronzeLakehouseHistory",
-- META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
-- META         },
-- META         {
-- META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
-- META         },
-- META         {
-- META           "id": "a28baf80-5d91-4995-9acc-3a8bfa39eed9"
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

-- MAGIC %%sql
-- MAGIC SELECT count(*),batchdate FROM BronzeLakehouse.CBS_Bknom
-- MAGIC GROUP by batchdate
-- MAGIC ORDER by batchdate desc

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

describe BronzeLakehouse.CBS_Bkhis

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC df = spark.read.format("delta").load("abfss://Dev_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/Cbs_Bknom")
-- MAGIC  
-- MAGIC # Write to Test lakehouse path
-- MAGIC df.write.format("delta").mode("overwrite").save("abfss://Dev_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/BronzeLakehouseHistory.Lakehouse/Tables/Cbs_Bknom")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

describe BronzeLakehouseHistory.Cbs_Bkchq

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--drop table BronzeLakehouseHistory.Cbs_Bkchq

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

CREATE TABLE BronzeLakehouseHistory.Cbs_Bkchq(
	AGE STRING,
	SERIE STRING,
	NCHE STRING,
	NCP STRING,
	SUF STRING,
	EVE STRING,
	DAR TIMESTAMP,
	SIT STRING,
	TYP STRING,
	ANCSIT STRING,
	MON DECIMAL(19, 4),
	ATRF STRING,
	MBLC DECIMAL(19, 4),
	BatchID INT,
	BatchDATE TIMESTAMP,
	SystemCode STRING,
	WorkFlowName STRING
)
USING DELTA
PARTITIONED BY (BatchDATE);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select count(*), DAG from Lakehouse.Cbs_Bkhis
GROUP by DAG
ORDER by DAG desc;

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

CREATE TABLE Lakehouse.Cbs_Bkprthis(
	AGE STRING,
	CLI STRING,
	EVE STRING,
	ORD STRING,
	AVE SMALLINT,
	NORD INT,
	DCO Date,
	TEXTE STRING,
	COD_MES STRING,
	MONTANT DECIMAL(19, 4),
	DEVMNT STRING,
	ETAPVAL STRING,
	UTI STRING,
	HSAI STRING,
	ANVA STRING,
	NOVA STRING,
	TEXT2 STRING,
	BatchID BIGINT,
	BatchDATE TIMESTAMP,
	SystemCode STRING,
	WorkFlowName STRING
)
USING DELTA
PARTITIONED BY (DCO);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE Lakehouse.Cbs_Bkhis(
	AGE STRING,
	DEV STRING,
	NCP STRING,
	SUF STRING,
	DCO TIMESTAMP,
	OPE STRING,
	MVT STRING,
	SER STRING,
	DVA TIMESTAMP,
	DIN TIMESTAMP,
	MON DECIMAL(19, 4),
	SEN STRING,
	LIB STRING,
	EXO STRING,
	PIE STRING,
	DES1 STRING,
	DES2 STRING,
	DES3 STRING,
	DES4 STRING,
	DES5 STRING,
	UTI STRING,
	UTF STRING,
	UTA STRING,
	EVE STRING,
	AGEM STRING,
	DAG DATE,
	NCC STRING,
	SUC STRING,
	CPL STRING,
	DDL TIMESTAMP,
	RLET STRING,
	UTL STRING,
	MAR STRING,
	DECH TIMESTAMP,
	AGSA STRING,
	AGDE STRING,
	DEVC STRING,
	MCTV DECIMAL(19, 4),
	PIEO STRING,
	IDEN STRING,
	NOSEQ DECIMAL(10, 0),
	DEXA TIMESTAMP,
	MODU STRING,
	REFDOS STRING,
	LABEL STRING,
	NAT STRING,
	ETA STRING,
	SCHEMA STRING,
	CETICPT STRING,
	FUSION STRING,
	BatchID INT,
	BatchDATE TIMESTAMP,
    CREATEDON TIMESTAMP,
	SystemCode STRING,
	WorkFlowName STRING,
    ROWHASH binary
)
USING DELTA
PARTITIONED BY (DAG);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE Lakehouse.Cbs_Bkhisl(
	IDEN STRING,
	NOSEQ DECIMAL(10, 0),
	ORD SMALLINT,
	LIB STRING,
	LANG STRING,
	DAG Date,
	BATCH_ID INT,
	BATCH_DATE TIMESTAMP,
	SYSTEMCODE STRING,
	WORKFLOWNAME STRING
)
USING DELTA
PARTITIONED BY (DAG);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from BronzeLakehouse.Mtd_Src_Brn_Tbl_Lst
order by ID asc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO Mtd_Src_Brn_Tbl_Lst VALUES 
(1, 'CBS', 'dbo', 'Bknom', 'AGE, CTAB, CACC, LIB1, LIB2, LIB3, LIB4, LIB5, MNT1, MNT2, MNT3, MNT4, MNT5, MNT6, MNT7, MNT8, TAU1, TAU2, TAU3, TAU4, TAU5, DUTI, DDOU, DDMO', 'Lakehouse', 'Bknom', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(2, 'CBS', 'dbo', 'Bkcom', 'CLI, NCP, SUF, AGE, DEV, CHA, CLC, INTI, SER, SBI, CRP, TYP, ARR, ECH, EXT, TAX, CVER, IFE, PSBF, SEUI, CFE, DOU, DMO, DIF, DFE, ORD, PRLIB, SDE, SVA, DVA, SHI, DHI, SAR, DAR, SIN, MIND, MINDS, MINJ, MINJS, DBT, CRT, DDM, DDC, DDD, UTI, PARRD, CTX, FUS_AGE, FUS_DEV, FUS_CHA, FUS_NCP, FUS_SUF, PARRC, CPL, DDL, AUT1, ECA1, DODB2, DAUT2, ECD, CATR, DODB, DAUT, RIBDEC, CPRO, DECH, NANTI, MDCHQ, TYCHQ, AGCHQ, MOTCLO, UTIC, UTIIF, UTIFE, AGECRE, AGERIB, DERNAGE, TADCH, CODADCH, CLEIBAN, OUVP, CPACK, MTFDR, MNTL2, DATL1, DATL2, EXTD, ZONL2, ZONL3, DERNDEV, DATDEV, CPTCOJ, CLIR, LIB, SHI_TA', 'Lakehouse', 'Bkcom', 'Full', '', '', True, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(3, 'CBS', 'dbo', 'Bkcli', 'CLI, NOM, TCLI, LIB, PRE, SEXT, NJF, DNA, VILN, DEPN, PAYN, LOCN, TID, NID, DID, LID, OID, VID, SIT, REG, CAPJ, DCAPJ, SITJ, DSITJ, TCONJ, CONJ, NBENF, CLIFAM, RSO, SIG, DATC, FJU, NRC, VRC, NCHC, NPA, VPA, NIDN, NIS, NIDF, GRP, SGRP, MET, SMET, CMC1, CMC2, AGE, GES, QUA, TAX, CATL, SEG, NST, CLIPAR, CHL1, CHL2, CHL3, LTER, LTERC, RESD, CATN, SEC, LIENBQ, ACLAS, MACLAS, EMTIT, NICR, CED, CLCR, NMER, LANG, NAT, RES, ICHQ, DICHQ, ICB, DICB, EPU, UTIC, UTI, DOU, DMO, ORD, CATR, MIDNAME, NOMREST, DRC, LRC, RSO2, REGN, RRC, DVRRC, UTI_VRRC, IDEXT, SITIMMO, OPETR, FATCA_STATUS, FATCA_DATE, FATCA_UTI, CRS_STATUS, CRS_DATE, CRS_UTI, PRE3', 'Lakehouse', 'Bkcli', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(4, 'CBS', 'dbo', 'Bkgestionnaire', '*', 'Lakehouse', 'Bkgestionnaire', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(5, 'CBS', 'dbo', 'Bktau', 'AGE, DCO, DEV, TAC, TVE, TIND, COURIND', 'Lakehouse', 'Bktau', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(6, 'CBS', 'dbo', 'Bkhis', 'AGE, DEV, NCP, SUF, DCO, OPE, MVT, SER, DVA, DIN, MON, SEN, LIB, EXO, PIE, DES1, DES2, DES3, DES4, DES5, UTI, UTF, UTA, EVE, AGEM, DAG, NCC, SUC, CPL, DDL, RLET, UTL, MAR, DECH, AGSA, AGDE, DEVC, MCTV, PIEO, IDEN, NOSEQ, DEXA, MODU, REFDOS, LABEL, NAT, ETA, SCHEMA, CETICPT, FUSION', 'Lakehouse', 'Bkhis', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(7, 'CBS', 'dbo', 'Bkhisl', 'IDEN, NOSEQ, ORD, LIB, LANG, DAG', 'Lakehouse', 'Bkhisl', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC import com.microsoft.spark.fabric
-- MAGIC from com.microsoft.spark.fabric.Constants import Constants
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType, BinaryType, DateType, TimestampType
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from delta.tables import DeltaTable

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC df  = spark.read.format("delta").table("GoldLakehouse.ClearingDim")

-- METADATA ********************

-- META {
-- META   "language": "python",
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

-- MAGIC %%pyspark
-- MAGIC df.write.mode("overwrite").synapsesql("DataOpsWarehouse.GoldWareHouse.ClearingDim")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC drop table BronzeLakehouseHistory.Cbs_Tft_Bonds_Files

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

CREATE TABLE BronzeLakehouseHistory.Cbs_Tft_Bonds_Files(
	FILE_NUMBER STRING,
	FILE_DESCRIPTION STRING,
	MODIF_ORDER_NUMBER INT,
	TEMPLATE STRING,
	TRANSACTION_TYPE STRING,
	FILE_TYPE STRING,
	TRANSACTION_SUB_TYPE STRING,
	TRANSACTION_SIDE STRING,
	STEP_CODE STRING,
	MARKET_CODE STRING,
	PROCESSING_CODE INT,
	CUSTOMER_CODE STRING,
	DEALING_METHOD STRING,
	MODIF_CANCEL_REASON STRING,
	NEGOTIATION_DATE TIMESTAMP,
	VALUE_DATE TIMESTAMP,
	SWIFT_SENDING_DATE TIMESTAMP,
	INITIAL_FILE_NUMBER STRING,
	ISIN_REFERENCE_OF_SECURITY STRING,
	SECURITY_DESIGNATION STRING,
	ISSUER_CODE STRING,
	CURRENCY STRING,
	NOMINAL DECIMAL(19, 4),
	ROUND_LOT_NEGOTIABLE DECIMAL(10, 0),
	INTEREST_RATE DECIMAL(7, 4),
	DATE_OF_ISSUE TIMESTAMP,
	MATURITY_DATE TIMESTAMP,
	REPAYMENT_METHOD STRING,
	REPAYMENT_FREQUENCY SMALLINT,
	INTEREST_PAYMENT_FREQUENCY SMALLINT,
	INTEREST_PAYMENT_DAY SMALLINT,
	INTEREST_CALCULATION_BASE STRING,
	PREVIOUS_INTEREST_PAYMENT_DATE TIMESTAMP,
	NEXT_INTEREST_PAYMENT_DATE TIMESTAMP,
	NOMINAL_VALUE DECIMAL(19, 4),
	FULL_BOND DECIMAL(19, 4),
	PURCHASED_SOLD_SECURITY_VALUE DECIMAL(19, 4),
	GAIN_LOSS_ACHIEVED DECIMAL(19, 4),
	FILE_RATE DECIMAL(9, 7),
	REPAID_SECURITIES DECIMAL(19, 4),
	SOLD_SECURITIES DECIMAL(19, 4),
	CURR_PMT_METHOD STRING,
	CURR_PMT_CORRESP_BIC STRING,
	CURR_PMT_CORRESP_BRANCH STRING,
	CURR_PMT_CORRESP_BANK STRING,
	CURR_PMT_CORRESP_SORT_CODE STRING,
	CURR_PMT_ACCOUNT_BRANCH STRING,
	CURR_PMT_ACCOUNT_CURRENCY STRING,
	CURR_PMT_ACCOUNT STRING,
	CURR_PMT_ACCOUNT_SUFFIX STRING,
	CURR_RCPT_METHOD STRING,
	CURR_RCPT_CORRESP_BIC STRING,
	CURR_RCPT_CORRESP_BRANCH STRING,
	CURR_RCPT_CORRESP_BANK STRING,
	CURR_RCPT_CORRESP_SORT_CODE STRING,
	CURR_RCPT_ACCOUNT_BRANCH STRING,
	CURR_RCPT_ACCOUNT_CURRENCY STRING,
	CURR_RCPT_ACCOUNT STRING,
	CURR_RCPT_ACCOUNT_SUFFIX STRING,
	CUST_PMT_METHOD STRING,
	CUST_PMT_CORRESP_BIC STRING,
	CUST_PMT_CORRESP_BRANCH STRING,
	CUST_PMT_CORRESP_BANK STRING,
	CUST_PMT_CORRESP_SORT_CODE STRING,
	CUST_PMT_ACCOUNT_BRANCH STRING,
	CUST_PMT_ACCOUNT_CURRENCY STRING,
	CUST_PMT_ACCOUNT STRING,
	CUST_PMT_ACCOUNT_SUFFIX STRING,
	CUST_RCPT_METHOD STRING,
	CUST_RCPT_CORRESP_BIC STRING,
	CUST_RCPT_CORRESP_BRANCH STRING,
	CUST_RCPT_CORRESP_BANK STRING,
	CUST_RCPT_CORRESP_SORT_CODE STRING,
	CUST_RCPT_ACCOUNT_BRANCH STRING,
	CUST_RCPT_ACCOUNT_CURRENCY STRING,
	CUST_RCPT_ACCOUNT STRING,
	CUST_RCPT_ACCOUNT_SUFFIX STRING,
	EDIT_USER_CODE STRING,
	EDIT_DATE TIMESTAMP,
	MODIF_USER_CODE STRING,
	MODIF_DATE TIMESTAMP,
	SWIFT_COMMON_REFERENCE STRING,
	BRCODE STRING,
	BRDAPP TIMESTAMP,
	BRMARGIN DECIMAL(8, 6),
	BRACTFREQ SMALLINT,
	BRMETHOD STRING,
	BatchID INT,
	BatchDATE TIMESTAMP,
	SystemCode STRING,
	WorkFlowName STRING
)
USING DELTA
PARTITIONED BY (BatchDATE);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
