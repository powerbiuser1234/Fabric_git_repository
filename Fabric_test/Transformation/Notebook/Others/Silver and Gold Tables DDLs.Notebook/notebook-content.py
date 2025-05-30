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
# META           "id": "7ba102aa-777a-4ad5-8f68-ce62922c90b3"
# META         },
# META         {
# META           "id": "5953e930-bc05-403f-8cad-ba3d204e63d3"
# META         },
# META         {
# META           "id": "b554114f-f6f1-42fa-b182-35c9b03d7bfd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE SilverLakehouse.ClearingFact (
# MAGIC     ClearingCode STRING,
# MAGIC     ClearingID BIGINT,
# MAGIC     ClearingType STRING,
# MAGIC     SettleDate INT,
# MAGIC     OperationID SMALLINT,
# MAGIC     InstitutionID INT,
# MAGIC     RemitAcc STRING,
# MAGIC     ReceiptAccID BIGINT,
# MAGIC     ReceiptBranchID SMALLINT,
# MAGIC     RecipientCurrID SMALLINT,
# MAGIC     OperationCurrID SMALLINT,
# MAGIC     Amount DECIMAL(19,4),
# MAGIC     TranRef STRING,
# MAGIC     ClearingDate INT,
# MAGIC     BatchID INT,
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE SilverLakehouse.ClearingEventFact (
# MAGIC     ClearingCode STRING,
# MAGIC     Sequence SMALLINT,
# MAGIC     ClearingID BIGINT,
# MAGIC     ClearingType STRING,
# MAGIC     SettleDate INT,
# MAGIC     TOperationID SMALLINT,
# MAGIC     InstitutionID INT,
# MAGIC     RemitAcc STRING,
# MAGIC     ReceiptAccID BIGINT,
# MAGIC     ReceiptBranchID SMALLINT,
# MAGIC     RecipientCurrID SMALLINT,
# MAGIC     OperationCurrID SMALLINT,
# MAGIC     Amount DECIMAL(19,4),
# MAGIC     TranRef STRING,
# MAGIC     ClearingDate INT,
# MAGIC     EventID STRING,
# MAGIC     TranNatureID SMALLINT,
# MAGIC     OperationID INT,
# MAGIC     EventDate INT,
# MAGIC     BatchID INT,
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC )

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
# MAGIC     ReceiverInformation STRING,
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
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE SilverLakehouse.ClearingCommFact (
# MAGIC     ClearingCode STRING,
# MAGIC     Sequence SMALLINT,
# MAGIC     ChargeType STRING,
# MAGIC     ClearingID BIGINT,
# MAGIC     ClearingType STRING,
# MAGIC     OperationID SMALLINT,
# MAGIC     InstitutionID INT,
# MAGIC     RemitAcc STRING,
# MAGIC     ReceiptAccID BIGINT,
# MAGIC     ReceiptBranchID SMALLINT,
# MAGIC     ReceipientCurrID SMALLINT,
# MAGIC     OperationCurrID SMALLINT,
# MAGIC     Amount DECIMAL(19,4),
# MAGIC     TranRef STRING,
# MAGIC     ClearingDate INT,
# MAGIC     TranNatureID SMALLINT,
# MAGIC     ChargeIden STRING,
# MAGIC     CommType STRING,
# MAGIC     IsTaxable STRING,
# MAGIC     TaxRate DECIMAL(15,8),
# MAGIC     AmtTranCurr DECIMAL(19,4),
# MAGIC     AmtLocalCurr DECIMAL(19,4),
# MAGIC     AmtAccCurr DECIMAL(19,4),
# MAGIC     ExchRateBaseCurr DECIMAL(15,8),
# MAGIC     AmtBaseCurr DECIMAL(19,4),
# MAGIC     BatchID INT,
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE SilverLakehouse.ClearingErrFact(
# MAGIC     ClearingErrCode STRING,
# MAGIC     ClearingID BIGINT,
# MAGIC     ClearingType STRING,
# MAGIC     SettleDate INT,
# MAGIC     OperationID SMALLINT,
# MAGIC     InstitutionID INT,
# MAGIC     RemitAcc STRING,
# MAGIC     ReceiptAccID BIGINT,
# MAGIC     ReceiptBranchID INT,
# MAGIC     RecipientCurrID SMALLINT,
# MAGIC     OperationCurrID SMALLINT,
# MAGIC     Amount DECIMAL(19,4),
# MAGIC     TranRef STRING,
# MAGIC     ClearingDate INT,
# MAGIC     ErrorType STRING,
# MAGIC     Program STRING,
# MAGIC     ShortErrCode STRING,
# MAGIC     LongErrCode STRING,
# MAGIC     ErrMessage STRING,
# MAGIC     ErrorStatus STRING,
# MAGIC     ErrorDate INT,
# MAGIC     ErrorTime STRING,
# MAGIC     BatchID INT,
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE GoldLakehouse.ClearingFact (
# MAGIC     ClearingCode STRING,
# MAGIC     ClearingID BIGINT,
# MAGIC     ClearingType STRING,
# MAGIC     SettleDate INT,
# MAGIC     OperationID SMALLINT,
# MAGIC     InstitutionID INT,
# MAGIC     RemitAcc STRING,
# MAGIC     ReceiptAccID BIGINT,
# MAGIC     ReceiptBranchID SMALLINT,
# MAGIC     RecipientCurrID SMALLINT,
# MAGIC     OperationCurrID SMALLINT,
# MAGIC     Amount DECIMAL(19,4),
# MAGIC     TranRef STRING,
# MAGIC     ClearingDate INT,
# MAGIC     BatchID INT,
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC );
# MAGIC CREATE TABLE GoldLakehouse.ClearingEventFact (
# MAGIC     ClearingCode STRING,
# MAGIC     Sequence SMALLINT,
# MAGIC     ClearingID BIGINT,
# MAGIC     ClearingType STRING,
# MAGIC     SettleDate INT,
# MAGIC     TOperationID SMALLINT,
# MAGIC     InstitutionID INT,
# MAGIC     RemitAcc STRING,
# MAGIC     ReceiptAccID BIGINT,
# MAGIC     ReceiptBranchID SMALLINT,
# MAGIC     RecipientCurrID SMALLINT,
# MAGIC     OperationCurrID SMALLINT,
# MAGIC     Amount DECIMAL(19,4),
# MAGIC     TranRef STRING,
# MAGIC     ClearingDate INT,
# MAGIC     EventID STRING,
# MAGIC     TranNatureID SMALLINT,
# MAGIC     OperationID INT,
# MAGIC     EventDate INT,
# MAGIC     BatchID INT,
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC );
# MAGIC CREATE TABLE GoldLakehouse.ClearingDim (
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
# MAGIC     ReceiverInformation STRING,
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
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC );
# MAGIC CREATE TABLE GoldLakehouse.ClearingCommFact (
# MAGIC     ClearingCode STRING,
# MAGIC     Sequence SMALLINT,
# MAGIC     ChargeType STRING,
# MAGIC     ClearingID BIGINT,
# MAGIC     ClearingType STRING,
# MAGIC     OperationID SMALLINT,
# MAGIC     InstitutionID INT,
# MAGIC     RemitAcc STRING,
# MAGIC     ReceiptAccID BIGINT,
# MAGIC     ReceiptBranchID SMALLINT,
# MAGIC     ReceipientCurrID SMALLINT,
# MAGIC     OperationCurrID SMALLINT,
# MAGIC     Amount DECIMAL(19,4),
# MAGIC     TranRef STRING,
# MAGIC     ClearingDate INT,
# MAGIC     TranNatureID SMALLINT,
# MAGIC     ChargeIden STRING,
# MAGIC     CommType STRING,
# MAGIC     IsTaxable STRING,
# MAGIC     TaxRate DECIMAL(15,8),
# MAGIC     AmtTranCurr DECIMAL(19,4),
# MAGIC     AmtLocalCurr DECIMAL(19,4),
# MAGIC     AmtAccCurr DECIMAL(19,4),
# MAGIC     ExchRateBaseCurr DECIMAL(15,8),
# MAGIC     AmtBaseCurr DECIMAL(19,4),
# MAGIC     BatchID INT,
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC );
# MAGIC CREATE TABLE GoldLakehouse.ClearingErrFact (
# MAGIC     ClearingErrCode STRING,
# MAGIC     ClearingID BIGINT,
# MAGIC     ClearingType STRING,
# MAGIC     SettleDate INT,
# MAGIC     OperationID SMALLINT,
# MAGIC     InstitutionID INT,
# MAGIC     RemitAcc STRING,
# MAGIC     ReceiptAccID BIGINT,
# MAGIC     ReceiptBranchID INT,
# MAGIC     RecipientCurrID SMALLINT,
# MAGIC     OperationCurrID SMALLINT,
# MAGIC     Amount DECIMAL(19,4),
# MAGIC     TranRef STRING,
# MAGIC     ClearingDate INT,
# MAGIC     ErrorType STRING,
# MAGIC     Program STRING,
# MAGIC     ShortErrCode STRING,
# MAGIC     LongErrCode STRING,
# MAGIC     ErrMessage STRING,
# MAGIC     ErrorStatus STRING,
# MAGIC     ErrorDate INT,
# MAGIC     ErrorTime STRING,
# MAGIC     BatchID INT,
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     WorkFlowName STRING
# MAGIC );
# MAGIC CREATE TABLE GoldLakehouse.ClearingDim_H(
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
# MAGIC     ReceiverInformation STRING,
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
# MAGIC     BatchDate DATE,
# MAGIC     CreatedOn DATE,
# MAGIC     UpdatedOn DATE,
# MAGIC     SystemCode STRING,
# MAGIC     RowHash BINARY,
# MAGIC     ScdStartDate DATE,
# MAGIC     ScdEndDate DATE,
# MAGIC     WorkFlowName STRING
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE BronzeLakehouse.Mtd_Src_Brn_Tbl_Lst 
# MAGIC (
# MAGIC     ID BIGINT,
# MAGIC     SourceSystem STRING,
# MAGIC     SchemaName STRING,
# MAGIC     TableName STRING,
# MAGIC     ColumnNames STRING,
# MAGIC     TargetSchema STRING,
# MAGIC     TargetTable STRING,
# MAGIC     LoadType STRING,
# MAGIC     FilterCondition STRING,
# MAGIC     PartitionColumn STRING,
# MAGIC     IsActive BOOLEAN,
# MAGIC     Priority INT,
# MAGIC     Comments STRING,
# MAGIC     CreatedDate TIMESTAMP,
# MAGIC     ModifiedDate TIMESTAMP
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO Mtd_Src_Brn_Tbl_Lst VALUES 
# MAGIC (1, 'CBS', 'dbo', 'Bknom', 'AGE, CTAB, CACC, LIB1, LIB2, LIB3, LIB4, LIB5, MNT1, MNT2, MNT3, MNT4, MNT5, MNT6, MNT7, MNT8, TAU1, TAU2, TAU3, TAU4, TAU5, DUTI, DDOU, DDMO', 'BronzeLakehouse', 'Bknom', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (2, 'CBS', 'dbo', 'Bkcom', 'CLI, NCP, SUF, AGE, DEV, CHA, CLC, INTI, SER, SBI, CRP, TYP, ARR, ECH, EXT, TAX, CVER, IFE, PSBF, SEUI, CFE, DOU, DMO, DIF, DFE, ORD, PRLIB, SDE, SVA, DVA, SHI, DHI, SAR, DAR, SIN, MIND, MINDS, MINJ, MINJS, DBT, CRT, DDM, DDC, DDD, UTI, PARRD, CTX, FUS_AGE, FUS_DEV, FUS_CHA, FUS_NCP, FUS_SUF, PARRC, CPL, DDL, AUT1, ECA1, DODB2, DAUT2, ECD, CATR, DODB, DAUT, RIBDEC, CPRO, DECH, NANTI, MDCHQ, TYCHQ, AGCHQ, MOTCLO, UTIC, UTIIF, UTIFE, AGECRE, AGERIB, DERNAGE, TADCH, CODADCH, CLEIBAN, OUVP, CPACK, MTFDR, MNTL2, DATL1, DATL2, EXTD, ZONL2, ZONL3, DERNDEV, DATDEV, CPTCOJ, CLIR, LIB, SHI_TA', 'BronzeLakehouse', 'Bkcom', 'Full', '', '', True, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (3, 'CBS', 'dbo', 'Bkcli', 'CLI, NOM, TCLI, LIB, PRE, SEXT, NJF, DNA, VILN, DEPN, PAYN, LOCN, TID, NID, DID, LID, OID, VID, SIT, REG, CAPJ, DCAPJ, SITJ, DSITJ, TCONJ, CONJ, NBENF, CLIFAM, RSO, SIG, DATC, FJU, NRC, VRC, NCHC, NPA, VPA, NIDN, NIS, NIDF, GRP, SGRP, MET, SMET, CMC1, CMC2, AGE, GES, QUA, TAX, CATL, SEG, NST, CLIPAR, CHL1, CHL2, CHL3, LTER, LTERC, RESD, CATN, SEC, LIENBQ, ACLAS, MACLAS, EMTIT, NICR, CED, CLCR, NMER, LANG, NAT, RES, ICHQ, DICHQ, ICB, DICB, EPU, UTIC, UTI, DOU, DMO, ORD, CATR, MIDNAME, NOMREST, DRC, LRC, RSO2, REGN, RRC, DVRRC, UTI_VRRC, IDEXT, SITIMMO, OPETR, FATCA_STATUS, FATCA_DATE, FATCA_UTI, CRS_STATUS, CRS_DATE, CRS_UTI, PRE3', 'BronzeLakehouse', 'Bkcli', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (4, 'CBS', 'dbo', 'Bkgestionnaire', '*', 'BronzeLakehouse', 'Bkgestionnaire', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (5, 'CBS', 'dbo', 'Bktau', 'AGE, DCO, DEV, TAC, TVE, TIND, COURIND', 'BronzeLakehouse', 'Bktau', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (6, 'CBS', 'dbo', 'Bkhis', 'AGE, DEV, NCP, SUF, DCO, OPE, MVT, SER, DVA, DIN, MON, SEN, LIB, EXO, PIE, DES1, DES2, DES3, DES4, DES5, UTI, UTF, UTA, EVE, AGEM, DAG, NCC, SUC, CPL, DDL, RLET, UTL, MAR, DECH, AGSA, AGDE, DEVC, MCTV, PIEO, IDEN, NOSEQ, DEXA, MODU, REFDOS, LABEL, NAT, ETA, SCHEMA, CETICPT, FUSION', 'BronzeLakehouse', 'Bkhis', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (7, 'CBS', 'dbo', 'Bkhisl', 'IDEN, NOSEQ, ORD, LIB, LANG, DAG', 'BronzeLakehouse', 'Bkhisl', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (8, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV', 'IDAV, IDFIC, TOPE, ETABD, GUIBD, COMD, CLEB, NOMD, AGEE, NCPE, SUFE, DEVE, DEV, MON, REF, DCOM, ID_ORIG_SENS, ID_ORIG, ZONE, ETA, SYST, NATOPE', 'BronzeLakehouse', 'BKCOMPENS_AV', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (9, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV_EVEC', 'TRIM(IDAV) AS IDAV, NORD,TRIM(NAT) AS NAT, TRIM(IDEN) AS IDEN, TRIM(TYPC) AS TYPC, TRIM(DEVR) AS DEVR, MCOMR,TXREF,MCOMC,MCOMN,MCOMT,TRIM(TAX) AS TAX,TCOM', 'BronzeLakehouse', 'BKCOMPENS_AV_EVEC', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (10, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV_EVE', 'TRIM(IDAV) AS IDAV, NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5,TRIM(FORC) AS FORC, DSAI, TRIM(HSAI) AS HSAI', 'BronzeLakehouse', 'BKCOMPENS_AV_EVE', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (11, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RV_EVE', 'TRIM(IDRV) AS IDRV,NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5, DSAI,TRIM(HSAI) AS HSAI', 'BronzeLakehouse', 'BKCOMPENS_RV_EVE', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (12, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RV_CHQ', 'IDRVCHQ,TRIM(IDRV) AS IDRV,TRIM(NCHQ) AS NCHQ,  TRIM(CTRLVISU) AS CTRLVISU, TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(SERIAL ) AS SERIAL,  DECH, CMC7', 'BronzeLakehouse', 'BKCOMPENS_RV_CHQ', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (13, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RV_ETA', 'TRIM(IDRV) AS IDRV,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT,NORDEVE, TRIM(UTI) AS UTI,DCO, DCOM, DCRE,TRIM(HCRE) AS HCRE', 'BronzeLakehouse', 'BKCOMPENS_RV_ETA', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (14, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RF_ETA', 'TRIM(IDFIC) AS IDFIC, NORD,TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE, TRIM(HCRE) AS HCRE,DCO, DCOM', 'BronzeLakehouse', 'BKCOMPENS_RF_ETA', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (15, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RF', 'TRIM(IDFIC) AS IDFIC, TRIM(NFIC) AS NFIC, TRIM(REF_COM) AS REF_COM,ZONE_ENTETE, ZONE_PIED,TRIM(ETA) AS ETA,TRIM(SYST) AS SYST, TRIM(CYCLE_PRES) AS CYCLE_PRES, TRIM(AGE) AS AGE', 'BronzeLakehouse', 'BKCOMPENS_RF', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (16, 'Clearing', 'RAWBK05100', 'BKCOMPENS_ERROR', 'TRIM(IDEN) AS IDEN, TRIM(SENS) AS SENS, TRIM(ID) AS ID, TRIM(TYPE) AS TYPE, TRIM(CERR) AS CERR, TRIM(LCERR) AS LCERR, TRIM(PROG) AS PROG, TRIM(MESS) AS MESS,DCRE,TRIM(HCRE) AS HCRE, TRIM(ETA) AS ETA, CONTENU', 'BronzeLakehouse', 'BKCOMPENS_ERROR', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (17, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV_CHQ', 'IDAVCHQ,TRIM(IDAV) AS IDAV, TRIM(IDCHQ) AS IDCHQ, TRIM(NCHQ) AS NCHQ, TRIM(SERIE) AS SERIE, DECH, CMC7', 'BronzeLakehouse', 'BKCOMPENS_AV_CHQ', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (18, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AF', 'TRIM(IDFIC) AS IDFIC, TRIM(NFIC) AS NFIC, TRIM(REF_COM) AS REF_COM,ZONE_ENTETE, ZONE_PIED, TRIM(ETA) AS ETA,CYCLE_PRES,TRIM(AGE) AS AGE', 'BronzeLakehouse', 'BKCOMPENS_AF', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (19, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV_ETA', 'TRIM(IDAV) AS IDAV, NORD,TRIM(ETA) AS ETA,TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, NORDEVE,TRIM(UTI) AS UTI, DCO, DCOM, DCRE,TRIM(HCRE) AS HCRE', 'BronzeLakehouse', 'BKCOMPENS_AV_ETA', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (20, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AF_ETA', 'TRIM(IDFIC) AS IDFIC,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE,  TRIM(HCRE) AS HCRE,DCO, DCOM', 'BronzeLakehouse', 'BKCOMPENS_AF_ETA', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (21, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RV_TRF', 'IDRVTRF, TRIM(IDRV) AS IDRV, TRIM(MOT1) AS MOT1, TRIM(MOT2) AS MOT2, TRIM(MOT3) AS MOT3, TRIM(MOT4) AS MOT4', 'BronzeLakehouse', 'BKCOMPENS_RV_TRF', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (22, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV_TRF', 'IDAVTRF,TRIM(IDAV) AS IDAV, TRIM(MOT1) AS MOT1, TRIM(MOT2) AS MOT2, TRIM(MOT3) AS MOT3, TRIM(MOT4) AS MOT4, TRIM(AGEF) AS AGEF, TRIM(NCPF) AS NCPF, TRIM(SUFF) AS SUFF, TRIM(DEVF) AS DEVF', 'BronzeLakehouse', 'BKCOMPENS_AV_TRF', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (23, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RV', 'TRIM(IDRV) AS IDRV, TRIM(IDFIC) AS IDFIC,NLIGNE, TRIM(TOPE) AS TOPE, TRIM(EMETTEUR) AS EMETTEUR, TRIM(DESTINATAIRE) AS DESTINATAIRE, TRIM(ETABE) AS ETABE, TRIM(GUIBE) AS GUIBE, TRIM(COME) AS COME, TRIM(CLEE) AS CLEE, TRIM(AGED) AS AGED, TRIM(NCPD) AS NCPD, TRIM(SUFD) AS SUFD, TRIM(DEVD) AS DEVD, TRIM(NOMD) AS NOMD, TRIM(DEV) AS DEV,MON, TRIM(REF) AS REF, DCOM,TRIM(ID_ORIG_SENS) AS ID_ORIG_SENS, TRIM(ID_ORIG) AS ID_ORIG, TRIM(ETA) AS ETA,DREG, NATOPE', 'BronzeLakehouse', 'BKCOMPENS_RV', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC 
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
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


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) from GoldLakehouse.ClearingCommFact 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
