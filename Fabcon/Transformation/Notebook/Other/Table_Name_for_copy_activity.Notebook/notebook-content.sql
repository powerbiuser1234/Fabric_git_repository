-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "e56ddaf5-2e15-4634-91cc-e2eb818afa61",
-- META       "default_lakehouse_name": "Lakehouse",
-- META       "default_lakehouse_workspace_id": "f1bde452-2399-41bc-9ba5-b3c078a190fc",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
-- META         },
-- META         {
-- META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
-- META         },
-- META         {
-- META           "id": "e56ddaf5-2e15-4634-91cc-e2eb818afa61"
-- META         },
-- META         {
-- META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "71772f92-b3a6-8ae5-4b8f-7b5f2ddbf62f",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "71772f92-b3a6-8ae5-4b8f-7b5f2ddbf62f",
-- META           "type": "Datawarehouse"
-- META         },
-- META         {
-- META           "id": "9c1f7c2c-87d6-4314-93d9-63ac013542aa",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### For schema name, table name and column name

-- CELL ********************

'''Created a table with all the tables names that we wanted in bronze lakehouse form source and insert it into lookup in 
z_pip(SRC_to_BronzeLH) pipeline. So all the tables could be inserted at once'''

schema_name = "RAWBK05100"
table_names = [
"BKCOMPENS_AV"	
,"BKCOMPENS_AV_EVEC"	
,"BKCOMPENS_AV_EVE"
,"BKCOMPENS_RV_EVE"
,"BKCOMPENS_RV_CHQ"
,"BKCOMPENS_RV_ETA"
,"BKCOMPENS_RF_ETA"
,"BKCOMPENS_RF"	
,"BKCOMPENS_ERROR"
,"BKCOMPENS_AV_CHQ"
,"BKCOMPENS_AF"	
,"BKCOMPENS_AV_ETA"
,"BKCOMPENS_AF_ETA"
,"BKCOMPENS_RV_TRF"
,"BKCOMPENS_AV_TRF" 
,"BKCOMPENS_RV"
]
column_names=[
    "IDAV, IDFIC, TOPE, ETABD, GUIBD, COMD, CLEB, NOMD, AGEE, NCPE, SUFE, DEVE, DEV, MON, REF, DCOM, ID_ORIG_SENS, ID_ORIG, ZONE, ETA, SYST, NATOPE",
    "TRIM(IDAV) AS IDAV, NORD,TRIM(NAT) AS NAT, TRIM(IDEN) AS IDEN, TRIM(TYPC) AS TYPC, TRIM(DEVR) AS DEVR, MCOMR,TXREF,MCOMC,MCOMN,MCOMT,TRIM(TAX) AS TAX,TCOM",
    "TRIM(IDAV) AS IDAV, NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5,TRIM(FORC) AS FORC, DSAI, TRIM(HSAI) AS HSAI",
    "TRIM(IDRV) AS IDRV,NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5, DSAI,TRIM(HSAI) AS HSAI",
    "IDRVCHQ,TRIM(IDRV) AS IDRV,TRIM(NCHQ) AS NCHQ,  TRIM(CTRLVISU) AS CTRLVISU, TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(SERIALb ) AS SERIAL,  DECH, CMC7",
    IDRVCHQ,TRIM(IDRV) AS IDRV,TRIM(NCHQ) AS NCHQ,  TRIM(CTRLVISU) AS CTRLVISU, TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(SERIALb ) AS SERIAL,  DECH, CMC7
    "TRIM(IDRV) AS IDRV,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT,NORDEVE, TRIM(UTI) AS UTI,DCO, DCOM, DCRE,TRIM(HCRE) AS HCRE",
    "TRIM(IDFIC) AS IDFIC, NORD,TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE, TRIM(HCRE) AS HCRE,DCO, DCOM",
    "TRIM(IDFIC) AS IDFIC, TRIM(NFIC) AS NFIC, TRIM(REF_COM) AS REF_COM,ZONE_ENTETE, ZONE_PIED,TRIM(ETA) AS ETA,TRIM(SYST) AS SYST, TRIM(CYCLE_PRES) AS CYCLE_PRES, TRIM(AGE) AS AGE",
    "TRIM(IDEN) AS IDEN, TRIM(SENS) AS SENS, TRIM(ID) AS ID, TRIM(TYPE) AS TYPE, TRIM(CERR) AS CERR, TRIM(LCERR) AS LCERR, TRIM(PROG) AS PROG, TRIM(MESS) AS MESS,DCRE,TRIM(HCRE) AS HCRE, TRIM(ETA) AS ETA, CONTENU",
    "IDAVCHQ,TRIM(IDAV) AS IDAV, TRIM(IDCHQ) AS IDCHQ, TRIM(NCHQ) AS NCHQ, TRIM(SERIE) AS SERIE, DECH, CMC7",
    "TRIM(IDFIC) AS IDFIC, TRIM(NFIC) AS NFIC, TRIM(REF_COM) AS REF_COM,ZONE_ENTETE, ZONE_PIED, TRIM(ETA) AS ETA,CYCLE_PRES,TRIM(AGE) AS AGE",
    "TRIM(IDAV) AS IDAV, NORD,TRIM(ETA) AS ETA,TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, NORDEVE,TRIM(UTI) AS UTI, DCO, DCOM, DCRE,TRIM(HCRE) AS HCRE",
    "TRIM(IDFIC) AS IDFIC,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE,  TRIM(HCRE) AS HCRE,DCO, DCOM",
    "IDRVTRF, TRIM(IDRV) AS IDRV, TRIM(MOT1) AS MOT1, TRIM(MOT2) AS MOT2, TRIM(MOT3) AS MOT3, TRIM(MOT4) AS MOT4",
    "IDAVTRF,TRIM(IDAV) AS IDAV, TRIM(MOT1) AS MOT1, TRIM(MOT2) AS MOT2, TRIM(MOT3) AS MOT3, TRIM(MOT4) AS MOT4, TRIM(AGEF) AS AGEF, TRIM(NCPF) AS NCPF, TRIM(SUFF) AS SUFF, TRIM(DEVF) AS DEVF",
    "TRIM(IDRV) AS IDRV, TRIM(IDFIC) AS IDFIC,NLIGNE, TRIM(TOPE) AS TOPE, TRIM(EMETTEUR) AS EMETTEUR, TRIM(DESTINATAIRE) AS DESTINATAIRE, TRIM(ETABE) AS ETABE, TRIM(GUIBE) AS GUIBE, TRIM(COME) AS COME, TRIM(CLEE) AS CLEE, TRIM(AGED) AS AGED, TRIM(NCPD) AS NCPD, TRIM(SUFD) AS SUFD, TRIM(DEVD) AS DEVD, TRIM(NOMD) AS NOMD, TRIM(DEV) AS DEV,MON, TRIM(REF) AS REF, DCOM,TRIM(ID_ORIG_SENS) AS ID_ORIG_SENS, TRIM(ID_ORIG) AS ID_ORIG, TRIM(ETA) AS ETA,DREG, NATOPE"

]

 
data = [(schema_name, table_name, col) for table_name, col in zip(table_names, column_names)]
columns = ["schemaName", "tableName", "columnNames"]

df = spark.createDataFrame(data, columns)

# Path for Bronze Lakehouse
bronze_lakehouse_path = "abfss://Dev_Rawbank_Data_Engineering@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/Mtd_Clr_Brn_Tbl_Lst"

# Write DataFrame to Delta format
df.write.format("delta").mode("overwrite").save(bronze_lakehouse_path)

print("Table list with column names successfully written to Bronze Lakehouse!")


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

--  ##### For only Schema and Table name

-- CELL ********************

# Define schema name
schema_name = "RAWBK05100"

# List of table names -- 2 table (bkdetsws,bkdetswe) are not included due to huge counts ingested these 2 table separatly

table_names = [
    "bkentsws", "bkentswe","bkactsws", "bkactswe",
    "bkdopi", "bkdbopi", "bkcorr", "bkhcor", "bkalcetr", "bkregetr",
    "bkargetr", "bkaldetr", "bkcpcorr", "bklgcorr", "bktyoetr", "bkalcopi",
    "bkaldopi", "bkmemopi", "bkibopi", "bkbqiopi", "bkdomopi"
]

# Create data with schema and table names
data = [(schema_name, table_name) for table_name in table_names]
columns = ["schemaName", "tableName"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Path for Bronze Lakehouse
bronze_lakehouse_path = "abfss://Dev_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/Swift_Tbl_list"

# Write DataFrame to Delta format
df.write.format("delta").mode("overwrite").save(bronze_lakehouse_path)

print("Table list with schema name successfully written to Bronze Lakehouse!")


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### Tables Count Check  

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT  'bkentsws' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkentsws UNION ALL
-- MAGIC SELECT  'bkentswe' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkentswe UNION ALL
-- MAGIC SELECT  'bkdetsws' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkdetsws UNION ALL
-- MAGIC SELECT  'bkdetswe' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkdetswe UNION ALL
-- MAGIC SELECT  'bkactsws' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkactsws UNION ALL
-- MAGIC SELECT  'bkactswe' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkactswe UNION ALL
-- MAGIC SELECT  'bkdopi' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkdopi UNION ALL
-- MAGIC SELECT  'bkdbopi' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkdbopi UNION ALL
-- MAGIC SELECT  'bkcorr' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkcorr UNION ALL
-- MAGIC SELECT  'bkhcor' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkhcor UNION ALL
-- MAGIC SELECT  'bkalcetr' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkalcetr UNION ALL
-- MAGIC SELECT  'bkregetr' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkregetr UNION ALL
-- MAGIC SELECT  'bkargetr' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkargetr UNION ALL
-- MAGIC SELECT  'bkaldetr' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkaldetr UNION ALL
-- MAGIC SELECT  'bkcpcorr' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkcpcorr UNION ALL
-- MAGIC SELECT  'bklgcorr' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bklgcorr UNION ALL
-- MAGIC SELECT  'bktyoetr' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bktyoetr UNION ALL
-- MAGIC SELECT  'bkalcopi' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkalcopi UNION ALL
-- MAGIC SELECT  'bkaldopi' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkaldopi UNION ALL
-- MAGIC SELECT  'bkmemopi' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkmemopi UNION ALL
-- MAGIC SELECT  'bkibopi' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkibopi UNION ALL
-- MAGIC SELECT  'bkbqiopi' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkbqiopi UNION ALL
-- MAGIC SELECT  'bkdomopi' AS TABLE_NAME, COUNT(*) AS COUNT FROM BronzeLakehouse.bkdomopi;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT modu,age,ndos,natc,nord
-- MAGIC , COUNT(*) as cnt
-- MAGIC FROM BronzeLakehouse.bkaldopi
-- MAGIC GROUP BY modu,age,ndos,natc,nord
-- MAGIC HAVING COUNT(*) > 1;


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

-- MAGIC %%sql
-- MAGIC CREATE TABLE Mtd_Src_Brn_Tbl_Lst 
-- MAGIC (
-- MAGIC     ID BIGINT,
-- MAGIC     SourceSystem STRING,
-- MAGIC     SchemaName STRING,
-- MAGIC     TableName STRING,
-- MAGIC     ColumnNames STRING,
-- MAGIC     TargetSchema STRING,
-- MAGIC     TargetTable STRING,
-- MAGIC     LoadType STRING,
-- MAGIC     FilterCondition STRING,
-- MAGIC     PartitionColumn STRING,
-- MAGIC     IsActive BOOLEAN,
-- MAGIC     Priority INT,
-- MAGIC     Comments STRING,
-- MAGIC     CreatedDate TIMESTAMP,
-- MAGIC     ModifiedDate TIMESTAMP
-- MAGIC );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC INSERT INTO Mtd_Src_Brn_Tbl_Lst VALUES 
-- MAGIC (1, 'CBS', 'dbo', 'Bknom', 'AGE, CTAB, CACC, LIB1, LIB2, LIB3, LIB4, LIB5, MNT1, MNT2, MNT3, MNT4, MNT5, MNT6, MNT7, MNT8, TAU1, TAU2, TAU3, TAU4, TAU5, DUTI, DDOU, DDMO', 'BronzeLakehouse', 'Bknom', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (2, 'CBS', 'dbo', 'Bkcom', 'CLI, NCP, SUF, AGE, DEV, CHA, CLC, INTI, SER, SBI, CRP, TYP, ARR, ECH, EXT, TAX, CVER, IFE, PSBF, SEUI, CFE, DOU, DMO, DIF, DFE, ORD, PRLIB, SDE, SVA, DVA, SHI, DHI, SAR, DAR, SIN, MIND, MINDS, MINJ, MINJS, DBT, CRT, DDM, DDC, DDD, UTI, PARRD, CTX, FUS_AGE, FUS_DEV, FUS_CHA, FUS_NCP, FUS_SUF, PARRC, CPL, DDL, AUT1, ECA1, DODB2, DAUT2, ECD, CATR, DODB, DAUT, RIBDEC, CPRO, DECH, NANTI, MDCHQ, TYCHQ, AGCHQ, MOTCLO, UTIC, UTIIF, UTIFE, AGECRE, AGERIB, DERNAGE, TADCH, CODADCH, CLEIBAN, OUVP, CPACK, MTFDR, MNTL2, DATL1, DATL2, EXTD, ZONL2, ZONL3, DERNDEV, DATDEV, CPTCOJ, CLIR, LIB, SHI_TA', 'BronzeLakehouse', 'Bkcom', 'Full', '', '', True, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (3, 'CBS', 'dbo', 'Bkcli', 'CLI, NOM, TCLI, LIB, PRE, SEXT, NJF, DNA, VILN, DEPN, PAYN, LOCN, TID, NID, DID, LID, OID, VID, SIT, REG, CAPJ, DCAPJ, SITJ, DSITJ, TCONJ, CONJ, NBENF, CLIFAM, RSO, SIG, DATC, FJU, NRC, VRC, NCHC, NPA, VPA, NIDN, NIS, NIDF, GRP, SGRP, MET, SMET, CMC1, CMC2, AGE, GES, QUA, TAX, CATL, SEG, NST, CLIPAR, CHL1, CHL2, CHL3, LTER, LTERC, RESD, CATN, SEC, LIENBQ, ACLAS, MACLAS, EMTIT, NICR, CED, CLCR, NMER, LANG, NAT, RES, ICHQ, DICHQ, ICB, DICB, EPU, UTIC, UTI, DOU, DMO, ORD, CATR, MIDNAME, NOMREST, DRC, LRC, RSO2, REGN, RRC, DVRRC, UTI_VRRC, IDEXT, SITIMMO, OPETR, FATCA_STATUS, FATCA_DATE, FATCA_UTI, CRS_STATUS, CRS_DATE, CRS_UTI, PRE3', 'BronzeLakehouse', 'Bkcli', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (4, 'CBS', 'dbo', 'Bkgestionnaire', '*', 'BronzeLakehouse', 'Bkgestionnaire', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (5, 'CBS', 'dbo', 'Bktau', 'AGE, DCO, DEV, TAC, TVE, TIND, COURIND', 'BronzeLakehouse', 'Bktau', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (6, 'CBS', 'dbo', 'Bkhis', 'AGE, DEV, NCP, SUF, DCO, OPE, MVT, SER, DVA, DIN, MON, SEN, LIB, EXO, PIE, DES1, DES2, DES3, DES4, DES5, UTI, UTF, UTA, EVE, AGEM, DAG, NCC, SUC, CPL, DDL, RLET, UTL, MAR, DECH, AGSA, AGDE, DEVC, MCTV, PIEO, IDEN, NOSEQ, DEXA, MODU, REFDOS, LABEL, NAT, ETA, SCHEMA, CETICPT, FUSION', 'BronzeLakehouse', 'Bkhis', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (7, 'CBS', 'dbo', 'Bkhisl', 'IDEN, NOSEQ, ORD, LIB, LANG, DAG', 'BronzeLakehouse', 'Bkhisl', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (8, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV', 'IDAV, IDFIC, TOPE, ETABD, GUIBD, COMD, CLEB, NOMD, AGEE, NCPE, SUFE, DEVE, DEV, MON, REF, DCOM, ID_ORIG_SENS, ID_ORIG, ZONE, ETA, SYST, NATOPE', 'BronzeLakehouse', 'BKCOMPENS_AV', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (9, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV_EVEC', 'TRIM(IDAV) AS IDAV, NORD,TRIM(NAT) AS NAT, TRIM(IDEN) AS IDEN, TRIM(TYPC) AS TYPC, TRIM(DEVR) AS DEVR, MCOMR,TXREF,MCOMC,MCOMN,MCOMT,TRIM(TAX) AS TAX,TCOM', 'BronzeLakehouse', 'BKCOMPENS_AV_EVEC', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (10, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV_EVE', 'TRIM(IDAV) AS IDAV, NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5,TRIM(FORC) AS FORC, DSAI, TRIM(HSAI) AS HSAI', 'BronzeLakehouse', 'BKCOMPENS_AV_EVE', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (11, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RV_EVE', 'TRIM(IDRV) AS IDRV,NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5, DSAI,TRIM(HSAI) AS HSAI', 'BronzeLakehouse', 'BKCOMPENS_RV_EVE', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (12, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RV_CHQ', 'IDRVCHQ,TRIM(IDRV) AS IDRV,TRIM(NCHQ) AS NCHQ,  TRIM(CTRLVISU) AS CTRLVISU, TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(SERIALb ) AS SERIAL,  DECH, CMC7', 'BronzeLakehouse', 'BKCOMPENS_RV_CHQ', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (13, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RV_ETA', 'TRIM(IDRV) AS IDRV,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT,NORDEVE, TRIM(UTI) AS UTI,DCO, DCOM, DCRE,TRIM(HCRE) AS HCRE', 'BronzeLakehouse', 'BKCOMPENS_RV_ETA', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (14, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RF_ETA', 'TRIM(IDFIC) AS IDFIC, NORD,TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE, TRIM(HCRE) AS HCRE,DCO, DCOM', 'BronzeLakehouse', 'BKCOMPENS_RF_ETA', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (15, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RF', 'TRIM(IDFIC) AS IDFIC, TRIM(NFIC) AS NFIC, TRIM(REF_COM) AS REF_COM,ZONE_ENTETE, ZONE_PIED,TRIM(ETA) AS ETA,TRIM(SYST) AS SYST, TRIM(CYCLE_PRES) AS CYCLE_PRES, TRIM(AGE) AS AGE', 'BronzeLakehouse', 'BKCOMPENS_RF', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (16, 'Clearing', 'RAWBK05100', 'BKCOMPENS_ERROR', 'TRIM(IDEN) AS IDEN, TRIM(SENS) AS SENS, TRIM(ID) AS ID, TRIM(TYPE) AS TYPE, TRIM(CERR) AS CERR, TRIM(LCERR) AS LCERR, TRIM(PROG) AS PROG, TRIM(MESS) AS MESS,DCRE,TRIM(HCRE) AS HCRE, TRIM(ETA) AS ETA, CONTENU', 'BronzeLakehouse', 'BKCOMPENS_ERROR', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (17, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV_CHQ', 'IDAVCHQ,TRIM(IDAV) AS IDAV, TRIM(IDCHQ) AS IDCHQ, TRIM(NCHQ) AS NCHQ, TRIM(SERIE) AS SERIE, DECH, CMC7', 'BronzeLakehouse', 'BKCOMPENS_AV_CHQ', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (18, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AF', 'TRIM(IDFIC) AS IDFIC, TRIM(NFIC) AS NFIC, TRIM(REF_COM) AS REF_COM,ZONE_ENTETE, ZONE_PIED, TRIM(ETA) AS ETA,CYCLE_PRES,TRIM(AGE) AS AGE', 'BronzeLakehouse', 'BKCOMPENS_AF', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (19, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV_ETA', 'TRIM(IDAV) AS IDAV, NORD,TRIM(ETA) AS ETA,TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, NORDEVE,TRIM(UTI) AS UTI, DCO, DCOM, DCRE,TRIM(HCRE) AS HCRE', 'BronzeLakehouse', 'BKCOMPENS_AV_ETA', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (20, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AF_ETA', 'TRIM(IDFIC) AS IDFIC,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE,  TRIM(HCRE) AS HCRE,DCO, DCOM', 'BronzeLakehouse', 'BKCOMPENS_AF_ETA', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (21, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RV_TRF', 'IDRVTRF, TRIM(IDRV) AS IDRV, TRIM(MOT1) AS MOT1, TRIM(MOT2) AS MOT2, TRIM(MOT3) AS MOT3, TRIM(MOT4) AS MOT4', 'BronzeLakehouse', 'BKCOMPENS_RV_TRF', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (22, 'Clearing', 'RAWBK05100', 'BKCOMPENS_AV_TRF', 'IDAVTRF,TRIM(IDAV) AS IDAV, TRIM(MOT1) AS MOT1, TRIM(MOT2) AS MOT2, TRIM(MOT3) AS MOT3, TRIM(MOT4) AS MOT4, TRIM(AGEF) AS AGEF, TRIM(NCPF) AS NCPF, TRIM(SUFF) AS SUFF, TRIM(DEVF) AS DEVF', 'BronzeLakehouse', 'BKCOMPENS_AV_TRF', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
-- MAGIC (23, 'Clearing', 'RAWBK05100', 'BKCOMPENS_RV', 'TRIM(IDRV) AS IDRV, TRIM(IDFIC) AS IDFIC,NLIGNE, TRIM(TOPE) AS TOPE, TRIM(EMETTEUR) AS EMETTEUR, TRIM(DESTINATAIRE) AS DESTINATAIRE, TRIM(ETABE) AS ETABE, TRIM(GUIBE) AS GUIBE, TRIM(COME) AS COME, TRIM(CLEE) AS CLEE, TRIM(AGED) AS AGED, TRIM(NCPD) AS NCPD, TRIM(SUFD) AS SUFD, TRIM(DEVD) AS DEVD, TRIM(NOMD) AS NOMD, TRIM(DEV) AS DEV,MON, TRIM(REF) AS REF, DCOM,TRIM(ID_ORIG_SENS) AS ID_ORIG_SENS, TRIM(ID_ORIG) AS ID_ORIG, TRIM(ETA) AS ETA,DREG, NATOPE', 'BronzeLakehouse', 'BKCOMPENS_RV', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
-- MAGIC 
-- MAGIC 
-- MAGIC 


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO Mtd_Src_Brn_Tbl_Lst VALUES 
(1, 'CBS', 'dbo', 'Bknom', 'AGE, CTAB, CACC, LIB1, LIB2, LIB3, LIB4, LIB5, MNT1, MNT2, MNT3, MNT4, MNT5, MNT6, MNT7, MNT8, TAU1, TAU2, TAU3, TAU4, TAU5, DUTI, DDOU, DDMO', 'BronzeLakehouse', 'Bknom', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(2, 'CBS', 'dbo', 'Bkcom', 'CLI, NCP, SUF, AGE, DEV, CHA, CLC, INTI, SER, SBI, CRP, TYP, ARR, ECH, EXT, TAX, CVER, IFE, PSBF, SEUI, CFE, DOU, DMO, DIF, DFE, ORD, PRLIB, SDE, SVA, DVA, SHI, DHI, SAR, DAR, SIN, MIND, MINDS, MINJ, MINJS, DBT, CRT, DDM, DDC, DDD, UTI, PARRD, CTX, FUS_AGE, FUS_DEV, FUS_CHA, FUS_NCP, FUS_SUF, PARRC, CPL, DDL, AUT1, ECA1, DODB2, DAUT2, ECD, CATR, DODB, DAUT, RIBDEC, CPRO, DECH, NANTI, MDCHQ, TYCHQ, AGCHQ, MOTCLO, UTIC, UTIIF, UTIFE, AGECRE, AGERIB, DERNAGE, TADCH, CODADCH, CLEIBAN, OUVP, CPACK, MTFDR, MNTL2, DATL1, DATL2, EXTD, ZONL2, ZONL3, DERNDEV, DATDEV, CPTCOJ, CLIR, LIB, SHI_TA', 'BronzeLakehouse', 'Bkcom', 'Full', '', '', True, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(3, 'CBS', 'dbo', 'Bkcli', 'CLI, NOM, TCLI, LIB, PRE, SEXT, NJF, DNA, VILN, DEPN, PAYN, LOCN, TID, NID, DID, LID, OID, VID, SIT, REG, CAPJ, DCAPJ, SITJ, DSITJ, TCONJ, CONJ, NBENF, CLIFAM, RSO, SIG, DATC, FJU, NRC, VRC, NCHC, NPA, VPA, NIDN, NIS, NIDF, GRP, SGRP, MET, SMET, CMC1, CMC2, AGE, GES, QUA, TAX, CATL, SEG, NST, CLIPAR, CHL1, CHL2, CHL3, LTER, LTERC, RESD, CATN, SEC, LIENBQ, ACLAS, MACLAS, EMTIT, NICR, CED, CLCR, NMER, LANG, NAT, RES, ICHQ, DICHQ, ICB, DICB, EPU, UTIC, UTI, DOU, DMO, ORD, CATR, MIDNAME, NOMREST, DRC, LRC, RSO2, REGN, RRC, DVRRC, UTI_VRRC, IDEXT, SITIMMO, OPETR, FATCA_STATUS, FATCA_DATE, FATCA_UTI, CRS_STATUS, CRS_DATE, CRS_UTI, PRE3', 'BronzeLakehouse', 'Bkcli', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(4, 'CBS', 'dbo', 'Bkgestionnaire', '*', 'BronzeLakehouse', 'Bkgestionnaire', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(5, 'CBS', 'dbo', 'Bktau', 'AGE, DCO, DEV, TAC, TVE, TIND, COURIND', 'BronzeLakehouse', 'Bktau', 'Full', '', '', TRUE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(6, 'CBS', 'dbo', 'Bkhis', 'AGE, DEV, NCP, SUF, DCO, OPE, MVT, SER, DVA, DIN, MON, SEN, LIB, EXO, PIE, DES1, DES2, DES3, DES4, DES5, UTI, UTF, UTA, EVE, AGEM, DAG, NCC, SUC, CPL, DDL, RLET, UTL, MAR, DECH, AGSA, AGDE, DEVC, MCTV, PIEO, IDEN, NOSEQ, DEXA, MODU, REFDOS, LABEL, NAT, ETA, SCHEMA, CETICPT, FUSION', 'BronzeLakehouse', 'Bkhis', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(7, 'CBS', 'dbo', 'Bkhisl', 'IDEN, NOSEQ, ORD, LIB, LANG, DAG', 'BronzeLakehouse', 'Bkhisl', 'Full', '', '', FALSE, 1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC describe Mtd_Src_Brn_Tbl_Lst

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE BronzeLakehouseHistory.Cbs_Bkhisl(
    IDEN STRING,
    NOSEQ DECIMAL(10, 0),
    ORD SMALLINT,
    LIB STRING,
    LANG STRING,
    DAG DATE,
    BatchID INT,
    BatchDATE TIMESTAMP,
    SystemCode STRING,
    WorkFlowName STRING
)USING DELTA
PARTITIONED BY (BatchDATE);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select * from CBS_Bkhis

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select count(*), DAG from CBS_Bkhis
-- MAGIC group by DAG
-- MAGIC order by DAG desc

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE CBS_Bkhis (
-- MAGIC     AGE STRING,
-- MAGIC     DEV STRING,
-- MAGIC     NCP STRING,
-- MAGIC     SUF STRING,
-- MAGIC     DCO TIMESTAMP,
-- MAGIC     OPE STRING,
-- MAGIC     MVT STRING,
-- MAGIC     SER STRING,
-- MAGIC     DVA TIMESTAMP,
-- MAGIC     DIN TIMESTAMP,
-- MAGIC     MON DECIMAL(19, 4),
-- MAGIC     SEN STRING,
-- MAGIC     LIB STRING,
-- MAGIC     EXO STRING,
-- MAGIC     PIE STRING,
-- MAGIC     DES1 STRING,
-- MAGIC     DES2 STRING,
-- MAGIC     DES3 STRING,
-- MAGIC     DES4 STRING,
-- MAGIC     DES5 STRING,
-- MAGIC     UTI STRING,
-- MAGIC     UTF STRING,
-- MAGIC     UTA STRING,
-- MAGIC     EVE STRING,
-- MAGIC     AGEM STRING,
-- MAGIC     DAG DATE,
-- MAGIC     NCC STRING,
-- MAGIC     SUC STRING,
-- MAGIC     CPL STRING,
-- MAGIC     DDL TIMESTAMP,
-- MAGIC     RLET STRING,
-- MAGIC     UTL STRING,
-- MAGIC     MAR STRING,
-- MAGIC     DECH TIMESTAMP,
-- MAGIC     AGSA STRING,
-- MAGIC     AGDE STRING,
-- MAGIC     DEVC STRING,
-- MAGIC     MCTV DECIMAL(19, 4),
-- MAGIC     PIEO STRING,
-- MAGIC     IDEN STRING,
-- MAGIC     NOSEQ DECIMAL(10, 0),
-- MAGIC     DEXA TIMESTAMP,
-- MAGIC     MODU STRING,
-- MAGIC     REFDOS STRING,
-- MAGIC     LABEL STRING,
-- MAGIC     NAT STRING,
-- MAGIC     ETA STRING,
-- MAGIC     SCHEMA STRING,
-- MAGIC     CETICPT STRING,
-- MAGIC     FUSION STRING,
-- MAGIC     BATCHID INT,
-- MAGIC     BATCHDATE TIMESTAMP,
-- MAGIC     CREATEDON TIMESTAMP,
-- MAGIC     SYSTEMCODE STRING,
-- MAGIC     WORKFLOWNAME STRING,
-- MAGIC     ROWHASH BINARY
-- MAGIC )USING DELTA
-- MAGIC PARTITIONED BY (DAG)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC insert overwrite CBS_Bkhis partition (DCO)
-- MAGIC select * from CBS_Bkhis_Bkup

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC ALTER TABLE CBS_Bkhis_New RENAME TO CBS_Bkhis;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT count(*), BATCH_DATE, DAG FROM CBS_Bkhisl
-- MAGIC group by BATCH_DATE, DAG
-- MAGIC order by DAG desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select count(*) FROM CBS_Bkhis
-- MAGIC where DCO >= '2023-01-01' and DCO < '2025-04-23' 

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC EXPLAIN FORMATTED
-- MAGIC SELECT * FROM CBS_Bkhis_Delete
-- MAGIC where DCO = '2023-01-02'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC EXPLAIN FORMATTED
-- MAGIC SELECT * FROM CBS_Bkhis
-- MAGIC where DCO = '2023-01-02'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE CBS_Bkprthis(
-- MAGIC     AGE STRING,
-- MAGIC     CLI STRING,
-- MAGIC     EVE STRING,
-- MAGIC     ORD STRING,
-- MAGIC     AVE SMALLINT,
-- MAGIC     NORD INT,
-- MAGIC     DCO DATE,
-- MAGIC     TEXTE STRING,
-- MAGIC     COD_MES STRING,
-- MAGIC     MONTANT DECIMAL(19, 4),
-- MAGIC     DEVMNT STRING,
-- MAGIC     ETAPVAL STRING,
-- MAGIC     UTI STRING,
-- MAGIC     HSAI STRING,
-- MAGIC     ANVA STRING,
-- MAGIC     NOVA STRING,
-- MAGIC     TEXT2 STRING,
-- MAGIC     BatchID BIGINT,
-- MAGIC     BatchDATE TIMESTAMP,
-- MAGIC     SystemCode STRING,
-- MAGIC     WorkflowName STRING
-- MAGIC )USING DELTA
-- MAGIC PARTITIONED BY (DCO)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC DESCRIBE History CBS_Bkhis;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC df = spark.read.format("delta").load("abfss://Dev_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/cbs_bkhis_new")
-- MAGIC 
-- MAGIC df_casted = df \
-- MAGIC     .withColumn("DCO", col("DCO").cast("timestamp")) \
-- MAGIC     .withColumn("DAG", col("DAG").cast("date"))
-- MAGIC 
-- MAGIC df_casted = df_casted.select("AGE","DEV","NCP","SUF","DCO","OPE","MVT","SER","DVA","DIN","MON","SEN","LIB","EXO","PIE","DES1","DES2","DES3","DES4","DES5","UTI","UTF","UTA","EVE","AGEM","DAG","NCC","SUC","CPL","DDL","RLET","UTL","MAR","DECH","AGSA","AGDE","DEVC","MCTV","PIEO","IDEN","NOSEQ","DEXA","MODU","REFDOS","LABEL","NAT","ETA","SCHEMA","CETICPT","FUSION","BATCHID","BATCHDATE","CREATEDON","SYSTEMCODE","WORKFLOWNAME","ROWHASH")
-- MAGIC 
-- MAGIC df_casted.write.format("delta").mode("overwrite").save(
-- MAGIC     "abfss://Dev_Rawbank_DataEng@onelake.dfs.fabric.microsoft.com/BronzeLakehouse.Lakehouse/Tables/CBS_Bkhis"
-- MAGIC )

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select count(*), DCO FROM CBS_Bkprthis
-- MAGIC group by DCO
-- MAGIC order by DCO desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select count(*), DAG FROM CBS_Bkhis
-- MAGIC group by DAG
-- MAGIC order by DAG desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

df = spark.sql("SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_AF LIMIT 1000")
display(df)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_AV' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_AV union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_AV_EVEC' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_AV_EVEC union all    
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_AV_EVE' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_AV_EVE union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_RV_EVE' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_RV_EVE union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_RV_CHQ' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_RV_CHQ union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_RV_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_RV_ETA union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_RF_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_RF_ETA union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_AV_CHQ' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_AV_CHQ union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_AF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_AF union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_AF_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_AF_ETA union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_RV_TRF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_RV_TRF union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_ERROR' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_ERROR union all    
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_AV_TRF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_AV_TRF union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_RF' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_RF union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_RV' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_RV union all  
-- MAGIC SELECT 'Temp1_CL_BKCOMPENS_AV_ETA' AS TABLENAME, COUNT(*) FROM BronzeLakehouse.Temp1_CL_BKCOMPENS_AV_ETA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

df = spark.sql("SELECT * FROM BronzeLakehouse.Temp1_Cl_Bkcompens_Af LIMIT 1000")
display(df)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT count(*), BatchDate, BatchID FROM GoldLakehouse.clearingdim_bkp
-- MAGIC group by BatchDate, BatchID
-- MAGIC order by BatchDate desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC describe history GoldLakehouse.clearingdim_h;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC --delete from GoldLakehouse.clearingDim;
-- MAGIC --delete from GoldLakehouse.clearingDimH;
-- MAGIC --delete from GoldLakehouse.clearingFact;
-- MAGIC --delete from GoldLakehouse.clearingCommFact;
-- MAGIC --delete from GoldLakehouse.clearingErrFact;
-- MAGIC --delete from GoldLakehouse.clearingEventFact;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC insert into GoldLakehouse.ClearingEventFact
-- MAGIC select * from GoldLakehouse.clearingeventfact_bkp;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select count(*) from GoldLakehouse.clearingDim union all
-- MAGIC select count(*) from GoldLakehouse.clearingDim_H union all
-- MAGIC select count(*) from GoldLakehouse.clearingFact union all
-- MAGIC select count(*) from GoldLakehouse.clearingCommFact union all
-- MAGIC select count(*) from GoldLakehouse.clearingErrFact union all
-- MAGIC select count(*) from GoldLakehouse.clearingEventFact;

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

-- CELL ********************

-- MAGIC %%sql
-- MAGIC --insert INTO  DataOpsWarehouse.GoldWareHouse.ClearingDim
-- MAGIC SELECT * FROM GoldLakehouse.ClearingDim

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT * FROM GoldLakehouse.ClearingDim_H
-- MAGIC where BatchDate = '2025-04-29' 
-- MAGIC --and InstrumentRef is not null;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_RF_ETA_V

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
-- MAGIC select count(*), DAG from BronzeLakehouse.CBS_Bkhis
-- MAGIC group by DAG
-- MAGIC order by DAG desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select * from GoldLakehouse.ClearingSK

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC select count(*) from GoldLakehouse.clearingDim Where BatchDate = '2025-04-30' union all
-- MAGIC select count(*) from GoldLakehouse.clearingDim_H Where BatchDate = '2025-04-30' union all
-- MAGIC select count(*) from GoldLakehouse.clearingFact Where BatchDate = '2025-04-30' union all
-- MAGIC select count(*) from GoldLakehouse.clearingCommFact Where BatchDate = '2025-04-30' union all
-- MAGIC select count(*) from GoldLakehouse.clearingErrFact Where BatchDate = '2025-04-30' union all
-- MAGIC select count(*) from GoldLakehouse.clearingEventFact Where BatchDate = '2025-04-30';

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select count(*) from GoldLakehouse.clearingSk
where ClearingID is null
--limit 100

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE BronzeLakehouse.Cbs_Tft_Bonds_Files(
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
	NETIATION_DATE TIMESTAMP,
	VALUE_DATE TIMESTAMP,
	SWIFT_SENDING_DATE TIMESTAMP,
	INITIAL_FILE_NUMBER STRING,
	ISIN_REFERENCE_OF_SECURITY STRING,
	SECURITY_DESIGNATION STRING,
	ISSUER_CODE STRING,
	CURRENCY STRING,
	NOMINAL DECIMAL(19, 4),
	ROUND_LOT_NETIABLE DECIMAL(10, 0),
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
-- MAGIC df_lh  = spark.read.format("delta").table("SilverLakehouse.ClearingDim")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC #Read from warehouse
-- MAGIC df_wh = spark.read.synapsesql("DataOpsWarehouse.GoldWareHouse.ClearingDim_test")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC df_new = spark.sql("""
-- MAGIC MERGE INTO df_wh AS target
-- MAGIC USING df_lh AS source
-- MAGIC ON target.ClearingID = source.ClearingID
-- MAGIC WHEN MATCHED AND target.RowHash <> source.RowHash THEN
-- MAGIC   UPDATE SET
-- MAGIC     target.ClearingID = source.ClearingID,
-- MAGIC     target.ClearingType = source.ClearingType,
-- MAGIC     target.ClearingCode = source.ClearingCode,
-- MAGIC     target.FileBranchCode = source.FileBranchCode,
-- MAGIC     target.FileBranch = source.FileBranch,
-- MAGIC     target.FileCode = source.FileCode,
-- MAGIC     target.FileStatus = source.FileStatus,
-- MAGIC     target.FileStatusDesc = source.FileStatusDesc,
-- MAGIC     target.FileReference = source.FileReference,
-- MAGIC     target.FileName = source.FileName,
-- MAGIC     target.FileStatusDate = source.FileStatusDate,
-- MAGIC     target.ClearingStatus = source.ClearingStatus,
-- MAGIC     target.ClearingStatusDesc = source.ClearingStatusDesc,
-- MAGIC     target.InstrumentRef = source.InstrumentRef,
-- MAGIC     target.SenderInfo = source.SenderInfo,
-- MAGIC     target.Receiver_Information = source.Receiver_Information,
-- MAGIC     target.CheckDigit = source.CheckDigit,
-- MAGIC     target.ReceiverName = source.ReceiverName,
-- MAGIC     target.TranRef = source.TranRef,
-- MAGIC     target.ClearingDate = source.ClearingDate,
-- MAGIC     target.ClearingStatusReason = source.ClearingStatusReason,
-- MAGIC     target.ChequeID = source.ChequeID,
-- MAGIC     target.ChqVisStatus = source.ChqVisStatus,
-- MAGIC     target.ChqNumber = source.ChqNumber,
-- MAGIC     target.Narration = source.Narration,
-- MAGIC     target.BatchID  = source.BatchID,
-- MAGIC     target.BatchDate = source.BatchDate,
-- MAGIC     target.Updated_On = source.BatchDate,
-- MAGIC     target.SystemCode = source.SystemCode,
-- MAGIC     target.RowHash = source.RowHash,
-- MAGIC     target.WorkflowName = source.WorkflowName
-- MAGIC WHEN NOT MATCHED THEN
-- MAGIC   INSERT (
-- MAGIC     ClearingID,
-- MAGIC     ClearingType,
-- MAGIC     ClearingCode,
-- MAGIC     FileBranchCode,
-- MAGIC     FileBranch,
-- MAGIC     FileCode,
-- MAGIC     FileStatus,
-- MAGIC     FileStatusDesc,
-- MAGIC     FileReference,
-- MAGIC     FileName,
-- MAGIC     FileStatusDate,
-- MAGIC     ClearingStatus,
-- MAGIC     ClearingStatusDesc,
-- MAGIC     InstrumentRef,
-- MAGIC     SenderInfo,
-- MAGIC     Receiver_Information,
-- MAGIC     CheckDigit,
-- MAGIC     ReceiverName,
-- MAGIC     TranRef,
-- MAGIC     ClearingDate,
-- MAGIC     ClearingStatusReason,
-- MAGIC     ChequeID,
-- MAGIC     ChqVisStatus,
-- MAGIC     ChqNumber,
-- MAGIC     Narration,
-- MAGIC     BatchID,
-- MAGIC     BatchDate,
-- MAGIC     Updated_On,
-- MAGIC     SystemCode,
-- MAGIC     RowHash,
-- MAGIC     WorkflowName
-- MAGIC   )
-- MAGIC   VALUES (
-- MAGIC      source.ClearingID,
-- MAGIC      source.ClearingType,
-- MAGIC      source.ClearingCode,
-- MAGIC      source.FileBranchCode,
-- MAGIC      source.FileBranch,
-- MAGIC      source.FileCode,
-- MAGIC      source.FileStatus,
-- MAGIC      source.FileStatusDesc,
-- MAGIC      source.FileReference,
-- MAGIC      source.FileName,
-- MAGIC      source.FileStatusDate,
-- MAGIC      source.ClearingStatus,
-- MAGIC      source.ClearingStatusDesc,
-- MAGIC      source.InstrumentRef,
-- MAGIC      source.SenderInfo,
-- MAGIC      source.Receiver_Information,
-- MAGIC      source.CheckDigit,
-- MAGIC      source.ReceiverName,
-- MAGIC      source.TranRef,
-- MAGIC      source.ClearingDate,
-- MAGIC      source.ClearingStatusReason,
-- MAGIC      source.ChequeID,
-- MAGIC      source.ChqVisStatus,
-- MAGIC      source.ChqNumber,
-- MAGIC      source.Narration,
-- MAGIC      source.BatchID,
-- MAGIC      source.BatchDate,
-- MAGIC      source.Updated_On,
-- MAGIC      source.SystemCode,
-- MAGIC      source.RowHash,
-- MAGIC      source.WorkflowName
-- MAGIC   )
-- MAGIC """)


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC df_r = (
-- MAGIC     df
-- MAGIC     .select(
-- MAGIC         df.ClearingID.cast("Integer").alias("ClearingID"),
-- MAGIC         df.ClearingType,
-- MAGIC         df.ClearingCode,
-- MAGIC         df.FileBranchCode,
-- MAGIC         df.FileBranch,
-- MAGIC         df.FileCode,
-- MAGIC         df.FileStatus,
-- MAGIC         df.FileStatusDesc,
-- MAGIC         df.FileReference,
-- MAGIC         df.FileName,
-- MAGIC         df.FileStatusDate,
-- MAGIC         df.ClearingStatus,
-- MAGIC         df.ClearingStatusDesc,
-- MAGIC         df.InstrumentRef,
-- MAGIC         df.SenderInfo,
-- MAGIC         df.ReceiverInformation,
-- MAGIC         df.CheckDigit,
-- MAGIC         df.ReceiverName,
-- MAGIC         df.TranRef,
-- MAGIC         df.ClearingDate,
-- MAGIC         df.ClearingStatusReason,
-- MAGIC         df.ChequeID.cast("Integer").alias("ChequeID"),
-- MAGIC         df.ChqVisStatus,
-- MAGIC         df.ChqNumber,
-- MAGIC         df.Narration,
-- MAGIC         df.BatchID,
-- MAGIC         df.BatchDate,
-- MAGIC         df.CreatedOn,
-- MAGIC         df.UpdatedOn,
-- MAGIC         df.SystemCode,
-- MAGIC         df.RowHash,
-- MAGIC         df.WorkFlowName
-- MAGIC         )
-- MAGIC )

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
-- MAGIC df.write.mode("overwrite").synapsesql("DataOpsWarehouse.GoldWareHouse.ClearingDim_test")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT count(*),BatchDate FROM SilverLakehouse.ClearingDim
-- MAGIC GROUP by BatchDate
-- MAGIC ORDER by BatchDate desc

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

show tables;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.functions import col, when

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC stg_df = spark.read.table("SilverLakehouse.ClearingDim")
-- MAGIC dw_df = spark.read.synapsesql("DataOpsWarehouse.GoldWareHouse.ClearingDim_test")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from delta.tables import DeltaTable
-- MAGIC from pyspark.sql import SparkSession

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Update operation: Update records in DataWarehouse from Staging if they differ based on RowHash
UPDATE DW
SET 
    DW.ClearingID = STG.ClearingID,
    DW.ClearingType = STG.ClearingType,
    DW.ClearingCode = STG.ClearingCode,
    DW.FileBranchCode = STG.FileBranchCode,
    DW.FileBranch = STG.FileBranch,
    DW.FileCode = STG.FileCode,
    DW.FileStatus = STG.FileStatus,
    DW.FileStatusDesc = STG.FileStatusDesc,
    DW.FileReference = STG.FileReference,
    DW.FileName = STG.FileName,
    DW.FileStatusDate = STG.FileStatusDate,
    DW.ClearingStatus = STG.ClearingStatus,
    DW.ClearingStatusDesc = STG.ClearingStatusDesc,
    DW.InstrumentRef = STG.InstrumentRef,
    DW.SenderInfo = STG.SenderInfo,
    DW.ReceiverInformation = STG.ReceiverInformation,
    DW.CheckDigit = STG.CheckDigit,
    DW.ReceiverName = STG.ReceiverName,
    DW.TranRef = STG.TranRef,
    DW.ClearingDate = STG.ClearingDate,
    DW.ClearingStatusReason = STG.ClearingStatusReason,
    DW.ChequeID = STG.ChequeID,
    DW.ChqVisStatus = STG.ChqVisStatus,
    DW.ChqNumber = STG.ChqNumber,
    DW.Narration = STG.Narration,
    DW.BatchID = STG.BatchID,
    DW.BatchDate = STG.BatchDate,
    DW.UpdatedOn = CAST(STG.BatchDate AS DATE),
    DW.SystemCode = STG.SystemCode,
    DW.RowHash = STG.RowHash,
    DW.WorkFlowName = STG.WorkFlowName
FROM [DataOpsWarehouse].[GoldWareHouse].[ClearingDim_test] DW
INNER JOIN [SilverLakehouse].[dbo].[clearingdim] STG 
    ON DW.ClearingType = STG.ClearingType
    AND DW.ClearingCode = STG.ClearingCode
    AND DW.RowHash <> STG.RowHash

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

describe BronzeLakehouse.Cbs_Bkchq

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--drop table BronzeLakehouse.Cbs_Bkchq

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

CREATE TABLE BronzeLakehouse.Cbs_Bkchq(
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

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select count(*), ID from Lakehouse.Cms_Linked_Account
group by ID
having count(*)>1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
