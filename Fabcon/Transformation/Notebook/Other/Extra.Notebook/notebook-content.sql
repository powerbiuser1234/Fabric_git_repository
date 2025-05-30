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
-- META           "id": "a28baf80-5d91-4995-9acc-3a8bfa39eed9"
-- META         },
-- META         {
-- META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
-- META         },
-- META         {
-- META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
-- META         },
-- META         {
-- META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE VIEW BronzeLakehouse.BKDETSWS_V AS select * from BronzeLakehouse.BKDETSWS

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

show TABLES IN BronzeLakehouse

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

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select batchdate, CreatedOn, UpdatedOn, count(*) from GoldLakehouse.ClearingDim
group by batchdate, CreatedOn, UpdatedOn
order by batchdate, CreatedOn, UpdatedOn

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select BatchDate, ScdStartDate, ScdEndDate, count(*) from GoldLakehouse.ClearingDim_H
group by BatchDate, ScdStartDate, ScdEndDate
order by BatchDate, ScdStartDate, ScdEndDate;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from GoldLakehouse.ClearingDim_H where ClearingCode = '25032810543000490471'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DESCRIBE HISTORY bronzelakehouse.cl_bkcompens_rv_trf

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC show create table bronzelakehouse.cl_bkcompens_af_eta_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_rv_trf_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_av_eve_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_rv_eve_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_rv_chq_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_rv_eta_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_rf_eta_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_rf_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_error_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_av_chq_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_af_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_av_eta_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_av_evec_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_av_trf_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_rv_v;
-- MAGIC show create table bronzelakehouse.cl_bkcompens_av_v;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM BronzeLakehouse.Mtd_Src_Brn_Tbl_Lst

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM BronzeLakehouse.Mtd_Clr_Brn_Tbl_Lst

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

show tables in bronzelakehouse

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT count(*) FROM GoldLakehouse.clearingdim_h --where scdenddate = '9999-12-31'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

tables = [
    "cl_bkcompens_af_eta_v",
    "cl_bkcompens_rv_trf_v",
    "cl_bkcompens_av_eve_v",
    "cl_bkcompens_rv_eve_v",
    "cl_bkcompens_rv_chq_v",
    "cl_bkcompens_rv_eta_v",
    "cl_bkcompens_rf_eta_v",
    "cl_bkcompens_rf_v",
    "cl_bkcompens_error_v",
    "cl_bkcompens_av_chq_v",
    "cl_bkcompens_af_v",
    "cl_bkcompens_av_eta_v",
    "cl_bkcompens_av_evec_v",
    "cl_bkcompens_av_trf_v",
    "cl_bkcompens_rv_v",
    "cl_bkcompens_av_v"
]

for table in tables:
    df = spark.sql(f"SHOW CREATE TABLE bronzelakehouse.{table}")
    print(f"--- DDL for {table} ---")
    df.show(truncate=False)


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

describe table bronzelakehouse.cl_bkcompens_av_eve

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

df = spark.sql("SELECT * FROM BronzeLakehouse.CBS_Bkhis_new LIMIT 1000")
display(df)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DESCRIBE TABLE BronzeLakehouse.CL_BKCOMPENS_AV

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

df = spark.sql("SELECT * FROM BronzeLakehouse.CBS_Bkhis_New LIMIT 1000")
display(df)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

from pyspark.sql import SparkSession
from IPython.display import display, HTML
import urllib.parse

# --- Step 1: Prepare table and schema ---
lakehouse_name = "BronzeLakehouse"
table_name = "cl_bkcompens_av"
qualified_table_name = f"{lakehouse_name}.{table_name}"

df = spark.table(qualified_table_name)

# Generate column definitions
columns = []
for field in df.schema.fields:
    col_name = field.name
    data_type = field.dataType.simpleString()
    nullable = "" if field.nullable else " NOT NULL"
    columns.append(f"  {col_name} {data_type}{nullable}")

# Fetch partition column info
desc = spark.sql(f"DESCRIBE DETAIL {qualified_table_name}").collect()[0]
partition_cols = desc["partitionColumns"]

# Create DDL string
ddl = f"CREATE TABLE {qualified_table_name} (\n" + ",\n".join(columns) + "\n) USING DELTA"
if partition_cols:
    ddl += f"\nPARTITIONED BY ({', '.join(partition_cols)})"

# --- Step 2: Save the file using Fabric's file system utilities ---
# File path inside Fabric's lakehouse default location
file_path = "Files/ddl_cl_bkcompens_av.sql"

# Save the file using mssparkutils
from notebookutils import mssparkutils
mssparkutils.fs.put(file_path, ddl, overwrite=True)

# --- Step 3: Create downloadable link ---
escaped_path = urllib.parse.quote(file_path)
download_url = f"/explore/default/{escaped_path}"

display(HTML(f"<a href='{download_url}' target='_blank' download>📥 Click here to download the DDL file</a>"))


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

import shutil
import tempfile

# Prepare the table and DDL as before
lakehouse_name = "BronzeLakehouse"
table_name = "cl_bkcompens_av"
qualified_table_name = f"{lakehouse_name}.{table_name}"

df = spark.table(qualified_table_name)

columns = []
for field in df.schema.fields:
    col_name = field.name
    data_type = field.dataType.simpleString()
    nullable = "" if field.nullable else " NOT NULL"
    columns.append(f"  {col_name} {data_type}{nullable}")

desc = spark.sql(f"DESCRIBE DETAIL {qualified_table_name}").collect()[0]
partition_cols = desc["partitionColumns"]

ddl = f"CREATE TABLE {qualified_table_name} (\n" + ",\n".join(columns) + "\n) USING DELTA"
if partition_cols:
    ddl += f"\nPARTITIONED BY ({', '.join(partition_cols)})"

# --- Step 1: Write DDL to a temporary file ---
temp_file_path = tempfile.mktemp(suffix=".sql")  # Create a temp file for DDL
with open(temp_file_path, 'w') as temp_file:
    temp_file.write(ddl)

# --- Step 2: Copy the temp DDL file to the Fabric lakehouse directory ---
import shutil
target_abfss = '/lakehouse/default/Files/ddl_cl_bkcompens_av.sql'
shutil.copyfile(temp_file_path, target_abfss)

# Clean up the temporary file
import os
os.remove(temp_file_path)

print(f"✅ DDL file saved at: {target_abfss}")


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

# List the files in the target directory using mssparkutils
file_list = mssparkutils.fs.ls('/lakehouse/default/Files')

# Display the file list
for file in file_list:
    print(file.name)


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC DESCRIBE detail  cbs_bkhisl

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

# Load the Delta table
lakehouse_name = "BronzeLakehouse"
table_name = ["cl_bkcompens_av", "cl_bkcompens_rv"]

for table in table_name:
    qualified_table_name = f"{lakehouse_name}.{table}"
    df = spark.table(qualified_table_name)

    # Generate column definitions
    columns = []
    for field in df.schema.fields:
        col_name = field.name
        data_type = field.dataType.simpleString()
        nullable = "" if field.nullable else " NOT NULL"
        columns.append(f"  {col_name} {data_type}{nullable}")

    # Fetch partition column info from DESCRIBE DETAIL
    desc = spark.sql(f"DESCRIBE DETAIL {qualified_table_name}").collect()[0]
    partition_cols = desc["partitionColumns"]

    # Combine into CREATE TABLE DDL
    ddl = f"CREATE TABLE {qualified_table_name} (\n" + ",\n".join(columns) + "\n) USING DELTA"

    # Add PARTITIONED BY clause if applicable
    if partition_cols:
        ddl += f"\nPARTITIONED BY ({', '.join(partition_cols)})"

    # Output final DDL
    print(ddl)


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SHOW CREATE TABLE CL_BKCOMPENS_AV


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

show tables in Bronzelakehouse;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM BronzeLakehouse.mtd_src_brn_tbl_lst

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM BronzeLakehouse.Mtd_Clr_Brn_Tbl_Lst LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from BronzeLakehouse.TEMP_BKCOMPENS_RV_TRF

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

Describe history BronzeLakehouse.TEMP_BKCOMPENS_RV_CHQ 

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC describe formatted BronzeLakehouse.TEMP_BKCOMPENS_AF_ETA

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE TABLE BronzeLakehouse.sample123 (
-- MAGIC     col_bigint BIGINT,
-- MAGIC     col_boolean BOOLEAN,
-- MAGIC     col_int INT,
-- MAGIC     col_smallint SMALLINT,
-- MAGIC     col_tinyint TINYINT,
-- MAGIC     col_double DOUBLE,
-- MAGIC     col_float FLOAT,
-- MAGIC     col_date DATE,
-- MAGIC     col_timestamp TIMESTAMP,
-- MAGIC     col_string_1 STRING,
-- MAGIC     col_string_2 VARCHAR(100),
-- MAGIC     col_binary BINARY,
-- MAGIC     col_decimal DECIMAL(10,2)
-- MAGIC );


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE TABLE GoldLakehouse.ClearingDim_H (
-- MAGIC     ClearingID BIGINT,
-- MAGIC     ClearingType STRING,
-- MAGIC     ClearingCode STRING,
-- MAGIC     FileBranchCode STRING,
-- MAGIC     FileBranch STRING,
-- MAGIC     FileCode STRING,
-- MAGIC     FileStatus STRING,
-- MAGIC     FileStatusDesc STRING,
-- MAGIC     FileReference STRING,
-- MAGIC     FileName STRING,
-- MAGIC     FileStatusDate TIMESTAMP,
-- MAGIC     ClearingStatus STRING,
-- MAGIC     ClearingStatusDesc STRING,
-- MAGIC     InstrumentRef STRING,
-- MAGIC     SenderInfo STRING,
-- MAGIC     ReceiverInformation STRING,
-- MAGIC     CheckDigit STRING,
-- MAGIC     ReceiverName STRING,
-- MAGIC     TranRef STRING,
-- MAGIC     ClearingDate TIMESTAMP,
-- MAGIC     ClearingStatusReason STRING,
-- MAGIC     ChequeID BIGINT,
-- MAGIC     ChqVisStatus STRING,
-- MAGIC     ChqNumber STRING,
-- MAGIC     Narration STRING,
-- MAGIC     BatchID INT,
-- MAGIC     BatchDate DATE,
-- MAGIC     CreatedOn DATE,
-- MAGIC     UpdatedOn DATE,
-- MAGIC     SystemCode STRING,
-- MAGIC     RowHash BINARY,
-- MAGIC     ScdStartDate DATE,
-- MAGIC     ScdEndDate DATE,
-- MAGIC     WorkFlowName STRING
-- MAGIC ) using Delta;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from SilverLakehouse.ClearingDim 

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from GoldLakehouse.ClearingDim where BatchDate = '2025-02-12'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC 
-- MAGIC Insert into GoldLakehouse.ClearingDim (
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
-- MAGIC     ReceiverInformation,
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
-- MAGIC     CreatedOn,
-- MAGIC     UpdatedOn,
-- MAGIC     SystemCode,
-- MAGIC     RowHash,
-- MAGIC     WorkFlowName
-- MAGIC ) 
-- MAGIC 
-- MAGIC select 
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
-- MAGIC     CreatedOn,
-- MAGIC     Updated_On,
-- MAGIC     SystemCode,
-- MAGIC     RowHash,
-- MAGIC     WorkFlowName
-- MAGIC 
-- MAGIC     from GoldLakehouse.ClearingDim_Bkp;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC -- select * from GoldLakehouse.ClearingDim where UpdatedOn is not null;
-- MAGIC 
-- MAGIC SELECT * FROM GoldLakehouse.ClearingDim where ClearingID = '270172'; --270172

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from GoldLakehouse.ClearingDim_H

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC describe extended GoldLakehouse.ClearingDim_H_bkp

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


%%sql
Insert into GoldLakehouse.ClearingDim_H (
    ClearingID,
    ClearingType,
    ClearingCode,
    FileBranchCode,
    FileBranch,
    FileCode,
    FileStatus,
    FileStatusDesc,
    FileReference,
    FileName,
    FileStatusDate,
    ClearingStatus,
    ClearingStatusDesc,
    InstrumentRef,
    SenderInfo,
    ReceiverInformation,
    CheckDigit,
    ReceiverName,
    TranRef,
    ClearingDate,
    ClearingStatusReason,
    ChequeID,
    ChqVisStatus,
    ChqNumber,
    Narration,
    BatchID,
    BatchDate,
    CreatedOn,
    UpdatedOn,
    SystemCode,
    RowHash,
    ScdStartDate,
    ScdEndDate,
    WorkFlowName
) 

select 
    ClearingID,
    ClearingType,
    ClearingCode,
    FileBranchCode,
    FileBranch,
    FileCode,
    FileStatus,
    FileStatusDesc,
    FileReference,
    FileName,
    FileStatusDate,
    ClearingStatus,
    ClearingStatusDesc,
    InstrumentRef,
    SenderInfo,
    Receiver_Information,
    CheckDigit,
    ReceiverName,
    TranRef,
    ClearingDate,
    ClearingStatusReason,
    ChequeID,
    ChqVisStatus,
    ChqNumber,
    Narration,
    BatchID,
    BatchDate,
    CreatedOn,
    Updated_On,
    SystemCode,
    SCD_RowHash,
    SCD_StartDate,
    SCD_EndDate,
    WorkFlowName

    from GoldLakehouse.ClearingDim_H_Bkp;




-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC 
-- MAGIC SELECT * FROM GoldLakehouse.ClearingDim where ClearingID = '270172'; 

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC create table GoldLakehouse.ClearingDim_DELETE_20250307 as select * from GoldLakehouse.ClearingDim;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC create table GoldLakehouse.ClearingDim_H_DELETE_20250307 as select * from GoldLakehouse.ClearingDim_H;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC -- select cast(BatchDate as date), UpdatedOn, ScdEndDate, count(*) from GoldLakehouse.dbo.ClearingDim_H 
-- MAGIC -- group by cast(BatchDate as date) , UpdatedOn, ScdEndDate
-- MAGIC -- order by cast(BatchDate as date), ScdEndDate ;
-- MAGIC 
-- MAGIC select * from GoldLakehouse.ClearingDim_H where ScdEndDate <> '9999-12-31' and ClearingType = 'A'
-- MAGIC order by ClearingID;
-- MAGIC 
-- MAGIC select * from GoldLakehouse.ClearingDim_H where ClearingID IN ('185591', '185592', '185593')
-- MAGIC order by ClearingID;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

                    

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from SilverLakehouse.ClearingDim where Narration is not null and ClearingType = 'A'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT MOT2, TRIM(MOT2) AS MOT2NULL, TRIM(MOT2) FROM RAWBK05100.BKCOMPENS_RV_TRF
-- MAGIC 
-- MAGIC select * from CL_BKCOMPENS_AV_TRF_DELETE WHERE MOT2 IS NULL OR MOT2NULL IS NULL

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_RV_TRF_DELETE1 where MOT2NULL is null
-- MAGIC                               

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC --SELECT '' as emptystring, TRIM(' ') as onespacestring, TRIM('') as trimemptystring, TRIM(NULL) as nullstring, NVL(TRIM(''), '') as nvlnullstring FROM dual
-- MAGIC 
-- MAGIC select * from CL_BKCOMPENS_AV_TRF_DELETE

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from CL_BKCOMPENS_AF_DELETE

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

df = spark.sql("SELECT * FROM BronzeLakehouse.Mtd_Clr_Brn_Tbl_Lst LIMIT 1000")
display(df)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC --Query to fetch data from oracle to lakehouse
-- MAGIC 
-- MAGIC -- SELECT IDAV, MOT2, 
-- MAGIC -- TRIM(MOT2) AS simpletrim, 
-- MAGIC -- NVL(TRIM(MOT2), '') as nvltrim, 
-- MAGIC -- COALESCE(TRIM(MOT2), '') as coalescetrim, 
-- MAGIC -- NULL as nullvalue,
-- MAGIC -- Cast(TRIM(MOT2) as VARCHAR2(10)) as caststringtrim,
-- MAGIC -- TO_CHAR(TRIM(MOT2)) as tochartrim,
-- MAGIC -- LTRIM(RTRIM(MOT2)) as leftrighttrim,
-- MAGIC -- CASE WHEN TRIM(MOT2) IS NULL THEN '' ELSE TRIM(MOT2) END AS casetrim,
-- MAGIC -- CASE WHEN TRIM(MOT2) IS NULL THEN CHR(1) ELSE TRIM(MOT2) END AS casechrtrim,
-- MAGIC -- REGEXP_REPLACE(MOT2, '^\s+$', '') as regextrim,
-- MAGIC -- COALESCE(NULLIF(TRIM(MOT2), ''), '') as coalescenulliftrim
-- MAGIC -- FROM RAWBK05100.BKCOMPENS_AV_TRF where IDAV in ('19103112032900000014', '19103112124400000018')
-- MAGIC 
-- MAGIC --Output Table
-- MAGIC select * from BronzeLakehouse.SAMPLE2
-- MAGIC 


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT * FROM BronzeLakehouse.Mtd_Clr_Brn_Tbl_Lst

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

df = spark.sql("SELECT Max(BATCHID) FROM BronzeLakehouse.CL_BKCOMPENS_RF LIMIT 1000")
display(df)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC 
-- MAGIC Insert into BronzeLakehouse.Mtd_Clr_Brn_Tbl_Lst
-- MAGIC Values
-- MAGIC ('RAWBK05100', 'BKCOMPENS_AF_ETA', 'IDFIC,NORD, ETA, PROG, CREJ, CMOT,MOT, UTI,DCRE,  HCRE,DCO, DCOM'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_RV_TRF', 'IDRVTRF, IDRV, MOT1, MOT2, MOT3, MOT4'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_AV_EVE', 'IDAV, NORD, AGE, OPE, EVE, NAT,DESA1, DESA2, DESA3, DESA4, DESA5,DESC1, DESC2, DESC3, DESC4, DESC5,FORC, DSAI, HSAI'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_RV_EVE', 'IDRV,NORD, AGE, OPE, EVE, NAT,DESA1, DESA2, DESA3, DESA4, DESA5,DESC1, DESC2, DESC3, DESC4, DESC5, DSAI,HSAI'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_RV_CHQ', 'IDRVCHQ,IDRV,NCHQ,  CTRLVISU, DESA1, DESA2, SERIAL,  DECH, CMC7'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_RV_ETA', 'IDRV,NORD, ETA, PROG, CREJ, CMOT,MOT,NORDEVE, UTI,DCO, DCOM, DCRE,HCRE'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_RF_ETA', 'IDFIC, NORD,ETA, PROG, CREJ, CMOT,MOT, UTI,DCRE, HCRE,DCO, DCOM'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_RF', 'IDFIC, NFIC, REF_COM,ZONE_ENTETE, ZONE_PIED,ETA,SYST, CYCLE_PRES, AGE'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_ERROR', 'IDEN, SENS, ID, TYPE, CERR, LCERR, PROG, MESS,DCRE,HCRE, ETA, CONTENU'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_AV_CHQ', 'IDAVCHQ,IDAV, IDCHQ, NCHQ, SERIE, DECH, CMC7'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_AF', 'IDFIC, NFIC, REF_COM,ZONE_ENTETE, ZONE_PIED, ETA,CYCLE_PRES,AGE'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_AV_ETA', 'IDAV, NORD,ETA,PROG, CREJ, CMOT,MOT, NORDEVE,UTI, DCO, DCOM, DCRE,HCRE'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_AV', 'IDAV, IDFIC, TOPE, ETABD, GUIBD, COMD, CLEB, NOMD, AGEE, NCPE, SUFE, DEVE, DEV, MON, REF, DCOM, ID_ORIG_SENS, ID_ORIG, ZONE, ETA, SYST, NATOPE'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_AV_EVEC', 'IDAV, NORD,NAT, IDEN, TYPC, DEVR, MCOMR,TXREF,MCOMC,MCOMN,MCOMT,TAX,TCOM'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_AV_TRF', 'IDAVTRF,IDAV, MOT1, MOT2, MOT3, MOT4, AGEF, NCPF, SUFF, DEVF'),
-- MAGIC ('RAWBK05100', 'BKCOMPENS_RV', 'IDRV, IDFIC,NLIGNE, TOPE, EMETTEUR, DESTINATAIRE, ETABE, GUIBE, COME, CLEE, AGED, NCPD, SUFD, DEVD, NOMD, DEV,MON, REF, DCOM,ID_ORIG_SENS, ID_ORIG, ETA, DREG, NATOPE');

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC Create or replace view BronzeLakehouse.CL_BKCOMPENS_AF_ETA_V AS
-- MAGIC select TRIM(IDFIC) AS IDFIC,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE,  TRIM(HCRE) AS HCRE,DCO, DCOM, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM BronzeLakehouse.CL_BKCOMPENS_AF_ETA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from BronzeLakehouse.CL_BKCOMPENS_AV_TRF where MOT2 = '                              ';

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC SELECT * FROM BronzeLakehouse.CL_BKCOMPENS_AV_TRF_V where MOT2 = '                              ';


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_RV_TRF_V AS select IDRVTRF, TRIM(IDRV) AS IDRV, TRIM(MOT1) AS MOT1, TRIM(MOT2) AS MOT2, TRIM(MOT3) AS MOT3, TRIM(MOT4) AS MOT4, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_RV_TRF;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_AV_EVE_V AS select TRIM(IDAV) AS IDAV, NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5,TRIM(FORC) AS FORC, DSAI, TRIM(HSAI) AS HSAI, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_AV_EVE;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_RV_EVE_V AS select TRIM(IDRV) AS IDRV,NORD, TRIM(AGE) AS AGE, TRIM(OPE) AS OPE, TRIM(EVE) AS EVE, TRIM(NAT) AS NAT,TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(DESA3) AS DESA3, TRIM(DESA4) AS DESA4, TRIM(DESA5) AS DESA5,TRIM(DESC1) AS DESC1, TRIM(DESC2) AS DESC2, TRIM(DESC3) AS DESC3, TRIM(DESC4) AS DESC4, TRIM(DESC5) AS DESC5, DSAI,TRIM(HSAI) AS HSAI, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_RV_EVE;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_RV_CHQ_V AS select IDRVCHQ,TRIM(IDRV) AS IDRV,TRIM(NCHQ) AS NCHQ,  TRIM(CTRLVISU) AS CTRLVISU, TRIM(DESA1) AS DESA1, TRIM(DESA2) AS DESA2, TRIM(SERIAL) AS SERIAL,  DECH, CMC7, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_RV_CHQ;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_RV_ETA_V AS select TRIM(IDRV) AS IDRV,NORD, TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT,NORDEVE, TRIM(UTI) AS UTI,DCO, DCOM, DCRE,TRIM(HCRE) AS HCRE, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_RV_ETA;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_RF_ETA_V AS select TRIM(IDFIC) AS IDFIC, NORD,TRIM(ETA) AS ETA, TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, TRIM(UTI) AS UTI,DCRE, TRIM(HCRE) AS HCRE,DCO, DCOM, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_RF_ETA;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_RF_V AS select TRIM(IDFIC) AS IDFIC, TRIM(NFIC) AS NFIC, TRIM(REF_COM) AS REF_COM,ZONE_ENTETE, ZONE_PIED,TRIM(ETA) AS ETA,TRIM(SYST) AS SYST, TRIM(CYCLE_PRES) AS CYCLE_PRES, TRIM(AGE) AS AGE, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_RF;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_ERROR_V AS select TRIM(IDEN) AS IDEN, TRIM(SENS) AS SENS, TRIM(ID) AS ID, TRIM(TYPE) AS TYPE, TRIM(CERR) AS CERR, TRIM(LCERR) AS LCERR, TRIM(PROG) AS PROG, TRIM(MESS) AS MESS,DCRE,TRIM(HCRE) AS HCRE, TRIM(ETA) AS ETA, CONTENU, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_ERROR;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_AV_CHQ_V AS select IDAVCHQ,TRIM(IDAV) AS IDAV, TRIM(IDCHQ) AS IDCHQ, TRIM(NCHQ) AS NCHQ, TRIM(SERIE) AS SERIE, DECH, CMC7, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_AV_CHQ;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_AF_V AS select TRIM(IDFIC) AS IDFIC, TRIM(NFIC) AS NFIC, TRIM(REF_COM) AS REF_COM,ZONE_ENTETE, ZONE_PIED, TRIM(ETA) AS ETA,CYCLE_PRES,TRIM(AGE) AS AGE, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_AF;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_AV_ETA_V AS select TRIM(IDAV) AS IDAV, NORD,TRIM(ETA) AS ETA,TRIM(PROG) AS PROG, TRIM(CREJ) AS CREJ, TRIM(CMOT) AS CMOT,TRIM(MOT) AS MOT, NORDEVE,TRIM(UTI) AS UTI, DCO, DCOM, DCRE,TRIM(HCRE) AS HCRE, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_AV_ETA;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_AV_V AS select IDAV, IDFIC, TOPE, ETABD, GUIBD, COMD, CLEB, NOMD, AGEE, NCPE, SUFE, DEVE, DEV, MON, REF, DCOM, ID_ORIG_SENS, ID_ORIG, ZONE, ETA, SYST, NATOPE, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_AV;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_AV_EVEC_V AS select TRIM(IDAV) AS IDAV, NORD,TRIM(NAT) AS NAT, TRIM(IDEN) AS IDEN, TRIM(TYPC) AS TYPC, TRIM(DEVR) AS DEVR, MCOMR,TXREF,MCOMC,MCOMN,MCOMT,TRIM(TAX) AS TAX,TCOM, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_AV_EVEC;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_AV_TRF_V AS select IDAVTRF,TRIM(IDAV) AS IDAV, TRIM(MOT1) AS MOT1, TRIM(MOT2) AS MOT2, TRIM(MOT3) AS MOT3, TRIM(MOT4) AS MOT4, TRIM(AGEF) AS AGEF, TRIM(NCPF) AS NCPF, TRIM(SUFF) AS SUFF, TRIM(DEVF) AS DEVF, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_AV_TRF;
-- MAGIC CREATE VIEW BronzeLakehouse.CL_BKCOMPENS_RV_V AS select TRIM(IDRV) AS IDRV, TRIM(IDFIC) AS IDFIC,NLIGNE, TRIM(TOPE) AS TOPE, TRIM(EMETTEUR) AS EMETTEUR, TRIM(DESTINATAIRE) AS DESTINATAIRE, TRIM(ETABE) AS ETABE, TRIM(GUIBE) AS GUIBE, TRIM(COME) AS COME, TRIM(CLEE) AS CLEE, TRIM(AGED) AS AGED, TRIM(NCPD) AS NCPD, TRIM(SUFD) AS SUFD, TRIM(DEVD) AS DEVD, TRIM(NOMD) AS NOMD, TRIM(DEV) AS DEV,MON, TRIM(REF) AS REF, DCOM,TRIM(ID_ORIG_SENS) AS ID_ORIG_SENS, TRIM(ID_ORIG) AS ID_ORIG, TRIM(ETA) AS ETA,DREG, NATOPE, BatchID, Cast(BatchDate as date) as BatchDate, SystemCode, WorkflowName FROM  BronzeLakehouse.CL_BKCOMPENS_RV;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select distinct(BatchDate) from GoldLakehouse.ClearingDim_delete_20250307;
-- MAGIC select distinct(BatchDate) from GoldLakehouse.ClearingDim_h_delete_20250307;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from cl_bkcompens_rv_chq

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select BatchDate, UpdatedOn, count(*) from GoldLakehouse.ClearingDim
-- MAGIC group by BatchDate, UpdatedOn

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select b.* from goldlakehouse.clearingdim_delete_20250307 a
-- MAGIC right join silverlakehouse.clearingdim b
-- MAGIC on a.ClearingID = b.ClearingID
-- MAGIC where a.clearingid is null

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE or REPLACE TABLE SilverLakehouse.ClearingFact (
-- MAGIC     ClearingID BIGINT,
-- MAGIC     ClearingType STRING,
-- MAGIC     SettleDate INT,
-- MAGIC     OperationID SMALLINT,
-- MAGIC     InstitutionID SMALLINT,
-- MAGIC     RemitAcc STRING,
-- MAGIC     ReceiptAccID BIGINT,
-- MAGIC     ReceiptBranchID SMALLINT,
-- MAGIC     ReceipientCurrID SMALLINT,
-- MAGIC     OperationCurrID SMALLINT,
-- MAGIC     Amount DECIMAL(19, 4),
-- MAGIC     TranRef STRING,
-- MAGIC     ClearingDate INT,
-- MAGIC     BatchID INT,
-- MAGIC     BatchDate DATE,
-- MAGIC     CreatedOn DATE,
-- MAGIC     UpdatedOn DATE,
-- MAGIC     SystemCode STRING,
-- MAGIC     RowHash BINARY,
-- MAGIC     WorkFlowName STRING
-- MAGIC );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE TABLE SilverLakehouse.ClearingErrFact (
-- MAGIC     ClearingID BIGINT,
-- MAGIC     ClearingType STRING,
-- MAGIC     StatusID SMALLINT,
-- MAGIC     SettleDate INT,
-- MAGIC     OperationID SMALLINT,
-- MAGIC     InstitutionID SMALLINT,
-- MAGIC     RemitAcc STRING,
-- MAGIC     ReceiptAccID BIGINT,
-- MAGIC     ReceiptBranchID INT,
-- MAGIC     RecipientCurrID SMALLINT,
-- MAGIC     OperationCurrID SMALLINT,
-- MAGIC     Amount DECIMAL(18, 0),
-- MAGIC     TranRef STRING,
-- MAGIC     ClearingDate INT,
-- MAGIC     Identifier STRING,
-- MAGIC     ErrorType STRING,
-- MAGIC     Program STRING,
-- MAGIC     ShortErrCode STRING,
-- MAGIC     LongErrCode STRING,
-- MAGIC     ErrMessage STRING,
-- MAGIC     ErrorStatus STRING,
-- MAGIC     ErrorDate INT,
-- MAGIC     ErrorTime STRING,
-- MAGIC     BatchID INT,
-- MAGIC     BatchDate DATE,
-- MAGIC     CreatedOn DATE,
-- MAGIC     UpdatedOn DATE,
-- MAGIC     SystemCode STRING,
-- MAGIC     RowHash BINARY,
-- MAGIC     WorkFlowName STRING
-- MAGIC );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE TABLE BronzeLakehouse.sample3 (
-- MAGIC     id              INT,
-- MAGIC     tiny_num        TINYINT,
-- MAGIC     small_num       SMALLINT,
-- MAGIC     big_num         BIGINT,
-- MAGIC     decimal_num     DECIMAL(10,2),
-- MAGIC     float_num       FLOAT,
-- MAGIC     double_num      DOUBLE,
-- MAGIC     str_col         STRING,
-- MAGIC     char_col        CHAR(10),
-- MAGIC     varchar_col     VARCHAR(50),
-- MAGIC     date_col        DATE,
-- MAGIC     timestamp_col   TIMESTAMP,
-- MAGIC     bool_col        BOOLEAN
-- MAGIC );


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from SilverLakehouse.ClearingEventFact where ClearingID in ('87322', '207256')

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from GoldLakehouse.DateDim where DateDim = '2022-05-30'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from GoldLakehouse.ClearingDim where ClearingID = '207256' and ClearingType = 'A'

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

insert overwrite GoldLakehouse.ClearingDim
SELECT * FROM GoldLakehouse.ClearingDim
where BatchDate <> '2025-05-14'

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
