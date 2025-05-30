# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re

def convert_to_spark_sql(ddl, new_schema_name):
    # Mapping SQL Server data types to Spark SQL data types
    type_mapping = {
        r'nvarchar\(max\)': 'STRING',
        r'nvarchar\(\d+\)': 'STRING',
        r'varchar\(max\)': 'STRING',
        r'varchar\(\d+\)': 'STRING',
        r'char\(\d+\)': 'STRING',
        r'text': 'STRING',
        r'ntext': 'STRING',
        r'uniqueidentifier': 'STRING',
        r'bigint': 'BIGINT',
        r'int': 'INT',
        r'smallint': 'SMALLINT',
        r'tinyint': 'INT',
        r'numeric\((\d+),\s*(\d+)\)': r'DECIMAL(\1, \2)',
        r'decimal\((\d+),\s*(\d+)\)': r'DECIMAL(\1, \2)',
        r'money': 'DECIMAL(19,4)',
        r'smallmoney': 'DECIMAL(10,4)',
        r'float': 'FLOAT',
        r'real': 'FLOAT',
        r'bit': 'BOOL',
        r'date': 'DATE',
        r'datetime2': 'TIMESTAMP',
        r'datetime': 'TIMESTAMP',
        r'smalldatetime': 'TIMESTAMP',
        r'time': 'TIME',
        r'xml': 'STRING',
        r'varbinary\(max\)': 'BYTES',
        r'varbinary\(\d+\)': 'BYTES',
        r'binary\(\d+\)': 'BYTES',
        r'json': 'STRING',
        r'geometry': 'GEOGRAPHY',
        r'geography': 'GEOGRAPHY'
    }

    # Pattern to change schema name
    pattern = re.compile(r'CREATE TABLE \[([^\]]+)\]\.', re.IGNORECASE)
    ddl = pattern.sub(f'CREATE TABLE [{new_schema_name}].', ddl)

    # Extract schema and table name
    match = re.search(r'CREATE TABLE \[(.*?)\]\.?\[(.*?)\]', ddl, re.IGNORECASE)
    if match:
        schema_name, table_name = match.groups()
    else:
        return None

    # Remove SQL Server specific syntax
    ddl = re.sub(r'ON \[PRIMARY\].*?(?=\s*CREATE TABLE|$)', '', ddl, flags=re.IGNORECASE | re.DOTALL)
    ddl = re.sub(r'\[TEXTIMAGE_ON PRIMARY\]', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\[([^\]]+)\]', r'\1', ddl)  # Remove square brackets
    ddl = re.sub(r'SET ANSI_NULLS ON|SET QUOTED_IDENTIFIER ON|GO', '', ddl, flags=re.IGNORECASE)

    # Replace SQL Server data types with Spark SQL equivalents
    for sql_type, spark_type in type_mapping.items():
        ddl = re.sub(sql_type, spark_type, ddl, flags=re.IGNORECASE)

    ddl = re.sub(r'TIMESTAMP\(\d+\)', 'TIMESTAMP', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'TIME\(\d+\)', 'TIME', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\s+NULL', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'IDENTITY\(\d+,\s*\d+\)', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\s+NOT NULL', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'(\bBIGINT\b|\bINT\b|\bSMALLINT\b|\bSTRING\b|\bDECIMAL\(\d+,\s*\d+\)\b|\bFLOAT64\b|\bBOOL\b|\bDATE\b|\bTIMESTAMP\b|\bTIME\b|\bBYTES\b|\bGEOGRAPHY\b)\s+NOT\b', r'\1', ddl, flags=re.IGNORECASE)


    # Remove NULL keywords since Spark SQL does not require them
    ddl = re.sub(r'\s+NULL', '', ddl, flags=re.IGNORECASE)

    # Convert CREATE TABLE structure with schema and table name
    ddl = re.sub(r'CREATE TABLE .*?\[.*?\]\.?\[.*?\]', f'CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}', ddl, flags=re.IGNORECASE)
    ddl = ddl.strip()

    return ddl

def process_sql_file(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # Split file content into separate DDLs using 'CREATE TABLE' as the delimiter
    ddl_statements = re.split(r'(?=CREATE TABLE)', sql_content, flags=re.IGNORECASE)

    spark_ddls = []
    new_schema_name = 'BronzeLakehouse' #enter schema name
    for ddl in ddl_statements:
        if 'CREATE TABLE' in ddl:
            spark_ddl = convert_to_spark_sql(ddl, new_schema_name)
            if spark_ddl:
                spark_ddls.append(spark_ddl)

    # Write the converted DDLs to the output file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n\n'.join(spark_ddls))

# input path
input_file = '/content/aa.textile'  # File containing SQL Server DDLs
output_file = 'output.sql'  # File to store Spark SQL DDLs
process_sql_file(input_file, output_file)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import re

# def convert_sqlserver_to_fabric(ddl: str) -> str:
#     type_mapping = {
#         r'bigint': 'BIGINT',
#         r'int': 'INT',
#         r'varchar\(\d+\)': 'STRING',
#         r'nvarchar\(\d+\)': 'STRING',
#         r'char\(\d+\)': 'STRING',
#         r'nchar\(\d+\)': 'STRING',
#         r'text': 'STRING',
#         r'ntext': 'STRING',
#         r'datetime2\(\d+\)': 'TIMESTAMP',
#         r'datetime': 'TIMESTAMP',
#         r'smalldatetime': 'TIMESTAMP',
#         r'date': 'DATE',
#         r'time\(\d+\)': 'STRING',
#         r'bit': 'BOOLEAN',
#         r'float': 'DOUBLE',
#         r'real': 'FLOAT',
#         r'decimal\(\d+,\d+\)': 'DECIMAL',
#         r'numeric\(\d+,\d+\)': 'DECIMAL',
#         r'money': 'DECIMAL',
#         r'smallmoney': 'DECIMAL',
#         r'varbinary\(\d+\)': 'BINARY',
#         r'binary\(\d+\)': 'BINARY'
#     }
    
#     # Remove unnecessary options like "ON [PRIMARY]"
#     ddl = re.sub(r'ON \[PRIMARY\]', '', ddl, flags=re.IGNORECASE)
    
#     # Replace SQL Server data types with Fabric Lakehouse equivalents
#     for sql_type, fabric_type in type_mapping.items():
#         ddl = re.sub(sql_type, fabric_type, ddl, flags=re.IGNORECASE)
    
#     # Replace table schema prefix
#     ddl = re.sub(r'\[dbo\]\.', '', ddl, flags=re.IGNORECASE)
    
#     # Remove NULL keywords
#     ddl = re.sub(r' NULL', '', ddl, flags=re.IGNORECASE)
    
#     return ddl

# # Example usage
# sql_server_ddl = """
# CREATE TABLE [dbo].[ClearingDim](
# 	[ClearingID] [bigint] NULL,
# 	[ClearingType] [varchar](1) NULL,
# 	[ClearingCode] [varchar](100) NULL,
# 	[FileBranchCode] [varchar](5) NULL,
# 	[FileBranch] [varchar](40) NULL,
# 	[FileCode] [varchar](100) NULL,
# 	[FileStatus] [varchar](8) NULL,
# 	[FileStatusDesc] [varchar](100) NULL,
# 	[FileReference] [varchar](150) NULL,
# 	[FileName] [varchar](500) NULL,
# 	[FileStatusDate] [datetime2](7) NULL,
# 	[ClearingStatus] [varchar](10) NULL,
# 	[ClearingStatusDesc] [varchar](100) NULL,
# 	[InstrumentRef] [varchar](100) NULL,
# 	[SenderInfo] [varchar](1000) NULL,
# 	[Receiver_Information] [varchar](1000) NULL,
# 	[CheckDigit] [varchar](10) NULL,
# 	[ReceiverName] [varchar](400) NULL,
# 	[TranRef] [varchar](150) NULL,
# 	[ClearingDate] [datetime2](7) NULL,
# 	[ClearingStatusReason] [varchar](1000) NULL,
# 	[ChequeID] [bigint] NULL,
# 	[ChqVisStatus] [varchar](15) NULL,
# 	[ChqNumber] [varchar](50) NULL,
# 	[Narration] [varchar](150) NULL,
# 	[BatchID] [int] NULL,
# 	[BatchDate] [datetime] NULL,
# 	[CreatedOn] [date] NULL,
# 	[Updated_On] [datetime] NULL,
# 	[SystemCode] [varchar](20) NULL,
# 	[RowHash] [varbinary](64) NULL,
# 	[WorkFlowName] [varchar](40) NULL
# ) ON [PRIMARY]
# """

# fabric_ddl = convert_sqlserver_to_fabric(sql_server_ddl)
# print(fabric_ddl)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
