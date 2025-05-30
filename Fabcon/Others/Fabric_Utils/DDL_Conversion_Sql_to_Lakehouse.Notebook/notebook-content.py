# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### Sql DDL to Lakehouse DDL conversion script

# MARKDOWN ********************

# You can use this script in VS Code, Colab or Jupyter to convert the SQL DDL into a Fabric Lakehouse-compatible DDL.

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
        r'\btext\b': 'STRING',
        r'ntext': 'STRING',
        r'uniqueidentifier': 'STRING',
        r'bigint': 'BIGINT',
        r'long': 'BIGINT',
        r'\bint\b': 'INT',
        r'\bdouble\b': 'double',
        r'integer':'INT',
        r'smallint': 'SMALLINT',
        r'\bshort\b': 'SMALLINT',
        r'tinyint': 'TINYINT',
        r'\bbyte\b': 'TINYINT',
        r'numeric\((\d+),\s*(\d+)\)': r'DECIMAL(\1, \2)',
        r'decimal\((\d+),\s*(\d+)\)': r'DECIMAL(\1, \2)',
        r'dec\((\d+),\s*(\d+)\)': r'DECIMAL(\1, \2)',
        r'\bmoney\b': 'DECIMAL(19,4)',
        r'smallmoney': 'DECIMAL(10,4)',
        r'float': 'FLOAT',
        r'real': 'FLOAT',
        r'\bbit\b': 'BOOLEAN',
        r'date': 'DATE',
        r'datetime2': 'TIMESTAMP',
        r'datetime': 'TIMESTAMP',
        r'smalldatetime': 'TIMESTAMP',
        r'\btime\b': 'TIME',
        r'xml': 'STRING',
        r'varbinary\(max\)': 'Binary',
        r'varbinary\(\d+\)': 'Binary',
        r'binary\(\d+\)': 'Binary'
        
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
    ddl = re.sub(r'\[([^\]]+)\]', r'\1', ddl)  
    ddl = re.sub(r'SET ANSI_NULLS ON|SET QUOTED_IDENTIFIER ON', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'(?m)^\s*GO\s*$', '', ddl, flags=re.IGNORECASE)  # Removes GO only when it's a standalone line

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
    new_schema_name = 'dbo' #-----------------enter schema name-------------------
    for ddl in ddl_statements:
        if 'CREATE TABLE' in ddl:
            spark_ddl = convert_to_spark_sql(ddl, new_schema_name)
            if spark_ddl:
                spark_ddls.append(spark_ddl)

    # Write the converted DDLs to the output file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n\n'.join(spark_ddls))

# input path
input_file = 'CBS DDL (2)'  #------------------File containing SQL Server DDLs-----------------------
output_file = 'output.sql'  # -------------------------File to store Spark SQL DDLs----------------------------------
process_sql_file(input_file, output_file)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
