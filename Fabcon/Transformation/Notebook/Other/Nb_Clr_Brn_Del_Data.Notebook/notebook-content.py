# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a43617b8-dd1a-46c9-aa3c-c2645e30c949",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785"
# META     }
# META   }
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

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

max_date_df = spark.sql("SELECT MAX(CAST(Batch_Date AS DATE)) AS max_date FROM BronzeLakehouse.dbo.abc")
max_date = max_date_df.collect()[0]["max_date"]  # Extract max date from the DataFrame


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

queries = [
 "DELETE FROM BronzeLakehouse.CL_BKCOMPENS_AV_CHQ WHERE CAST(Batch_Date AS DATE) <= '2025-02-20'",
 "DELETE FROM BronzeLakehouse.CL_BKCOMPENS_AF WHERE CAST(Batch_Date AS DATE) <= '2025-02-20'",
 "DELETE FROM BronzeLakehouse.CL_BKCOMPENS_AV_ETA WHERE CAST(BATCH_DATE AS DATE) <= '2025-02-20'",
 "DELETE FROM BronzeLakehouse.CL_BKCOMPENS_AF_ETA WHERE CAST(BATCH_DATE AS DATE) <= '2025-02-20'",
 "DELETE FROM BronzeLakehouse.CL_BKCOMPENS_RV_TRF WHERE CAST(bacth_Date AS DATE) <= '2025-02-20'",
 "DELETE FROM BronzeLakehouse.CL_BKCOMPENS_AV_TRF WHERE CAST(Batch_Date AS DATE) <= '2025-02-20'"
]

for query in queries:
 spark.sql(query) # Executes each SQL query in Spark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

queries = [
"Delete from BronzeLakehouse.CL_BKCOMPENS_AV where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_AV_EVEC where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_AV_EVE where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_RV_EVE where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_RV_CHQ where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_RV where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_RV_ETA where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_RF_ETA where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_RF where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_ERROR where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_AV_CHQ where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_AF where cast(Batch_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_AV_ETA where cast(BATCH_DATE as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_AF_ETA where cast(BATCH_DATE as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_RV_TRF where cast(bacth_Date as date) < '2025-02-21'",
"Delete from BronzeLakehouse.CL_BKCOMPENS_AV_TRF where cast(Batch_Date as date) < '2025-02-21'",
]

for query in queries:
 spark.sql(query) # Executes each SQL query in Spark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the list of tables
tables = [
    "CL_BKCOMPENS_AV", "CL_BKCOMPENS_AV_EVEC", "CL_BKCOMPENS_AV_EVE", "CL_BKCOMPENS_RV_EVE",
    "CL_BKCOMPENS_RV_CHQ", "CL_BKCOMPENS_RV", "CL_BKCOMPENS_RV_ETA", "CL_BKCOMPENS_RF_ETA",
    "CL_BKCOMPENS_RF", "CL_BKCOMPENS_ERROR", "CL_BKCOMPENS_AV_CHQ", "CL_BKCOMPENS_AF",
    "CL_BKCOMPENS_AV_ETA", "CL_BKCOMPENS_AF_ETA", "CL_BKCOMPENS_RV_TRF", "CL_BKCOMPENS_AV_TRF"
]

# Loop through each table and execute the query
for table in tables:
    query = f"SELECT DISTINCT Batch_Date FROM BronzeLakehouse.{table}"
    print(f"\nExecuting query on table: {table}\n")
    
    try:
        df = spark.sql(query)
        df.show(truncate=False)  # Display results without truncating values
    except Exception as e:
        print(f"Error executing query on {table}: {str(e)}")


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
