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
# META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785",
# META       "known_lakehouses": [
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
# META         },
# META         {
# META           "id": "a28baf80-5d91-4995-9acc-3a8bfa39eed9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import com.microsoft.spark.fabric  # Required in Fabric Notebooks
from pyspark.sql.functions import expr, col
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Step 1: Read metadata
df_meta = spark.read.synapsesql("DataOpsWarehouse.Metadata.Brn_Tbl_Lst_With_Trim")

# Step 2: Filter for CBS + Full Load
df_filtered = df_meta.filter(
    (col("SourceSystem") == "CBS") &
    (col("LoadType") == "Full") &
    (col("IsActive") == 1) #&(col("TargetTable").isin("Cbs_Bkacod", "Cbs_Bkacoc"))
)

# Step 3: Collect metadata entries as list of dicts
table_list = df_filtered.select("SourceSchema", "SourceTable", "SourceColumnList") \
    .rdd.map(lambda row: row.asDict()).collect()

# Step 4: Define processing function
def process_table(entry):
    source_schema = entry["SourceSchema"]
    source_table = entry["SourceTable"]
    column_list_str = entry["SourceColumnList"]

    full_table_name = f"{source_schema}.{source_table}"
    full_history_table = f"BronzeLakehouseHistory.{source_table}"

    print(f"[START] Processing: {full_table_name}")
    
    try:
        df_source = spark.read.format("delta").table(full_table_name)

        if column_list_str:
            expressions = [expr(c.strip()) for c in column_list_str.split(",")]
            df_source = df_source.select(*expressions)

        # Print preview
       # print(f"[DATA] Preview for {full_table_name}:")
       # df_source.show(5, truncate=False)

        # Write with optimization
        #df_source.repartition("BatchDate") \
                # .write.mode("append") \
                # .saveAsTable(full_history_table)
        df_source.write \
        .format("delta") \
        .mode("overwrite") \
        .insertInto(full_history_table)

        print(f"[SUCCESS] Appended data to: {full_history_table}")

    except Exception as e:
        print(f"[ERROR] Failed processing {full_table_name}: {str(e)}")

# Step 5: Use ThreadPoolExecutor for parallelism
print("[INFO] Starting parallel processing...")
max_workers = 5

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(process_table, entry) for entry in table_list]
    for future in as_completed(futures):
        # wait for all futures to complete
        future.result()  # raises any exceptions that occurred

print("[INFO] All tables processed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
