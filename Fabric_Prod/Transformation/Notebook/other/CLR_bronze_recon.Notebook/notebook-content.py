# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9c04cb09-56e5-49d6-a2b0-5e2e578f481a",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "197819da-2cec-4c3c-a96a-0fe62ae2300b",
# META       "known_lakehouses": [
# META         {
# META           "id": "9c04cb09-56e5-49d6-a2b0-5e2e578f481a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import lit, countDistinct
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# List of table names
tables = [
    "Cl_Bkcompens_Rv_Trf",
    "Cl_Bkcompens_Rf",
    "Cl_Bkcompens_Af_Eta",
    "Cl_Bkcompens_Rf_Eta",
    "Cl_Bkcompens_Rv_Eve",
    "Cl_Bkcompens_Rv_Chq",
    "Cl_Bkcompens_Rv_Eta",
    "Cl_Bkcompens_Av_Chq",
    "Cl_Bkcompens_Af",
    "Cl_Bkcompens_Av_Eve",
    "Cl_Bkcompens_Av_Trf",
    "Cl_Bkcompens_Av_Evec",
    "Cl_Bkcompens_Error",
    "Cl_Bkcompens_Av",
    "Cl_Bkcompens_Av_Eta",
    "Cl_Bkcompens_Rv"
]

normal_counts = []
distinct_counts = []

# Loop through each table
for table in tables:
    full_table_name = f"BronzeLakehouse.{table}"
    
    # Read the table
    df = spark.table(full_table_name)
    
    # Count total rows
    total_count = df.count()
    
    # Count distinct rows (across all columns)
    distinct_count = df.distinct().count()
    
    # Append to lists
    normal_counts.append((table, total_count))
    distinct_counts.append((table, distinct_count))

# Create DataFrames
normal_df = spark.createDataFrame(normal_counts, ["table_name", "row_counts"])
distinct_df = spark.createDataFrame(distinct_counts, ["table_name", "distinct_row_counts"])

# Join and calculate difference
result_df = normal_df.join(distinct_df, on="table_name") \
    .withColumn("counts_difference", normal_df.row_counts - distinct_df.distinct_row_counts)

# Show the result
result_df.orderBy("table_name").show(truncate=False)
result_df.write.mode("overwrite").saveAsTable("BronzeLakehouse.Cl_bronze_count_comparison")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
