# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ###  To fetch all view def at once 

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
