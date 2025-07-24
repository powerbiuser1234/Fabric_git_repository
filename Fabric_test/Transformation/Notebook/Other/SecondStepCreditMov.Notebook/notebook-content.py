# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f0cb0e22-356b-4bf1-81ce-5167c0a5a785",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "d1ef06f0-6f40-4597-b69c-c056f928168c",
# META       "known_lakehouses": [
# META         {
# META           "id": "b554114f-f6f1-42fa-b182-35c9b03d7bfd"
# META         },
# META         {
# META           "id": "f8f7fa37-8257-4a7c-944d-0dd60f2f250b"
# META         },
# META         {
# META           "id": "f0cb0e22-356b-4bf1-81ce-5167c0a5a785"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import trim, regexp_replace, round as spark_round, col, when
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# cr_movt2024_df = spark.table("Lakehouse.CR_MOVT2024")
# max_dco_df = cr_movt2024_df.select(max(to_date("dco", "dd/MM/yyyy")).alias("dco"))
# max_dco_df.write.mode("overwrite").saveAsTable("Lakehouse.maxdco_CR_MOVT2024")

# maxdco_CR_MOVT2024=spark.read.format("delta").table("Lakehouse.maxdco_CR_MOVT2024")
# max_dco = maxdco_CR_MOVT2024.select("dco").first()["dco"]


crmov1typ21 = spark.read.table("Lakehouse.crmovtype21").filter((col("Type") == '1'))
crmov2typ21 = spark.read.table("Lakehouse.crmovtype21").filter((col("Type") == '2'))
crmov3typ21 = spark.read.table("Lakehouse.crmovtype21").filter((col("Type") == '3'))
crmov41typ21 = spark.read.table("Lakehouse.crmovtype21").filter((col("Type") == '4'))
crmov52typ21 = spark.read.table("Lakehouse.crmovtype21").filter((col("Type") == '5'))
crmov6typ21 = spark.read.table("Lakehouse.crmovtype21").filter((col("Type") == '6'))


etxd_df = spark.read.format("delta").table("Lakehouse.Bktau_CM")
dtxd_df = etxd_df.filter((col("dev") == '840') & (col("tac") != 0))
xyz_df = spark.read.format("delta").table("Lakehouse.Bkcom_CM").selectExpr("cli as comcli", "cpro", "ncp as comncp","DEV","AGE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ab_df = crmov1typ21.union(crmov2typ21) \
    .union(crmov3typ21) \
    .union(crmov41typ21) \
    .union(crmov52typ21) \
    .union(crmov6typ21) \
    


joined_df = (
    ab_df.alias("ab")
    .join(etxd_df.alias("etxd"), (col("etxd.dco") == col("ab.dte")) & (col("etxd.dev") == col("ab.dev")))
    .join(dtxd_df.alias("dtxd"), col("dtxd.dco") == col("ab.dte")) 
    .join(xyz_df.alias("xyz"),(col("xyz.comncp") == col("ab.ncp")) & (col("xyz.DEV") == col("ab.DEV")) & (col("xyz.AGE") == col("ab.AGE")))
    .filter((trim(col("xyz.cpro")) != '560') & (trim(col("xyz.cpro")) != '') & (col("etxd.tac") != 0))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df = joined_df.select(
    "ab.DC", 
    "ab.DCO",
    "ab.DAG",
    "ab.DVA",
    "ab.NCP",
    "ab.NC",
    trim("ab.CLI").alias("cli"),
    regexp_replace("ab.INTI", r"[^[:print:]].* ", "").alias("INTI"),
    "ab.M_AGE",
    "ab.NCC", 
    "ab.RLET", 
    "ab.LIB",
    "ab.UTI", 
    "ab.UTA", 
    "ab.UTF", 
    "ab.AGSA", 
    "ab.MCTV", 
    "ab.AGE", 
    "ab.CHA",
    "ab.DEV",
    "ab.MON",
    "ab.SEN", 
    "ab.DTE",
    "ab.OPE", 
    "ab.NAT", 
    "ab.TYP",
    "ab.PIE",
    "ab.PIEO",
    "ab.REFDOS", 
    "ab.MODU", 
    "ab.EVE",
    "ab.DEVC",
    when(col("ab.dev") == '840',
    spark_round(col("ab.mon"))).otherwise(spark_round((col("ab.mon") * col("etxd.tind")) / col("dtxd.tind"))).alias("cvusd"),
    "ab.NOSEQ"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df = result_df \
    .withColumn("MCTV", col("MCTV").cast("decimal(38,18)")) \
    .withColumn("MON", col("MON").cast("decimal(38,18)"))\
    .withColumn("cvusd", col("cvusd").cast("decimal(38,18)"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df.write.format("delta").mode("overwrite").saveAsTable("Lakehouse.CR_MOVT2024")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# max_dco_dbt = (
#     spark.read.table("BronzeLakehouse.dbt_mov24")
#     .agg({"dco": "max"})
#     .withColumnRenamed("max(dco)", "dco")
#     .first()["dco"]
# )

# new_dbt_df = (
#     spark.read.table("BronzeLakehouse.cdt_dbt_mov_2021")
#     .filter((col("Sen") == "C") & (to_date(col("dco"), "dd/MM/yyyy") > lit(max_dco_dbt)))
#     .selectExpr("cli", "dco", "pie", "trim(uti) as uti", "ope")
# )

# new_dbt_df.write.mode("append").saveAsTable("BronzeLakehouse.dbt_mov24")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
