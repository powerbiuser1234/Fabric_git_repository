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
# META           "id": "f0cb0e22-356b-4bf1-81ce-5167c0a5a785"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import trim, col, concat_ws, date_format, lit,to_date, max as spark_max, broadcast

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Get max dco from target
# cdt_dbt_mov_df = spark.read.format("delta").table("BronzeLakehouse.cdt_dbt_mov_2021")
# max_dco = cdt_dbt_mov_df.select(spark_max("dco").alias("max_dco")).first()["max_dco"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Step 2: Load bkcom (small dimension table)
# max_batch_date = spark.read.format("delta").table("BronzeLakehouseHistory.Cbs_Bkcom")
# max_batch_date = max_batch_date.selectExpr("cast(max(BatchDATE) as date) as max_date").collect()[0]["max_date"]
# com_df = spark.read.format("delta").table("BronzeLakehouseHistory.Cbs_Bkcom").filter(to_date(col("BatchDATE")) == max_batch_date)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

com_df = spark.read.format("delta").table("Lakehouse.Bkcom_CM")
his_df = spark.read.format("delta").table("Lakehouse.Bkhis_CM")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Step 3: Load and filter bkhis_daily for CREDITS (sen = 'C')
# his_credit_df = (
#     spark.read.format("delta").table("BronzeLakehouseHistory.Cbs_Bkhis")
#     .filter((col("dco") > lit(max_dco)) & (col("sen") == "C"))
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Step 4: Load and filter bkhis_daily for DEBITS (sen = 'D')
# his_debit_df = (
#     spark.read.format("delta").table("BronzeLakehouseHistory.Cbs_Bkhis")
#     .filter((col("dco") > lit(max_dco)) & (col("sen") == "D"))
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 5: Join credits with com
credit_joined = (
    his_df.alias("his")
    .join(
        broadcast(com_df.alias("com")),
        (trim(col("com.ncp")) == trim(col("his.ncp"))) &
        (trim(col("com.age")) == trim(col("his.age"))) &
        (trim(col("com.dev")) == trim(col("his.dev"))) &
        (col("his.sen") == "C"),
        how="inner"
    )
    .select(
        date_format("his.dco", "MM/yyyy").alias("DC"),
        col("his.dco").alias("DCO"),
        col("his.dag").alias("DAG"),
        col("his.dva").alias("DVA"),
        trim(col("com.ncp")).alias("NCP"),
        concat_ws("-", trim(col("com.ncp")), trim(col("com.clc"))).alias("NC"),
        trim(col("com.cli")).alias("CLI"),
        trim(col("com.inti")).alias("INTI"),
        trim(col("his.age")).alias("M_AGE"),
        trim(col("his.ncc")).alias("NCC"),
        trim(col("his.rlet")).alias("RLET"),
        trim(col("his.lib")).alias("LIB"),
        col("his.uti").alias("UTI"),
        col("his.uta").alias("UTA"),
        col("his.utf").alias("UTF"),
        trim(col("his.agsa")).alias("AGSA"),
        col("his.mctv").alias("MCTV"),
        trim(col("com.age")).alias("AGE"),
        trim(col("com.cha")).alias("CHA"),
        col("his.dev").alias("DEV"),
        col("his.mon").alias("MON"),
        col("his.sen").alias("SEN"),
        col("his.dco").alias("DTE"),
        trim(col("his.ope")).alias("OPE"),
        trim(col("his.nat")).alias("NAT"),
        trim(col("com.typ")).alias("TYP"),
        trim(col("his.pie")).alias("PIE"),
        trim(col("his.pieo")).alias("PIEO"),
        trim(col("his.refdos")).alias("REFDOS"),
        trim(col("his.modu")).alias("MODU"),
        trim(col("his.eve")).alias("EVE"),
        col("his.devc").alias("DEVC"),
        col("his.dco").alias("WEEK_END_DT"),
        col("his.NOSEQ").alias("NOSEQ")
    ).distinct()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 8: Join with weekend_dates
weekend_df = spark.read.format("delta").table("Lakehouse.Weekend_Dates").alias("wk")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

credit_joined = credit_joined.alias("cdt")
weekend_df = weekend_df.alias("wk")

credit_joined = credit_joined.join(
    weekend_df,
    col("cdt.DCO") == col("wk.dco"),
    how="inner"
).select("cdt.*")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 6: Join debits with com
debit_joined = (
    his_df.alias("his")
    .join(
        broadcast(com_df.alias("com")),
        (trim(col("com.ncp")) == trim(col("his.ncp"))) &
        (trim(col("com.age")) == trim(col("his.age"))) &
        (trim(col("com.dev")) == trim(col("his.dev"))) &
        (col("his.sen") == "D"),
        how="inner"
    )
    .select(
        date_format("his.dco", "MM/yyyy").alias("DC"),
        col("his.dco").alias("DCO"),
        col("his.dag").alias("DAG"),
        col("his.dva").alias("DVA"),
        trim(col("com.ncp")).alias("NCP"),
        concat_ws("-", trim(col("com.ncp")), trim(col("com.clc"))).alias("NC"),
        trim(col("com.cli")).alias("CLI"),
        trim(col("com.inti")).alias("INTI"),
        trim(col("his.age")).alias("M_AGE"),
        trim(col("his.ncc")).alias("NCC"),
        trim(col("his.rlet")).alias("RLET"),
        trim(col("his.lib")).alias("LIB"),
        col("his.uti").alias("UTI"),
        col("his.uta").alias("UTA"),
        col("his.utf").alias("UTF"),
        trim(col("his.agsa")).alias("AGSA"),
        col("his.mctv").alias("MCTV"),
        trim(col("com.age")).alias("AGE"),
        trim(col("com.cha")).alias("CHA"),
        col("his.dev").alias("DEV"),
        col("his.mon").alias("MON"),
        col("his.sen").alias("SEN"),
        col("his.dco").alias("DTE"),
        trim(col("his.ope")).alias("OPE"),
        trim(col("his.nat")).alias("NAT"),
        trim(col("com.typ")).alias("TYP"),
        trim(col("his.pie")).alias("PIE"),
        trim(col("his.pieo")).alias("PIEO"),
        trim(col("his.refdos")).alias("REFDOS"),
        trim(col("his.modu")).alias("MODU"),
        trim(col("his.eve")).alias("EVE"),
        col("his.devc").alias("DEVC"),
        col("his.dco").alias("WEEK_END_DT"),
        col("his.NOSEQ").alias("NOSEQ")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

debit_joined = debit_joined.alias("cdt")
weekend_df = weekend_df.alias("wk")

debit_joined = debit_joined.join(
    weekend_df,
    col("cdt.DCO") == col("wk.dco"),
    how="inner"
).select("cdt.*")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# Step 7: Union both
final_df = credit_joined.union(debit_joined).alias("cdt")

# Step 9: Write to single target table
final_df.write.format("delta").mode("overwrite").saveAsTable("Lakehouse.cdt_dbt_mov_2021")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, trim, max as spark_max

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Step 1: Get max dco from target table (crmov1typ21)
# crmov_df = spark.read.format("delta").table("BronzeLakehouse.crmovtype21").filter(col("Type") == lit("1"))
# max_crmov_dco = crmov_df.select(spark_max("dco").alias("max_dco")).first()["max_dco"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Separate credit and debit subsets from unified dataframe
crmov_credit_df = final_df.filter(
    (col("SEN") == "C") &
    (col("OPE") == "021")
    # (col("DCO") > lit(max_crmov_dco))
)

crmov_debit_df = final_df.filter(
    (col("SEN") == "D") &
    (col("OPE") == "021") &
    # (col("DCO") > lit(max_crmov_dco)) &
    (~col("NCP").isin(
        "70099999754", "70009999926", "70009999924", "70009999927", "70009999928"
    )) &
    (col("NCP") == "45190100099")
).select("PIE", "MON", "DCO")  # Only needed fields for join

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4: Join credit and debit data on PIE, MCTV=MON, DCO
crmov1typ21_df = (
    crmov_credit_df.alias("cr")
    .join(
        crmov_debit_df.alias("mov"),
        (col("cr.PIE") == col("mov.PIE")) &
        (col("cr.MCTV") == col("mov.MON")) &
        (col("cr.DCO") == col("mov.DCO")),
        how="inner"
    )
    .select(
        col("cr.DC"),
        col("cr.DCO"),
        col("cr.DAG"),
        col("cr.DVA"),
        col("cr.NCP"),
        col("cr.NC"),
        col("cr.CLI"),
        col("cr.INTI"),
        col("cr.M_AGE"),
        col("cr.NCC"),
        col("cr.RLET"),
        col("cr.LIB"),
        col("cr.UTI"),
        col("cr.UTA"),
        col("cr.UTF"),
        col("cr.AGSA"),
        col("cr.MCTV"),
        col("cr.AGE"),
        col("cr.CHA"),
        col("cr.DEV"),
        col("cr.MON"),
        col("cr.SEN"),
        col("cr.DTE"),
        col("cr.OPE"),
        col("cr.NAT"),
        col("cr.TYP"),
        col("cr.PIE"),
        col("cr.PIEO"),
        col("cr.REFDOS"),
        col("cr.MODU"),
        col("cr.EVE"),
        col("cr.DEVC"),
        col("cr.WEEK_END_DT"),
        col("cr.NOSEQ"),
        lit("1").alias("Type")
    )  # select only credit side for insert
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import trim, col, substring, max as spark_max, broadcast, lit


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Step 1: Load max dco from crmov2typ21
# crmov2_df = spark.read.format("delta").table("BronzeLakehouse.crmovtype21").filter(col("Type") == lit("2"))
# max_crmov2_dco = crmov2_df.select(spark_max("dco").alias("max_dco")).first()["max_dco"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2: Load mov_sin_2021 and separate credit & debit
crmov2_credit_df = final_df.filter((col("sen") == "C") 
# & (col("dco") > lit(max_crmov2_dco))
)

# Step 3: Build mov_cdt_ncc DataFrame (DON'T write to table)
crmov2_debit_df = final_df.filter((col("sen") == "D") 
# & (col("dco") > lit(max_crmov2_dco))
)

crmov2_mov_cdt_ncc_df = (
    crmov2_debit_df.alias("mov")
    .join(broadcast(com_df.alias("com")), col("mov.ncc") == col("com.ncp"), how="left")
    .select("mov.*", col("com.cli").alias("cli_cli"))
)

# Step 4: Define the exclusion list for ope
crmov2_excluded_ope_list = [
    '088','038','110','222','223','224','228','229','232','234','328','329',
    '383','387','391','392','393','423','424','428','431','433','458','459',
    '468','700','702','705','742','744','082','227','021','204','727','797','798','799'
]

# Step 5: Build pie exclusion set (anti-join)
crmov2_mov_cdt_ncc_filtered = crmov2_mov_cdt_ncc_df.filter(
    (substring("ope", 1, 1) != "8") &
    (~col("ope").isin(crmov2_excluded_ope_list)) &
    (substring("lib", 1, 5) != "FRAIS")
)

# Build PIEs to exclude based on complex condition
crmov2_excluded_pies_df = (
    crmov2_mov_cdt_ncc_filtered.alias("mov")
    .select("pie", "cli", "cli_cli", "dco", "lib")
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 6: Filter credit_df based on full condition
crmov2type21_df = (
    crmov2_credit_df.alias("cr")
    .filter(
        (trim(col("uti")) != "WEBR") &
        (substring("lib", 1, 3) != "EXT") &
        (substring("ope", 1, 1) != "8") &
        (~col("ope").isin(crmov2_excluded_ope_list)) &
        (col("cli").isNotNull()) & (col("cli") != "")
    )
    .join(
        crmov2_excluded_pies_df.alias("mov"),
        (col("cr.pie") == col("mov.pie")) &
        (col("cr.cli") == col("mov.cli")) &
        (col("cr.cli") == col("mov.cli_cli")) &
        (col("cr.dco") == col("mov.dco")),
        how="left_anti"  # exclude matched pies
    )
    .select(
        col("cr.DC"),
        col("cr.DCO"),
        col("cr.DAG"),
        col("cr.DVA"),
        col("cr.NCP"),
        col("cr.NC"),
        col("cr.CLI"),
        col("cr.INTI"),
        col("cr.M_AGE"),
        col("cr.NCC"),
        col("cr.RLET"),
        col("cr.LIB"),
        col("cr.UTI"),
        col("cr.UTA"),
        col("cr.UTF"),
        col("cr.AGSA"),
        col("cr.MCTV"),
        col("cr.AGE"),
        col("cr.CHA"),
        col("cr.DEV"),
        col("cr.MON"),
        col("cr.SEN"),
        col("cr.DTE"),
        col("cr.OPE"),
        col("cr.NAT"),
        col("cr.TYP"),
        col("cr.PIE"),
        col("cr.PIEO"),
        col("cr.REFDOS"),
        col("cr.MODU"),
        col("cr.EVE"),
        col("cr.DEVC"),
        col("cr.WEEK_END_DT"),
        col("cr.NOSEQ"),
        lit("2").alias("Type")
    )  # select only credit side for insert
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql.functions import trim, col, lit, max as spark_max

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Step 1: Load max dco from crmov2typ21
# crmov3_df = spark.read.format("delta").table("BronzeLakehouse.crmovtype21").filter(col("Type") == lit("3"))
# max_crmov3_dco = crmov3_df.select(spark_max("dco").alias("max_dco")).first()["max_dco"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Filter credit records (cr)
crmov3_credit_df = final_df.filter(
    (col("sen") == "C") &
    # (col("dco") > lit(max_crmov3_dco)) &
    (trim(col("uti")) == "WEBR") &
    (col("cli").isNotNull())
)

# Step 4: Filter debit records (dr) for exclusion condition
crmov3_debit_df = final_df.filter(
    (col("sen") == "D") &
    # (col("dco") > lit(max_crmov3_dco)) &
    (trim(col("uti")) == "WEBR")
).select("pie", "cli")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 5: Exclude credit records with PIE/CLI match in debit (anti-join)
crmov3type21_df = (
    crmov3_credit_df.alias("cr")
    .join(
        crmov3_debit_df.alias("dr"),
        (col("cr.pie") == col("dr.pie")) & (col("cr.cli") == col("dr.cli")),
        how="left_anti"  # keep only records that don't match the exclusion
    )
    .select(
        col("cr.DC"),
        col("cr.DCO"),
        col("cr.DAG"),
        col("cr.DVA"),
        col("cr.NCP"),
        col("cr.NC"),
        col("cr.CLI"),
        col("cr.INTI"),
        col("cr.M_AGE"),
        col("cr.NCC"),
        col("cr.RLET"),
        col("cr.LIB"),
        col("cr.UTI"),
        col("cr.UTA"),
        col("cr.UTF"),
        col("cr.AGSA"),
        col("cr.MCTV"),
        col("cr.AGE"),
        col("cr.CHA"),
        col("cr.DEV"),
        col("cr.MON"),
        col("cr.SEN"),
        col("cr.DTE"),
        col("cr.OPE"),
        col("cr.NAT"),
        col("cr.TYP"),
        col("cr.PIE"),
        col("cr.PIEO"),
        col("cr.REFDOS"),
        col("cr.MODU"),
        col("cr.EVE"),
        col("cr.DEVC"),
        col("cr.WEEK_END_DT"),
        col("cr.NOSEQ"),
        lit("3").alias("Type")
    )  # select only credit side for insert
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Step 1: Get max dco from crmov41typ21
# crmov41_df = spark.read.format("delta").table("BronzeLakehouse.crmovtype21").filter(col("Type") == lit("4"))
# max_dco = crmov41_df.select(spark_max("dco").alias("max_dco")).first()["max_dco"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Filter credit records (cr)
crmov41_credit_df = final_df.filter(
    (col("sen") == "C") &
    # (col("dco") > lit(max_dco)) &
    (col("ope") == "204") &
    (col("cli").isNotNull())
)

# Step 4: Filter debit records for exclusion
crmov41_debit_df = final_df.filter(
    (col("sen") == "D") &
    # (col("dco") > lit(max_dco)) &
    (col("cli").isNotNull())
).select(
    trim(col("cli")).alias("cli"),
    trim(col("pie")).alias("pie"),
    col("dco")
).distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 5: Anti-join based on cli, pie = pieo, dco
crmov41_filtered_df = (
    crmov41_credit_df.alias("cr")
    .join(
        crmov41_debit_df.alias("dr"),
        (trim(col("cr.cli")) == col("dr.cli")) &
        (trim(col("cr.pieo")) == col("dr.pie")) &
        (col("cr.dco") == col("dr.dco")),
        how="left_anti"
    )
)

# Step 6: Replace RLET, LIB, EVE with blank strings
crmov41type21_df = crmov41_filtered_df.select(
    "DC", "DCO", "DAG", "DVA", "NCP", "NC", "CLI", "INTI", "M_AGE", "NCC",
    lit(" ").alias("RLET"),
    lit(" ").alias("LIB"),
    "UTI", "UTA", "UTF", "AGSA", "MCTV", "AGE", "CHA", "DEV", "MON", "SEN",
    "DTE", "OPE", "NAT", "TYP", "PIE", "PIEO", "REFDOS", "MODU",
    lit(" ").alias("EVE"),
    "DEVC", "WEEK_END_DT", "NOSEQ",
    lit("4").alias("Type")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, trim, lit, max as spark_max, substring

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Step 1: Get max DCO from crmov52typ21
# crmov52_df = spark.read.format("delta").table("BronzeLakehouse.crmovtype21").filter(col("Type") == lit("5"))
# max_dco = crmov52_df.select(spark_max("dco").alias("max_dco")).first()["max_dco"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Prepare credit data (cr)
crmov52_credit_df = final_df.filter(
    (col("sen") == "C") &
    # (col("dco") > lit(max_dco)) &
    (trim(col("uti")) != "WEBR") &
    (substring("lib", 1, 3) != "EXT") &
    (substring("ope", 1, 1) != "8") &
    (~col("ope").isin(
        '088','038','110','222','223','224','228','229','232','234','328',
        '329','383','387','391','392','393','423','424','428','431','433',
        '458','459','468','700','702','705','742','744','082','227','021',
        '204','727','797','798','799'
    )) &
    (col("cli").isNotNull())
)

# Step 4: Prepare simulated mov_cdt_ncc (debit side + join with bkcom)
crmov52_debit_df = final_df.filter(
    (col("sen") == "D")
    # (col("dco") > lit(max_dco))
)

crmov52_mov_cdt_ncc_df = (
    crmov52_debit_df.alias("mov")
    .join(com_df.alias("com"), col("mov.ncc") == col("com.ncp"), how="left")
    .select("mov.*", col("com.cli").alias("cli_cli"))
)

# Step 5: Apply filtering to build exclusion PIE list
crmov52_exclusion_df = crmov52_mov_cdt_ncc_df.filter(
    (substring("ope", 1, 1) != "8") &
    (~col("ope").isin(
        '088','038','110','222','223','224','228','229','232','234','328',
        '329','383','387','391','392','393','423','424','428','431','433',
        '458','459','468','700','702','705','742','744','082','227','021',
        '204','727','797','798','799'
    )) &
    (substring("lib", 1, 5) != "FRAIS")
).select(
    col("pie"), col("cli"), col("cli_cli"), col("dco"), col("mon")
).distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 6: Anti-join (exclude matching pies)
crmov52type21_df = (
    crmov52_credit_df.alias("cr")
    .join(
        crmov52_exclusion_df.alias("mov"),
        (col("cr.pie") == col("mov.pie")) &
        (col("cr.cli") == col("mov.cli")) &
        (col("cr.cli") == col("mov.cli_cli")) &
        (col("cr.dco") == col("mov.dco")) &
        (col("cr.mon") <= col("mov.mon")),
        how="left_anti"
    )
    .select(
        col("cr.DC"),
        col("cr.DCO"),
        col("cr.DAG"),
        col("cr.DVA"),
        col("cr.NCP"),
        col("cr.NC"),
        col("cr.CLI"),
        col("cr.INTI"),
        col("cr.M_AGE"),
        col("cr.NCC"),
        col("cr.RLET"),
        col("cr.LIB"),
        col("cr.UTI"),
        col("cr.UTA"),
        col("cr.UTF"),
        col("cr.AGSA"),
        col("cr.MCTV"),
        col("cr.AGE"),
        col("cr.CHA"),
        col("cr.DEV"),
        col("cr.MON"),
        col("cr.SEN"),
        col("cr.DTE"),
        col("cr.OPE"),
        col("cr.NAT"),
        col("cr.TYP"),
        col("cr.PIE"),
        col("cr.PIEO"),
        col("cr.REFDOS"),
        col("cr.MODU"),
        col("cr.EVE"),
        col("cr.DEVC"),
        col("cr.WEEK_END_DT"),
        col("cr.NOSEQ"),
        lit("5").alias("Type")
    )  # select only credit side for insert
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, max as spark_max

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Step 1: Get max DCO from crmov6typ21
# crmov6_df = spark.read.format("delta").table("BronzeLakehouse.crmovtype21").filter(col("Type") == lit("6"))
# max_dco = crmov6_df.select(spark_max("dco").alias("max_dco")).first()["max_dco"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2: Filter credit records for this job
crmov6_filtered_df = final_df.filter(
    (col("sen") == "C") &
    (col("ope") == "613") &
    (col("lib").contains("RAMASSAGE")) &
    (col("cli").isNotNull())
    # (col("dco") > lit(max_dco))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Select and transform the required columns
crmov6type21_df = (
    crmov6_filtered_df.alias("cr").select(
        col("cr.DC"),
        col("cr.DCO"),
        col("cr.DAG"),
        col("cr.DVA"),
        col("cr.NCP"),
        col("cr.NC"),
        col("cr.CLI"),
        col("cr.INTI"),
        col("cr.M_AGE"),
        col("cr.NCC"),
        col("cr.RLET"),
        col("cr.LIB"),
        col("cr.UTI"),
        col("cr.UTA"),
        col("cr.UTF"),
        col("cr.AGSA"),
        col("cr.MCTV"),
        col("cr.AGE"),
        col("cr.CHA"),
        col("cr.DEV"),
        col("cr.MON"),
        col("cr.SEN"),
        col("cr.DTE"),
        col("cr.OPE"),
        col("cr.NAT"),
        col("cr.TYP"),
        col("cr.PIE"),
        col("cr.PIEO"),
        col("cr.REFDOS"),
        col("cr.MODU"),
        col("cr.EVE"),
        col("cr.DEVC"),
        col("cr.WEEK_END_DT"),
        col("cr.NOSEQ"),
        lit("6").alias("Type")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === Union all result sets ===
crmov_unioned_result_set = crmov1typ21_df.unionByName(crmov2type21_df) \
                                         .unionByName(crmov3type21_df) \
                                         .unionByName(crmov41type21_df) \
                                         .unionByName(crmov52type21_df) \
                                         .unionByName(crmov6type21_df)

# === Write to unified table ===
crmov_unioned_result_set.write.format("delta").mode("overwrite").saveAsTable("Lakehouse.crmovtype21")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
