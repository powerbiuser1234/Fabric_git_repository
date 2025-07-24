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

from pyspark.sql.functions import *

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "Legacy")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

maxdco_tmp_cr_mov01_05_jan=spark.read.format("delta").table("Lakehouse.tmp_cr_mov01_05_jan")
CR_MOVT2024=spark.read.format("delta").table("Lakehouse.CR_MOVT2024")
bkdopi_rpt=spark.read.format("delta").table("Lakehouse.Bkdopi_rpt_CM")
cdt_dbt_mov_2021=spark.read.format("delta").table("Lakehouse.cdt_dbt_mov_2021").filter(col("SEN") == 'D')
bkcli=spark.read.format("delta").table("Lakehouse.Bkcli_CM")
bknom=spark.read.format("delta").table("Lakehouse.Bknom_CM")
temp_com_cli=spark.read.format("delta").table("Lakehouse.temp_com_cli_CM")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

max_dco = maxdco_tmp_cr_mov01_05_jan.selectExpr("max(to_date(dco, 'dd/MM/yyyy')) as dco").first()['dco']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bkcli_filtered = bkcli.filter(col("seg").isin('002','003','005','027','0d	01','004'))

bkcli_filtered_201 = bkcli.filter(col("seg").isin('002','003','005','027','001','004'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

crmov = CR_MOVT2024.alias("a") \
    .join(bkcli_filtered.alias("cli"), col("a.cli") == trim(col("cli.cli"))) \
    .filter(
        # (col("dco") > lit(max_dco)) &
        (~col("ncp").isin('70018038001', '70018038002', '70518038001', '70518038002')) 
        & (col("ope") != '201')
    ) \
    .select("a.*")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

crmov201 = CR_MOVT2024.alias("a") \
    .join(bkcli_filtered_201.alias("cli"), col("a.cli") == trim(col("cli.cli"))) \
    .filter(
        # (col("dco") > lit(max_dco)) &
        (~col("ncp").isin('70018038001', '70018038002', '70518038001', '70518038002')) &
        (col("ope") == '201')
    ) \
    .select("a.*")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dopi = bkdopi_rpt.alias("a") \
    .join(temp_com_cli.alias("b"), col("a.ncpbf") == col("b.ncp")) \
    .join(bkcli_filtered_201.alias("cli"), col("b.cli") == trim(col("cli.cli")), how="left") \
    .filter((col("dexec") > lit(max_dco)) & (col("modu") == 'RPT')) \
    .selectExpr(" eve", "dexec", "nomdo", "ndos").distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dopi = bkdopi_rpt.alias("a") \
#     .join(temp_com_cli.alias("b"), col("a.ncpbf") == col("b.ncp")) \
#     .join(bkcli_filtered_201.alias("cli"), col("b.cli") == trim(col("cli.cli")), how="left") \
#     .filter((col("dexec") > lit(max_dco)) & (col("modu") == 'RPT')) \
#     .selectExpr(" eve", "dexec", "nomdo", "ndos").distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cbtmov = cdt_dbt_mov_2021 \
    .withColumn(
        "db_cli",
        when(col("NCP").isin('70018038001', '70018038002', '70518038001', '70518038002'), ' 00180380')
        .when(col("OPE") == '676', '00000000')
        .otherwise(col("cli"))
    ) \
    .selectExpr("cli", "dco", "pie", "pieo", "trim(uti) as uti", "db_cli").distinct()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# cbtmov = cdt_dbt_mov_2021 \
#     .filter(col("dco") > lit(max_dco)) \
#     .withColumn(
#         "db_cli",
#         when(col("NCP").isin('70018038001', '70018038002', '70518038001', '70518038002'), ' 00180380')
#         .when(col("OPE") == '676', '00000000')
#         .otherwise(col("cli"))
#     ) \
#     .selectExpr("cli", "dco", "pie", "pieo", "trim(uti) as uti", "db_cli").distinct()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# Aliases
cr = crmov.alias("crmov")
cb = cbtmov.alias("cbtmov")
dp = dopi.alias("dopi")

# Perform the joins with fully qualified column names
joined_df = cr \
    .join(cb, 
          (col("crmov.dco") == col("cbtmov.dco")) &
          ((col("crmov.pieo") == col("cbtmov.pie")) | (col("crmov.pie") == col("cbtmov.pie"))) &
          (col("crmov.cli") != col("cbtmov.db_cli")), 
          how="left_outer") \
    .join(dp,
          (col("crmov.eve") == col("dopi.eve")) &
          (col("dopi.ndos") == substring(col("crmov.refdos"), 4, 12)),
          how="left_outer")

# Apply filter condition using qualified columns
filtered_df = joined_df.filter(
    col("cbtmov.db_cli").isNotNull() & 
    (~col("crmov.ope").isin("902", "909", "201", "179", "C44"))
)

# Select and transform the required columns
union_1_df = filtered_df.select(
    col("crmov.m_age").alias("AGE"),
    col("crmov.nc").alias("NC"),
    date_format(col("crmov.dco"), "dd/MM/yyyy").alias("DCO"),
    date_format(col("crmov.dva"), "dd/MM/yyyy").alias("DVA"),
    col("crmov.dev").alias("DEV"),
    trim(col("crmov.inti")).alias("INTI"),
    col("crmov.ope").alias("OPE"),
    trim(col("crmov.lib")).alias("NARAT"),
    col("crmov.mon").alias("CREDIT"),
    col("crmov.pie").alias("PIE"),
    col("crmov.pieo").alias("PIEO"),
    col("crmov.eve").alias("CHEQUENO"),
    when(col("crmov.modu") == "RPT", col("crmov.refdos"))
    .when(col("cbtmov.db_cli") == "00099987", "Visa Acquiring")
    .when(col("cbtmov.db_cli") == "00999999", "Master Card Acquiring").otherwise(col("cbtmov.db_cli")).alias("DEBIT_CLI"),
    col("dopi.nomdo").alias("NOMDO"),
    col("dopi.ndos").alias("NDOS"),
    col("cvusd").alias("CVUSD")
).distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# Filter the DataFrame based on the specified OPE values
filtered_df = crmov.filter(
    col("ope").isin(
        '179', '004', '008', '013', '021', '025', '026', '027',
        '028', '029', '049', '054', '071', '072', '078', '098', '214',
        '309', '343', '344', '349', '408', '613', '618', '619', '625',
        '637', '639', '723', '724', '734', '735', '737', '738',
        '668', '765', 'N01', '409', '005', '909'
    )
)

# Select and transform the columns
union_2_df = filtered_df.select(
    col("m_age").alias("AGE"),
    col("nc").alias("NC"),
    date_format("dco", "dd/MM/yyyy").alias("DCO"),
    date_format("dva", "dd/MM/yyyy").alias("DVA"),
    col("dev").alias("DEV"),
    trim(col("inti")).alias("INTI"),
    col("ope").alias("OPE"),
    trim(col("lib")).alias("NARAT"),
    col("mon").alias("CREDIT"),
    col("pie").alias("PIE"),
    col("pieo").alias("PIEO"),
    col("eve").alias("CHEQUENO"),
    when(col("ope") == "179", "RTGS").otherwise("Cash Deposit").alias("DEBIT_CLI"),
    lit(None).cast("string").alias("NOMDO"),
    lit(None).cast("string").alias("NDOS"),
    col("cvusd").alias("CVUSD")
).distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, trim, when, date_format, lit

# Aliases for tables
cr = crmov201.alias("cr")
db = cbtmov.alias("db")

# Join with conditions
joined_df = cr.join(
    db,
    (col("cr.dco") == col("db.dco")) &
    ((col("cr.pieo") == col("db.pie")) | (col("cr.pie") == col("db.pie"))) &
    (trim(col("cr.uti")) == col("db.uti")) &
    (col("cr.cli") != col("db.db_cli")),
    how="left_outer"
)

# Filter the rows
filtered_df = joined_df.filter(
    (col("db.db_cli").isNotNull()) & 
    (col("cr.ope") == "201")
)

# Select and transform columns
union_3_df = filtered_df.select(
    col("cr.m_age").alias("AGE"),
    col("cr.nc").alias("NC"),
    date_format(col("cr.dco"), "dd/MM/yyyy").alias("DCO"),
    date_format(col("cr.dva"), "dd/MM/yyyy").alias("DVA"),
    col("cr.dev").alias("DEV"),
    trim(col("cr.inti")).alias("INTI"),
    col("cr.ope").alias("DVA"),
    trim(col("cr.lib")).alias("NARAT"),
    col("cr.mon").alias("CREDIT"),
    col("cr.pie").alias("PIE"),
    col("cr.pieo").alias("PIEO"),
    col("cr.eve").alias("CHEQUENO"),
    when(col("cr.modu") == "RPT", col("cr.refdos"))
    .when(col("db.db_cli") == "00099987", lit("Visa Acquiring"))
    .when(col("db.db_cli") == "00999999", lit("Master Card Acquiring"))
    .otherwise(col("db.db_cli")).alias("DEBIT_CLI"),
    lit(None).cast("string").alias("NOMDO"),
    lit(None).cast("string").alias("NDOS"),
    col("cvusd").alias("CVUSD")
).distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_union_df = union_1_df.union(union_2_df).union(union_3_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, upper, when, trim, substring

# Prepare reference DataFrames
clie_df = (
    bkcli
    .select(trim(col("cli")).alias("clie"), trim(col("nomrest")).alias("DEBIT_CLI_NOM"))
    .alias("clie")
)

dv_df = (
    bknom
    .filter(col("ctab") == '005')
    .select(trim(col("cacc")).alias("cacc"), col("ctab"), trim(col("lib2")).alias("dv"))
    .alias("dv")
)

# Apply transformations and joins
final_result_df = (
    final_union_df.alias("mvmt")
    .join(clie_df, col("mvmt.DEBIT_CLI") == col("clie.clie"), "left_outer")
    .join(dv_df, col("mvmt.DEV") == col("dv.cacc"), "left_outer")
    .filter(~substring(col("nc"), 1, 11).isin('70018038001', '70018038002', '70518038001', '70518038002'))
    .select(
        col("DCO").alias("DATE_COMPTE"),
        col("DVA").alias("VALUE_DATE"),
        col("AGE"),
        col("NC").alias("COMPTE"),
        col("dv").alias("DEV"),
        col("INTI"),
        col("OPE"),
        col("NARAT").alias("NARRATION"),
        col("CREDIT"),
        col("CHEQUENO"),
        col("DEBIT_CLI"),
        when(upper(col("NARAT")).like("RAP%"), col("NOMDO"))
        .when(col("DEBIT_CLI") == "00180380", "suspens account vodacash")
        .otherwise(col("DEBIT_CLI_NOM"))
        .alias("DEBIT_CLI_NOM"),
        when(upper(col("NARAT")).like("VERS%"), "VERSEMENT")
        .when(upper(col("NARAT")).like("VIR%"), "VIREMENT")
        .when(upper(col("NARAT")).like("RAP%"), "RAPATRIEMENT")
        .when(upper(col("NARAT")).like("RAWBANK%"), "RAWBANK")
        .when(upper(col("NARAT")).like("RWB%"), "RAWBANK")
        .when(upper(col("NARAT")).like("REMIS%"), "REMISE CHQ")
        .when(upper(col("NARAT")).like("RRN%"), "POS").otherwise("Others").alias("TRX_TYP"),
        col("PIE"),
        col("PIEO"),
        col("cvusd").alias("CVUSD"),
        col("dco").alias("DCO")
    ).distinct()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_result_df.write.format("delta").mode("Append").save("abfss://f1bde452-2399-41bc-9ba5-b3c078a190fc@onelake.dfs.fabric.microsoft.com/e56ddaf5-2e15-4634-91cc-e2eb818afa61/Tables/tmp_cr_mov01_05_jan")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
