# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ef516724-eca9-40d5-92bc-40212bb6944e",
# META       "default_lakehouse_name": "GoldLakehouse",
# META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785",
# META       "known_lakehouses": [
# META         {
# META           "id": "ef516724-eca9-40d5-92bc-40212bb6944e"
# META         },
# META         {
# META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
# META         },
# META         {
# META           "id": "a294adf6-b6d4-4251-b1b6-1bcc76f15b03"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### 1. Libraries

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType, BinaryType, DateType, TimestampType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Reading tables to prepare clientSK and maxBatch Date

# CELL ********************

#Bronze Tables
bkcli_df = spark.read.format("delta").table("BronzeLakehouse.CBS_Bkcli")
CustomerCardholder_df = spark.read.format("delta").table("BronzeLakehouse.CMS_CustomerCardholder")
#Gold Tables
clientsk_df  = spark.read.format("delta").table("GoldLakehouse.ClientSK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. ClientSK ETL

# CELL ********************

# Get new SourceIDs not in ClientSK
new_clients_df = bkcli_df.select(trim(col("CLI")).alias("SourceID")) \
    .join(clientsk_df, bkcli_df.CLI == clientsk_df.SourceID, how="left_anti")

# Append new SourceIDs
new_clients_df.write.format("delta").mode("append").saveAsTable("GoldLakehouse.ClientSK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Join and filter
updated_df = clientsk_df.alias("sk") \
    .join(CustomerCardholder_df.alias("cc"), col("sk.SourceID") == col("cc.CUS_IDEN")) \
    .filter(col("cc.CUS_IDEN").isNotNull() & col("sk.ClientCodeCMS").isNull()) \
    .select("sk.SourceID", "cc.CUS_CODE")

# Merge back into ClientSK
from delta.tables import DeltaTable

clientsk_delta = DeltaTable.forName(spark, "GoldLakehouse.ClientSK")

clientsk_delta.alias("sk").merge(
    updated_df.alias("updates"),
    "sk.SourceID = updates.SourceID"
).whenMatchedUpdate(
    condition="sk.ClientCodeCMS IS NULL",
    set={"ClientCodeCMS": "updates.CUS_CODE"}
).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. EXCEPTION HANDLING FOR MULTIPLE LOADS ON SAME DAY

# CELL ********************

# Get MaxBatchDate (with most entries)
max_batchdate_df = (
    bkcli_df.groupBy("BATCHDATE")
    .agg(count("*").alias("CNT"))
    .withColumn("RN", row_number().over(Window.orderBy(col("CNT").desc())))
    .filter(col("RN") == 1)
    .select("BATCHDATE")
)
max_batchdate = max_batchdate_df.first()["BATCHDATE"]

# Filter BKCLI on MaxBatchDate
bkcli_df = bkcli_df.filter(col("BATCHDATE") == max_batchdate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. Reading tables to prepare ClientDim dataframe

# CELL ********************

nom_df = spark.read.format("delta").table("GoldLakehouse.CbsBknom")
evuti_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Evuti")
bkville_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkville")
metier_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bksmetier")
prfcli_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkprfcli")
bkempl_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkempl")
grp_df = spark.read.format("delta").table("BronzeLakehouse.Cbs_Bkgrp")
clientsk_df = spark.read.format("delta").table("GoldLakehouse.ClientSK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 6. ClientDim Silver ETL

# MARKDOWN ********************

# ### 6. Prepairing resultant dataframe

# CELL ********************

prf_sub = (
    prfcli_df.groupBy("CLI")
    .agg(spark_max("DEMB").alias("DEMB"))
)

prf_df = (
    prfcli_df.join(prf_sub, on=["CLI", "DEMB"], how="inner")
    .filter((col("DEMP") != "") | col("DEMP").isNull())
    .select("CLI", "PRF", "DEMP", "EMP", "TREV", "DEMB")
)

emp1_df = (
    bkempl_df.groupBy("EMP", "NOM")
    .agg(spark_max("DOU").alias("mxDate"))
)

client_dim_df = (
    bkcli_df
    .join(clientsk_df, trim(col("cli")) == col("sourceID"), "left")
    .join(nom_df.alias("nom1"), (col("AGE") == col("nom1.CACC")) & (col("nom1.CTAB") == "001"), "left")
    .join(nom_df.alias("nom2"), (col("CAPJ") == col("nom2.CACC")) & (col("nom2.CTAB") == "172"), "left")
    .join(nom_df.alias("nom3"), (col("CATL") == col("nom3.CACC")) & (col("nom3.CTAB") == "041"), "left")
    .join(nom_df.alias("nom4"), (col("CATN") == col("nom4.CACC")) & (col("nom4.CTAB") == "042"), "left")
    .join(nom_df.alias("nom5"), (col("Chl3") == col("nom5.CACC")) & (col("nom5.CTAB") == "065"), "left")
    .join(nom_df.alias("nom6"), (col("depn") == col("nom6.CACC")) & (col("nom6.CTAB") == "191"), "left")
    .join(nom_df.alias("nom7"), (col("fju") == col("nom7.CACC")) & (col("nom7.CTAB") == "049"), "left")
    .join(nom_df.alias("nom8"), (col("ges") == col("nom8.CACC")) & (col("nom8.CTAB") == "035"), "left")
    .join(nom_df.alias("nom9"), (col("lang") == col("nom9.CACC")) & (col("nom9.CTAB") == "190"), "left")
    .join(nom_df.alias("nom10"), (col("lib") == col("nom10.CACC")) & (col("nom10.CTAB") == "036"), "left")
    .join(nom_df.alias("nom11"), (col("lienbq") == col("nom11.CACC")) & (col("nom11.CTAB") == "187"), "left")
    .join(nom_df.alias("nom12"), (col("LTER") == col("nom12.CACC")) & (col("nom12.CTAB") == "079"), "left")
    .join(nom_df.alias("nom13"), (col("nat") == col("nom13.CACC")) & (col("nom13.CTAB") == "033"), "left")
    .join(nom_df.alias("nom14"), (col("nchc") == col("nom14.CACC")) & (col("nom14.CTAB") == "077"), "left")
    .join(nom_df.alias("nom15"), (col("payn") == col("nom15.CACC")) & (col("nom15.CTAB") == "040"), "left")
    .join(nom_df.alias("nom16"), (col("qua") == col("nom16.CACC")) & (col("nom16.CTAB") == "043"), "left")
    .join(nom_df.alias("nom17"), (col("reg") == col("nom17.CACC")) & (col("nom17.CTAB") == "048"), "left")
    .join(nom_df.alias("nom18"), (col("res") == col("nom18.CACC")) & (col("nom18.CTAB") == "040"), "left")
    .join(nom_df.alias("nom19"), (col("resd") == col("nom19.CACC")) & (col("nom19.CTAB") == "186"), "left")
    .join(nom_df.alias("nom20"), (col("sec") == col("nom20.CACC")) & (col("nom20.CTAB") == "071"), "left")
    .join(nom_df.alias("nom21"), (col("seg") == col("nom21.CACC")) & (col("nom21.CTAB") == "188"), "left")
    .join(nom_df.alias("nom22"), (col("sit") == col("nom22.CACC")) & (col("nom22.CTAB") == "047"), "left")
    .join(nom_df.alias("nom23"), (col("sitimmo") == col("nom23.CACC")) & (col("nom23.CTAB") == "264"), "left")
    .join(nom_df.alias("nom24"), (col("SITJ") == col("nom24.CACC")) & (col("nom24.CTAB") == "185"), "left")
    .join(nom_df.alias("nom25"), (col("tid") == col("nom25.CACC")) & (col("nom25.CTAB") == "078"), "left")


    .join(evuti_df.alias("evu1"), col("CRS_UTI") == col("evu1.CUTI"), "left")
    .join(evuti_df.alias("evu2"), col("FATCA_UTI") == col("evu2.CUTI"), "left")
    .join(evuti_df.alias("evu3"), col("UTI") == col("evu3.CUTI"), "left")
    .join(evuti_df.alias("evu4"), col("UTI_VRRC") == col("evu4.CUTI"), "left")
    .join(evuti_df.alias("evu5"), col("UTIC") == col("evu5.CUTI"), "left")

    .join(bkville_df.alias("vil1"), col("LOCN") == col("vil1.CLOC"), "left")
    .join(bkville_df.alias("vil2"), col("VILN") == col("vil2.CLOC"), "left")

    .join(metier_df.alias("metier"), col("SMET") == col("metier.MET"), "left")

    .join(prf_df.alias("prf"), trim(col("CLI")) == col("prf.CLI"), "left")
    .join(emp1_df.alias("emp1"), col("prf.EMP") == col("emp1.EMP"), "left")
    .join(nom_df.alias("nom26"), (col("prf.PRF") == col("nom26.CACC")) & (col("nom26.CTAB") == "045"), "left")
    .join(nom_df.alias("nom27"), (col("prf.TREV") == col("nom27.CACC")) & (col("nom27.CTAB") == "046"), "left")
    .join(grp_df.alias("grp"), col("GRP") == col("grp.GRP"), "left")
    .select(
        clientsk_df.ClientID.alias("ClientID"),
        bkcli_df.AGE.alias("CliAgenceCode"),
        nom1.LIB1.alias("CliAgenceName"),
        bkcli_df.CAPJ.alias("LegalCapacityCode"),
        nom2.LIB1.alias("LegalCapacity"),
        bkcli_df.CATL.alias("InternalCatCode"),
        nom3.LIB1.alias("InternalCategory"),
        bkcli_df.CATN.alias("CompanyTypeCode"),
        nom4.LIB1.alias("CompanyType"),
        bkcli_df.CATR.alias("RealTimeXferAllowed"),
        bkcli_df.CHL3.alias("ProvinceCode"),
        nom5.LIB1.alias("Province"),
        trim(bkcli_df.CLI).alias("ClientBK"),
        bkcli_df.CONJ.alias("SpouseCode"),
        bkcli_df.CRS_DATE.alias("LastCRSDAte"),
        bkcli_df.CRS_STATUS.alias("LastCRSStatus"),
        bkcli_df.CRS_UTI.alias("CRSPerformedBy"),
        evu1.LIB.alias("CRSPerfByName"),
        bkcli_df.DATC.alias("CompanyyStartDate"),
        bkcli_df.DCAPJ.alias("CDate"),
        bkcli_df.DEPN.alias("BirthCountyCode"),
        nom6.LIB1.alias("BirthCounty"),
        bkcli_df.DID.alias("IDRecDeliveryDate"),
        bkcli_df.DMO.alias("UpdatedOn"),
        bkcli_df.DNA.alias("DOB"),
        bkcli_df.DOU.alias("CreatedOn"),
        bkcli_df.DRC.alias("TradeRegDeliveryDate"),
        bkcli_df.DSITJ.alias("LegalStatusStartDate"),
        bkcli_df.FATCA_DATE.alias("FATCAStatusDate"),
        bkcli_df.FATCA_STATUS.alias("FATCAStatus"),
        bkcli_df.FATCA_UTI.alias("FATCAStatusUpdBy"),
        evu2.LIB.alias("FATCAStatusUpdByName"),
        bkcli_df.FJU.alias("LegalFormCode"),
        nom7.LIB1.alias("LegalForm"),
        bkcli_df.GES.alias("AccountOfficer"),
        nom8.LIB1.alias("AccountOfficerName"),
        bkcli_df.LANG.alias("PreferredLangCode"),
        nom9.LIB1.alias("PreferredLang"),
        bkcli_df.LIB.alias("CivilStatusCode"),
        nom10.LIB1.alias("CivilStatus"),
        bkcli_df.LID.alias("IDRecDeliveryLoc"),
        bkcli_df.LIENBQ.alias("CliBankRelCode"),
        nom11.LIB1.alias("CliBankRel"),
        bkcli_df.LOCN.alias("BirthLocalityCode"),
        vil1.NOM.alias("BirthLocality"),
        bkcli_df.LRC.alias("TradeRegDeliveryLoc"),
        bkcli_df.LTER.alias("CliTerrCode"),
        nom12.LIB1.alias("CliTerritory"),
        bkcli_df.MIDNAME.alias("MiddleName"),
        bkcli_df.NAT.alias("NationalityCode"),
        nom13.LIB1.alias("Nationality"),
        bkcli_df.NBENF.alias("ChildrenCount"),
        bkcli_df.NCHC.alias("ChamberOfCommCode"),
        nom14.LIB1.alias("ChamberOfCommerce"),
        bkcli_df.NICR.alias("CreditInfoRegCOde"),
        bkcli_df.NID.alias("IDRecNumber"),
        bkcli_df.NIDF.alias("TaxIDNumber"),
        bkcli_df.NIDN.alias("NationalIDNumber"),
        bkcli_df.NIS.alias("SocialIDNumber"),
        bkcli_df.NJF.alias("CliMaidenName"),
        bkcli_df.NOM.alias("CliLastName"),
        trim(bkcli_df.NOMREST).alias("DisplayName"),
        bkcli_df.NPA.alias("BussLicenseNumber"),
        bkcli_df.NRC.alias("TradeRegNumber"),
        bkcli_df.NST.alias("StatsID"),
        bkcli_df.OID.alias("IDRecDlvyOrg"),
        bkcli_df.OPETR.alias("IntlOperation"),
        bkcli_df.ORD.alias("ModifSheetNum"),
        bkcli_df.PAYN.alias("BirthCountryCode"),
        nom15.LIB1.alias("BirthCountry"),
        bkcli_df.PRE.alias("CliFirstName"),
        bkcli_df.QUA.alias("QualityCode"),
        nom16.LIB1.alias("Quality"),
        bkcli_df.REG.alias("MaritalAgmtCode"),
        nom17.LIB1.alias("MaritalAgmt"),
        bkcli_df.RES.alias("ResidenceCountryCode"),
        nom18.LIB1.alias("ResidenceCountry"),
        bkcli_df.RESD.alias("DeclaredResidenceCode"),
        nom19.LIB1.alias("DeclaredResidence"),
        concat_ws(' ', bkcli_df.RSO, bkcli_df.RSO2).alias("DeclaredCorpName"),
        bkcli_df.SEC.alias("ActivityFldCode"),
        nom20.LIB1.alias("ActivityField"),
        bkcli_df.SEG.alias("CliSegmentCode"),
        nom21.LIB1.alias("CliSegment"),
        bkcli_df.SEXT.alias("Gender"),
        bkcli_df.SIG.alias("Abbreviation"),
        bkcli_df.SIT.alias("MaritalStatusCode"),
        nom22.LIB1.alias("MaritalStatus"),
        bkcli_df.SITIMMO.alias("ResidenceTypeCode"),
        nom23.LIB1.alias("ResidenceType"),
        bkcli_df.SITJ.alias("LegalStatusCode"),
        nom24.LIB1.alias("LegalStatus"),
        bkcli_df.SMET.alias("CliSubJobCode"),
        metier.NOM.alias("CliSubJob"),
        bkcli_df.TAX.alias("IsTaxable"),
        bkcli_df.TCLI.alias("CliTypeCode"),
        bkcli_df.TCONJ.alias("SpouseType"),
        bkcli_df.TID.alias("IDRecTypeCode"),
        nom25.LIB1.alias("IDRecType"),
        bkcli_df.UTI.alias("InitiatedBy"),
        evu3.LIB.alias("InitiatedByName"),
        bkcli_df.UTI_VRRC.alias("VerifiedBy"),
        evu4.LIB.alias("VerifiedByName"),
        bkcli_df.UTIC.alias("CreatedBy"),
        evu5.LIB.alias("CreatedByName"),
        bkcli_df.VID.alias("IDRecValidDate"),
        bkcli_df.VILN.alias("BirthTownCode"),
        vil2.NOM.alias("BirthTown"),
        bkcli_df.VPA.alias("BussLicenseValidDate"),
        bkcli_df.VRC.alias("TradeRegValidDate"),

        # CASE statements for profession and employer info
        when((prf.emp.rlike("^[0-9]")) & ((emp1.emp.isNull()) | (emp1.emp == "")), None)
            .otherwise(prf.PRF).alias("ProfessionCode"),

        when((prf.emp.rlike("^[0-9]")) & ((emp1.emp.isNull()) | (emp1.emp == "")), None)
            .otherwise(nom26.LIB1).alias("Profession"),

        when((prf.emp.rlike("^[0-9]")) & ((emp1.emp.isNull()) | (emp1.emp == "")), None)
            .otherwise(prf.EMP).alias("EmployerCode"),

        when((prf.emp.rlike("^[0-9]")) & ((emp1.emp.isNull()) | (emp1.emp == "")), None)
            .otherwise(emp1.NOM).alias("Employer"),

        when((prf.emp.rlike("^[0-9]")) & ((emp1.emp.isNull()) | (emp1.emp == "")), None)
            .otherwise(prf.DEMP).alias("CompanyDept"),

        when((prf.emp.rlike("^[0-9]")) & ((emp1.emp.isNull()) | (emp1.emp == "")), None)
            .otherwise(prf.DEMB).alias("HireDate"),

        bkcli_df.GRP.alias("GroupCode"),
        grp.NOM.alias("GroupName"),

        # New columns
        bkcli_df.CLIPAR.alias("SponserCustomerCode"),
        prf.TREV.alias("IncomeBracket"),
        nom27.LIB1.alias("IncomeBracketDesc")

        bkcli_df.BATCHID,
        bkcli_df.BATCHDATE,
        bkcli_df.BATCHDATE.cast("date").alias("CREATEDON"),
        lit(None).cast("date").alias("UpdatedOn"),
        bkcli_df.SYSTEMCODE,
        lit(None).cast("binary").alias("RowHash"),
        lit("P_Dly_Cbs_Slv_Gld").alias("WorkflowName")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 6.2 Adding Values to RowHash Column

# CELL ********************

client_dim_df = client_dim_df.withColumn(
    "RowHash",
    sha2(
        concat_ws(
            "",  # separator
            col("ClientID"),
            col("CliAgenceCode"),
            col("CliAgenceName"),
            col("LegalCapacityCode"),
            col("LegalCapacity"),
            col("InternalCatCode"),
            col("InternalCategory"),
            col("CompanyTypeCode"),
            col("CompanyType"),
            col("RealTimeXferAllowed"),
            col("ProvinceCode"),
            col("Province"),
            col("ClientBK"),
            col("SpouseCode"),
            col("LastCRSDAte"),
            col("LastCRSStatus"),
            col("CRSPerformedBy"),
            col("CRSPerfByName"),
            col("CompanyyStartDate"),
            col("CDate"),
            col("BirthCountyCode"),
            col("BirthCounty"),
            col("IDRecDeliveryDate"),
            col("UpdatedOn"),
            col("DOB"),
            col("CreatedOn"),
            col("TradeRegDeliveryDate"),
            col("LegalStatusStartDate"),
            col("FATCAStatusDate"),
            col("FATCAStatus"),
            col("FATCAStatusUpdBy"),
            col("FATCAStatusUpdByName"),
            col("LegalFormCode"),
            col("LegalForm"),
            col("AccountOfficer"),
            col("AccountOfficerName"),
            col("PreferredLangCode"),
            col("PreferredLang"),
            col("CivilStatusCode"),
            col("CivilStatus"),
            col("IDRecDeliveryLoc"),
            col("CliBankRelCode"),
            col("CliBankRel"),
            col("BirthLocalityCode"),
            col("BirthLocality"),
            col("TradeRegDeliveryLoc"),
            col("CliTerrCode"),
            col("CliTerritory"),
            col("MiddleName"),
            col("NationalityCode"),
            col("Nationality"),
            col("ChildrenCount"),
            col("ChamberOfCommCode"),
            col("ChamberOfCommerce"),
            col("CreditInfoRegCOde"),
            col("IDRecNumber"),
            col("TaxIDNumber"),
            col("NationalIDNumber"),
            col("SocialIDNumber"),
            col("CliMaidenName"),
            col("CliLastName"),
            col("DisplayName"),
            col("BussLicenseNumber"),
            col("TradeRegNumber"),
            col("StatsID"),
            col("IDRecDlvyOrg"),
            col("IntlOperation"),
            col("ModifSheetNum"),
            col("BirthCountryCode"),
            col("BirthCountry"),
            col("CliFirstName"),
            col("QualityCode"),
            col("Quality"),
            col("MaritalAgmtCode"),
            col("MaritalAgmt"),
            col("ResidenceCountryCode"),
            col("ResidenceCountry"),
            col("DeclaredResidenceCode"),
            col("DeclaredResidence"),
            col("DeclaredCorpName"),
            col("ActivityFldCode"),
            col("ActivityField"),
            col("CliSegmentCode"),
            col("CliSegment"),
            col("Gender"),
            col("Abbreviation"),
            col("MaritalStatusCode"),
            col("MaritalStatus"),
            col("ResidenceTypeCode"),
            col("ResidenceType"),
            col("LegalStatusCode"),
            col("LegalStatus"),
            col("CliSubJobCode"),
            col("CliSubJob"),
            col("IsTaxable"),
            col("CliTypeCode"),
            col("SpouseType"),
            col("IDRecTypeCode"),
            col("IDRecType"),
            col("InitiatedBy"),
            col("InitiatedByName"),
            col("VerifiedBy"),
            col("VerifiedByName"),
            col("CreatedBy"),
            col("CreatedByName"),
            col("IDRecValidDate"),
            col("BirthTownCode"),
            col("BirthTown"),
            col("BussLicenseValidDate"),
            col("TradeRegValidDate"),
            col("ProfessionCode"),
            col("Profession"),
            col("EmployerCode"),
            col("Employer"),
            col("CompanyDept"),
            col("HireDate"),
            col("GroupCode"),
            col("GroupName"),
            col("SponserCustomerCode"),
            col("IncomeBracket"),
            col("IncomeBracketDesc")
        ),
        256
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.caseSensitive", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 7. Writing Data into "**SilverLakehouse.ClientDim**"

# CELL ********************

client_dim_df.write.format("delta").mode("overwrite").saveAsTable("SilverLakehouse.CbsClientDim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
