-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "a43617b8-dd1a-46c9-aa3c-c2645e30c949",
-- META       "default_lakehouse_name": "BronzeLakehouse",
-- META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "a43617b8-dd1a-46c9-aa3c-c2645e30c949"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

select * from BronzeLakehouse.Mtd_Tbl

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from sample_trim_pipeline;
--MOT2 have whitespaces
                                                          

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from sample_trim;
                              

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
