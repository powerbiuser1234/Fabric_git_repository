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
-- META       "default_lakehouse_workspace_id": "e10c1eb4-131b-4138-b4ac-b8e97bb56785"
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE Sample2 (
-- MAGIC     ID INT NOT NULL,
-- MAGIC     SmallNumber SMALLINT,
-- MAGIC     BigNumber BIGINT,
-- MAGIC     TinyNumber TINYINT,
-- MAGIC     DecimalNumber DECIMAL(10,2),
-- MAGIC     FloatNumber FLOAT,
-- MAGIC     DoubleNumber DOUBLE,
-- MAGIC     
-- MAGIC     -- String Data Types
-- MAGIC     FixedText CHAR(10),
-- MAGIC     VariableText VARCHAR(100),
-- MAGIC     LargeText STRING,  -- Preferred for large text
-- MAGIC 
-- MAGIC     -- Date & Time
-- MAGIC     BirthDate DATE,
-- MAGIC     EventTimestamp TIMESTAMP,
-- MAGIC 
-- MAGIC     -- Boolean Type
-- MAGIC     IsActive BOOLEAN,
-- MAGIC 
-- MAGIC     -- Complex Data Types
-- MAGIC     ListOfValues ARRAY<STRING>,  -- Example: ["A", "B", "C"]
-- MAGIC     KeyValuePairs MAP<STRING, INT>,  -- Example: {"A": 1, "B": 2}
-- MAGIC     NestedStructure STRUCT<
-- MAGIC         Name: STRING,
-- MAGIC         Age: INT
-- MAGIC     >  -- Example: { "Name": "John", "Age": 30 }
-- MAGIC ) USING DELTA;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM BronzeLakehouse.Cl_Bkcompens_Af_Eta where MOT is not null

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
