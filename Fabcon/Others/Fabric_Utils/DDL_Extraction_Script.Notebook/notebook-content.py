# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Load the Delta table
lakehouse_name = "BronzeLakehouse"
table_name = ["cl_bkcompens_av", "cl_bkcompens_rv"]

for table in table_name:
    qualified_table_name = f"{lakehouse_name}.{table}"
    df = spark.table(qualified_table_name)

    # Generate column definitions
    columns = []
    for field in df.schema.fields:
        col_name = field.name
        data_type = field.dataType.simpleString()
        nullable = "" if field.nullable else " NOT NULL"
        columns.append(f"  {col_name} {data_type}{nullable}")

    # Fetch partition column info from DESCRIBE DETAIL
    desc = spark.sql(f"DESCRIBE DETAIL {qualified_table_name}").collect()[0]
    partition_cols = desc["partitionColumns"]

    # Combine into CREATE TABLE DDL
    ddl = f"CREATE TABLE {qualified_table_name} (\n" + ",\n".join(columns) + "\n) USING DELTA"

    # Add PARTITIONED BY clause if applicable
    if partition_cols:
        ddl += f"\nPARTITIONED BY ({', '.join(partition_cols)})"

    # Output final DDL
    print(ddl)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
