# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import re
import pandas as pd
 
def extract_table_info_from_file(file_path):
    with open(file_path, 'r', encoding='utf-16') as file:
        ddl_script = file.read()
 
    # Regex to match CREATE TABLE statements
    table_pattern = r"CREATE TABLE \[dbo\]\.\[(\w+)\]\s*\((.*?)\)\s*(?=GO|CREATE|$)"
    # Robust column pattern that handles all SQL Server DDL cases
    column_pattern = r"\[(\w+)\]\s+\[(\w+)\](?:\((\d+(?:,\s*\d+)?)\))?(?:\s+(?:NULL|NOT NULL))?"
 
    matches = re.finditer(table_pattern, ddl_script, re.DOTALL | re.IGNORECASE)
 
    data = []
   
    for match in matches:
        table_name = match.group(1)
        columns_text = match.group(2)
       
        # Split columns while handling multi-line definitions
        columns = [col.strip() for col in re.split(r",\s*\n", columns_text) if col.strip()]
       
        for column in columns:
            # Skip constraints and other non-column definitions
            if not column.startswith('[') or 'CONSTRAINT' in column.upper():
                continue
               
            column_match = re.match(column_pattern, column)
            if column_match:
                column_name = column_match.group(1)
                data_type = column_match.group(2)
                precision = column_match.group(3)
               
                data.append([table_name, column_name, data_type, precision])
 
    df = pd.DataFrame(data, columns=['Table Name', 'Column Name', 'Data Type', 'Precision'])
    return df
 
def save_to_excel(dataframe, output_file_path):
    # Replace None with empty string for better Excel display
    dataframe['Precision'] = dataframe['Precision'].fillna('')
    dataframe.to_excel(output_file_path, index=False)
 
if __name__ == "__main__":
    input_file_path = '/content/MI Datawarehouse DDL (2).sql'
    table_info_df = extract_table_info_from_file(input_file_path)
   
    output_file_path = 'tables_columns_data.xlsx'
    save_to_excel(table_info_df, output_file_path)
   
    print(f"Data has been successfully extracted and saved to {output_file_path}")
    print("\nFirst 20 rows of extracted data:")
    print(table_info_df.head(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
