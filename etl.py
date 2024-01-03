import pandas as pd
import numpy as np
import yaml
import mysql.connector
from mysql.connector import errorcode

# Function to process a single sheet
def process_sheet(data, year):
    """
    Transforms the data from the Excel sheet into a cleaned DataFrame.
    
    Args:
    data: DataFrame containing the raw data from the Excel sheet.
    year: Integer representing the year of the data.
    
    Returns:
    A DataFrame with the columns 'Month', 'Year', 'Category', and 'Sales'.
    """
    data = data.iloc[3:70, 1:-1]
    # Set the proper column names and remove the top rows with merged cells
    data.columns = ['Category'] + list(data.iloc[0, 1:].values)
    data = data.iloc[2:]
    
    # Unpivot the table from wide format to long format
    melted_data = data.melt(id_vars=['Category'], var_name='Month_Year', value_name='Sales')
    
    # Split 'Month_Year' into separate 'Month' and 'Year' columns
    melted_data[['Month', 'Year']] = melted_data['Month_Year'].str.split(' ', expand=True)
    
    # Drop the original 'Month_Year' column as it's no longer needed
    melted_data.drop('Month_Year', axis=1, inplace=True)
    
    # Reorder columns to match the desired structure
    melted_data = melted_data[['Month', 'Year', 'Category', 'Sales']]
    
    # Replace '(S)' values with 0
    melted_data['Sales'] = melted_data['Sales'].replace('(S)', 0)
    melted_data['Sales'] = melted_data['Sales'].replace('(NA)', 0)
    
    # Convert month strings to integers
    month_to_int = {
        'Jan.': 1, 'Feb.': 2, 'Mar.': 3, 'Apr.': 4, 'May': 5, 'Jun.': 6,
        'Jul.': 7, 'Aug.': 8, 'Sep.': 9, 'Oct.': 10, 'Nov.': 11, 'Dec.': 12
    }
    melted_data['Month'] = melted_data['Month'].map(month_to_int)
    
    return melted_data

# Load the Excel file
xls = pd.ExcelFile('mrtssales92-present.xls')

# Initialize an empty DataFrame to store the consolidated data
consolidated_data = pd.DataFrame()

# Loop through the sheets for the years 2000 to 2020
for year in range(2000, 2021):
    # Load the specific year's sheet
    data = pd.read_excel(xls, sheet_name=str(year))
    
    # Process the sheet and return the cleaned DataFrame
    cleaned_data = process_sheet(data, year)
    
    # Append the cleaned data to the consolidated DataFrame
    consolidated_data = pd.concat([consolidated_data, cleaned_data], ignore_index=True)

# Function to create and load data into MySQL using mysql.connector
def create_and_load_mysql(config, dataframe):
    try:
        # Connect to the MySQL database
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        
        # Create table query with appropriate column types
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS sales_data_agg (
            Month INT,
            Year INT,
            Category VARCHAR(255),
            Sales DECIMAL(10, 2)
        );
        """
        
        # Execute the create table query
        cursor.execute(create_table_query)
        
        # Construct insert statement for the data
        insert_statement = f"INSERT INTO sales_data_agg (Month, Year, Category, Sales) VALUES (%s, %s, %s, %s)"
        
        # Insert data row by row
        for _, row in dataframe.iterrows():
            cursor.execute(insert_statement, tuple(row))
        
        # Commit the changes
        cnx.commit()
        
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table already exists.")
        else:
            print(err.msg)
    else:
        # Close the cursor and connection
        cursor.close()
        cnx.close()
        
db = yaml.safe_load(open('db.yaml'))

config = {
    'user':     db['user'],
    'password': db['password'],
    'host':     db['host'],
    'database': db['db'],
    'auth_plugin':  'mysql_native_password'
}

# Extract and transform data
xls = pd.ExcelFile('mrtssales92-present.xls')
consolidated_data = pd.DataFrame()

for year in range(2000, 2021):
    data = pd.read_excel(xls, sheet_name=str(year))
    cleaned_data = process_sheet(data, year)
    consolidated_data = pd.concat([consolidated_data, cleaned_data], ignore_index=True)

consolidated_data.dropna(subset=['Month'], inplace=True)
consolidated_data['Year'] = consolidated_data['Year'].astype('int')
consolidated_data['Month'] = consolidated_data['Month'].astype('int')

# Optional: Save the consolidated DataFrame to a CSV file
consolidated_data.to_csv('consolidated_data_2000_2020.csv', index=False)

# Create and load data into MySQL
create_and_load_mysql(db, consolidated_data)
