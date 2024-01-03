import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import yaml
import mysql.connector

# Load database configuration from YAML file
with open('db.yaml', 'r') as file:
    db_config = yaml.safe_load(file)

config = {
    'user': db_config['user'],
    'password': db_config['password'],
    'host': db_config['host'],
    'database': db_config['db'],
    'auth_plugin': 'mysql_native_password'
}

# Establish a connection to the database
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

# Query the data from the sales_data_agg table
query = "SELECT * FROM sales_data_agg"
cursor.execute(query)

# Fetch the results and load into a DataFrame
data = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

# Close the cursor and connection
cursor.close()
cnx.close()

# Now, data holds the DataFrame with data from your MySQL table
# Function to plot sales trends
def plot_sales_trends(filtered_data, title, ylabel, hue=None):
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=filtered_data, x='Date', y='Sales', hue=hue)
    plt.title(title)
    plt.xlabel('Year')
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.show()

# 1. Total Sales for Retail and Food Services
retail_food_data = data[data['Category'] == "Retail and food services sales, total"]
retail_food_data['Date'] = pd.to_datetime(retail_food_data[['Year', 'Month']].assign(DAY=1))
plot_sales_trends(retail_food_data, 'Trend of Total Sales in Retail and Food Services (2000-2020)', 'Sales (in thousands)')

# 2. Sales Trends of Specific Stores
categories = ['Book stores', 'Sporting goods stores', 'Hobby, toy, and game stores']
filtered_categories_data = data[data['Category'].isin(categories)]
filtered_categories_data['Date'] = pd.to_datetime(filtered_categories_data[['Year', 'Month']].assign(DAY=1))
plot_sales_trends(filtered_categories_data, 'Monthly Sales Trends: Specific Stores (2000-2020)', 'Sales (in thousands)', 'Category')

# 3. Yearly Analysis
yearly_data = filtered_categories_data.groupby(['Year', 'Category']).agg({'Sales': 'sum'}).reset_index()
yearly_data['Date'] = pd.to_datetime(yearly_data[['Year']].assign(Month=1, DAY=1))
plot_sales_trends(yearly_data, 'Yearly Sales Trends: Specific Stores (2000-2020)', 'Sales (in thousands)', 'Category')

# 4. Men's and Women's Clothing Stores Analysis
clothing_categories = ["Men's clothing stores", "Women's clothing stores"]
clothing_data = data[data['Category'].isin(clothing_categories)]
clothing_data['Date'] = pd.to_datetime(clothing_data[['Year', 'Month']].assign(DAY=1))
total_sales_per_month = data[data['Category'] == "Retail and food services sales, total"]
total_sales_per_month['Date'] = pd.to_datetime(total_sales_per_month[['Year', 'Month']].assign(DAY=1))
clothing_merged_data = clothing_data.merge(total_sales_per_month, on=['Year', 'Month', 'Date'], suffixes=('_clothing', '_total'))

# 5. Convert the Sales clothing column to the right format (float instead of decimal) 
clothing_merged_data['Sales_clothing'] = clothing_merged_data['Sales_clothing'].astype(float)
clothing_merged_data['Sales_total'] = clothing_merged_data['Sales_total'].astype(float)


# 6. Calculate the percentage change
clothing_merged_data['Percentage_Contribution'] = (clothing_merged_data['Sales_clothing'] / clothing_merged_data['Sales_total']) * 100
clothing_merged_data['Yearly_Percentage_Change'] = clothing_merged_data.groupby('Category_clothing')['Sales_clothing'].pct_change() * 100

# Plotting the yearly percentage change in sales for clothing categories
plt.figure(figsize=(14, 7))
sns.lineplot(data=clothing_merged_data, x='Year', y='Yearly_Percentage_Change', hue='Category_clothing')
plt.title('Year-over-Year Percentage Change in Sales: Men\'s and Women\'s Clothing Stores (2000-2020)')
plt.xlabel('Year')
plt.ylabel('Percentage Change')
plt.legend(title='Category')
plt.grid(True)
plt.show()

# Plotting the percentage contribution over time for clothing categories
plt.figure(figsize=(14, 7))
sns.lineplot(data=clothing_merged_data, x='Year', y='Percentage_Contribution', hue='Category_clothing')
plt.title('Percentage Contribution to Total Sales: Men\'s and Women\'s Clothing Stores (2000-2020)')
plt.xlabel('Year')
plt.ylabel('Percentage Contribution')
plt.legend(title='Category')
plt.grid(True)
plt.show()

# Choosing categories for the rolling time window analysis
chosen_categories = [
    "Sporting goods stores",
    "Electronics stores"
]

# Filtering data for the chosen categories
chosen_data = data[data['Category'].isin(chosen_categories)]

# Creating a datetime column for easier plotting and analysis
chosen_data['Date'] = pd.to_datetime(chosen_data[['Year', 'Month']].assign(DAY=1))

# Calculating the rolling 12-month average for each category
chosen_data['Rolling_Average'] = chosen_data.groupby('Category')['Sales'].rolling(window=12).mean().reset_index(level=0, drop=True)

# Plotting the rolling averages for each category
plt.figure(figsize=(14, 7))
sns.lineplot(data=chosen_data, x='Date', y='Rolling_Average', hue='Category')
plt.title('12-Month Rolling Average Sales: Sporting Goods Stores and Electronics Stores (2000-2020)')
plt.xlabel('Year')
plt.ylabel('12-Month Rolling Average Sales (in thousands)')
plt.legend(title='Category')
plt.grid(True)
plt.show()

