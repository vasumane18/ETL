import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime

def log_progress(message):
    time_format = "%Y-%m-%d %H:%M:%S"  # Corrected the format for month (m) and day (d)
    now = datetime.now()  # Get the current datetime
    timestamp = now.strftime(time_format)  # Format the datetime according to the time_format
    with open(".code_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")

log_progress("Preliminaries complete. Initiating ETL process")

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')  
    tables = data.find_all('tbody')
    
    rows = tables[0].find_all('tr')
    
    # List to store all rows
    all_data = []
    
    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            rank = cols[0].text.strip()
            name = cols[1].text.strip()
            market_cap = cols[2].text.strip().rstrip('\n').replace(',', '')
            try:
                market_cap_float = float(market_cap)
            except ValueError:
                market_cap_float = None
            
            # Append the row as a dictionary to the list
            all_data.append({
                "Rank": rank,
                "Name": name,
                "Market Cap (US$ Billion)": market_cap_float,     
            })
    
    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(all_data, columns=["Rank", "Name", "Market Cap (US$ Billion)"])
    
    return df

# Example usage
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Rank", "Name", "Market Cap (US$ Billion)"]
df = extract(url, table_attribs)
print(df)
    


def transform(df1):
    exchange_rates_df = pd.read_csv('exchange_rate.csv')     
    exchange_rate = exchange_rates_df.set_index('Currency').to_dict()['Rate']
    if 'Market Cap (US$ Billion)' not in df1.columns:
        raise KeyError("'Market Cap (US$ Billion)' column is missing in the DataFrame")
    
    # Transform the data
    df1['MC_GBP_Billion'] = [np.round(x * exchange_rate.get('GBP', 0), 2) for x in df1['Market Cap (US$ Billion)']]
    df1['MC_EUR_Billion'] = [np.round(x * exchange_rate.get('EUR', 0), 2) for x in df1['Market Cap (US$ Billion)']]
    df1['MC_INR_Billion'] = [np.round(x * exchange_rate.get('INR', 0), 2) for x in df1['Market Cap (US$ Billion)']]
    
    log_progress("Data transformation complete. Initiating Loading process")
    return df1

# URL of the webpage
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Rank", "Name", "Market Cap (US$ Billion)"]

# Extract data from the webpage
df1 = extract(url, table_attribs)

# Perform transformation using the exchange rates
transformed_df = transform(df1)

# Print the transformed DataFrame
print(transformed_df)

# Print the MC_EUR_Billion for the 5th largest bank
print("MC_EUR_Billion for the 5th largest bank:", transformed_df['MC_EUR_Billion'][4])

log_progress("Data loaded to Database as a table, Executing queries")


def load_to_csv(df, output_path):
    df.to_csv(output_path, index = False)
    log_progress("Data saved to CSV file")
output_path = './transformed data.csv'
load_to_csv(transformed_df, output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")

df = transformed_df
conn = sqlite3.connect('Banks.db')
load_to_db(df, conn, 'Largest_banks')
conn.close()
log_progress("Server Connection closed")


def run_query(query_statement, sql_connection):
    print("Query Statement:")
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print("Query Output:")
    print(query_output)

conn = sqlite3.connect('Banks.db')
query1 = "SELECT * FROM Largest_banks"
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query3 = "SELECT Name FROM Largest_banks LIMIT 5"  # Using 'Name' instead of 'Company'
run_query(query1, conn)
run_query(query2, conn)
run_query(query3, conn)
conn.close()
log_progress("Process Complete")
