import requests
import pandas as pd
import config
import pyodbc


# Extract data from the currency exchange API.
def extract_data(): 
    BASE_URL = config.API_BASE_URL
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Failed to fetch conversion rates: {e}")



def transform_data(currency_data):
    df = pd.DataFrame(currency_data)

    # Drop unnecessary columns
    df.drop(columns=['documentation', 'terms_of_use', 'time_next_update_utc', 
                     'time_last_update_unix', 'time_next_update_unix', 'result'], inplace=True)

    # Getting the Target Currency (index) into the dataframe
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Target_Currency'}, inplace=True)

    # Converting column format to date
    df['time_last_update_utc'] = pd.to_datetime(df['time_last_update_utc'], errors='coerce')

    # Changing columns name
    df.rename(columns={'base_code': 'Base_Currency', 'conversion_rates': 'Conversion_Rates', 'time_last_update_utc': 'Last_Update_Date'}, inplace=True)

    # Changing the order of columns
    new_order = ['Target_Currency', 'Base_Currency', 'Conversion_Rates', 'Last_Update_Date']
    df = df[new_order]
    return df



def load_data(df):
    conn = None
    cursor = None
    
    try:
        conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config.DB_SERVER};DATABASE={config.DB_NAME};Trusted_Connection=yes;')
        cursor = conn.cursor()
        
        # Inserting data into SQL Server
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO Exchange_Rates (Target_Currency, Base_Currency, Conversion_Rates, Last_Update_Date)
                VALUES (?, ?, ?, ?)
            """, (row['Target_Currency'], row['Base_Currency'], row['Conversion_Rates'], row['Last_Update_Date']))
        
        conn.commit()
        print(f"Data loaded successfully into SQL Server. Total rows inserted: {len(df)}")
        
    except Exception as e:
        print(f"Error loading data into SQL Server: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    


# Execut the ETL proccess
def main(): 
    try:
        # Extract data
        currency_data = extract_data()
        
        # Transform data
        df = transform_data(currency_data)
        
        # Load data into SQL Server
        load_data(df)
        
    except Exception as e:
        print(f"ETL process failed: {e}")


if __name__ == "__main__":
    main()