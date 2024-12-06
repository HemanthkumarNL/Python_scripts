import pandas as pd
import sqlite3

def read_file(file_path):
    return pd.read_csv(file_path)

# Function to transform data
def transform_data(df, region):
    df['total_sales'] = df['QuantityOrdered'] * df['ItemPrice']
    df['region'] = region
    df['net_sales'] = df['total_sales'] - df['PromotionDiscount']
    df = df.drop_duplicates(subset=['OrderId'])
    df = df[df['net_sales'] > 0]
    return df

# Function to load data into SQLite database
def load_data(df, db_name='sales_data.db'):
    conn = sqlite3.connect(db_name)
    df.to_sql('sales_data', conn, if_exists='replace', index=False)
    conn.close()


def main():
    # Reading the input files using read file function
    df_a = read_file('sales_region_a.csv')
    df_b = read_file('sales_region_b.csv')
    
    # Transform the data into desired format as per function 
    df_a = transform_data(df_a, 'A')
    df_b = transform_data(df_b, 'B')
    
    #combine the result into one variable
    df_combined = pd.concat([df_a, df_b], ignore_index=True)
    
    # calling the load data function to load the data into database
    load_data(df_combined)


main()
def validate_data(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    queries = {
        "total_records": "SELECT COUNT(*) FROM sales_data;",
        "total_sales_by_region": "SELECT region, SUM(total_sales) FROM sales_data GROUP BY region;",
        "average_sales": "SELECT AVG(total_sales) FROM sales_data;",
        "check_duplicates": "SELECT COUNT(OrderId) - COUNT(DISTINCT OrderId) FROM sales_data;"
    }
    
    results = {}
    for key, query in queries.items():
        cursor.execute(query)
        results[key] = cursor.fetchall()
    
    conn.close()
    return results

result=validate_data(db_name='sales.db')
output=pd.DataFrame(result)
output.to_csv('result_set.csv')




