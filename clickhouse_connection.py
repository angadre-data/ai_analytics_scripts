import pandas as pd

# Install the required library
# pip install clickhouse-connect
# Import the ClickHouse client

from clickhouse_connect import get_client

# ClickHouse connection credentials
host = 'clickhouse.statera.internal'
port = 8123
username = 'data-scientist'
password = 'l4lnZGx9VJcdoIrN'

# SQL query to fetch data
sql_query = '''select * from assembly.client_events ce limit 100'''

# Replace with your actual SQL query

# Connect to ClickHouse
client = get_client(host=host, port=port, username=username, password=password)

# Execute query and fetch data into a pandas DataFrame
query_result = client.query(sql_query)
# Get the rows of the query result
rows = query_result.result_rows

# Get the column names of the query result
columns = query_result.column_names

# Convert to a pandas DataFrame
df = pd.DataFrame(rows, columns=columns)

# Display the DataFrame
print(df)
