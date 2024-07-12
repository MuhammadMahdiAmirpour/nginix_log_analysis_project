from dotenv import load_dotenv, dotenv_values
import re
import pandas as pd
from urllib.parse import urlparse, parse_qs
import numpy as np
import mysql.connector
import matplotlib.pyplot as plt
import seaborn as sns

# Step 1: Define a function to parse a single log line
def parse_log_line(line):
    log_pattern = re.compile(
        r'(?P<ip>\d+\.\d+\.\d+\.\d+|\bNULL\b) - - \[(?P<timestamp>[^\]]+)\] "(?P<method>\w+) (?P<url>[^\s]+) HTTP/[^\"]+" (?P<status>\d{3}) (?P<size>\d+|-)$'
    )
    
    match = log_pattern.match(line)
    if match:
        log_dict = match.groupdict()
        parsed_url = urlparse(log_dict['url'])
        log_dict['query_parameters'] = parsed_url.query
        log_dict['url'] = parsed_url.path
        return log_dict
    return None

# Step 2: Define a function to read and parse the log file
def read_log_file(file_path):
    parsed_logs = []
    with open(file_path, 'r') as file:
        for line in file:
            parsed_log = parse_log_line(line)
            if parsed_log:
                parsed_logs.append(parsed_log)
    return parsed_logs

# Step 3: Read and clean the parsed data
log_file_path = 'nginx_logs.txt'
parsed_logs = read_log_file(log_file_path)

df = pd.DataFrame(parsed_logs)

# Convert timestamp to datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d/%b/%Y:%H:%M:%S %z')

# Replace '-' with NaN in 'size' and convert to numeric
df['size'] = df['size'].replace('-', np.nan).astype(float)

# Ensure the DataFrame only has the specified columns
df = df[['ip', 'timestamp', 'method', 'url', 'status', 'size', 'query_parameters']]

# Replace NaN values in 'size' with 0
df['size'] = df['size'].fillna(0)

# Step 4: Store the data in a MySQL database
# Load environment variables from .env file
load_dotenv()
config = dotenv_values(".env")

# Create a connection to the MySQL database
cnx = mysql.connector.connect(
    host=config['HOST'],
    user=config['USERNAME'],
    password=config['DB_PASSWORD']
)
my_cursor = cnx.cursor(buffered=True)

# Create the database if it doesn't exist
db_description = f"CREATE DATABASE IF NOT EXISTS {config['DB_NAME']};"
my_cursor.execute("SHOW DATABASES")
if (config['DB_NAME'],) in my_cursor:
    print(f"Database {config['DB_NAME']} already exists.")
else:
    try:
        my_cursor.execute(db_description)
        print(f"Database {config['DB_NAME']} created successfully.")
    except mysql.connector.Error as err:
        print(f"Failed creating database: {err}")
        exit(1)

# Use the database
my_cursor.execute(f"USE {config['DB_NAME']};")

# Create the table if it doesn't exist
table_description = (
    "CREATE TABLE IF NOT EXISTS nginx_logs ("
    "log_id INT NOT NULL AUTO_INCREMENT,"
    "ip_addr CHAR(15) NOT NULL,"
    "time_stamp TIMESTAMP NOT NULL,"
    "request_method CHAR(15) NOT NULL,"
    "url CHAR(255) NOT NULL,"
    "status_code INT NOT NULL,"
    "response_size FLOAT,"
    "query_parameters TEXT NOT NULL,"
    "PRIMARY KEY (log_id)"
    ") ENGINE=InnoDB"
)
my_cursor.execute("SHOW TABLES;")
if ("nginx_logs",) in my_cursor:
    print("Table nginx_logs already exists.")
else:
    try:
        my_cursor.execute(table_description)
        print("Table nginx_logs created successfully.")
    except mysql.connector.Error as err:
        print(f"Failed creating table: {err}")
        exit(1)

# Insert data into the table
insert_query = (
    "INSERT INTO nginx_logs (ip_addr, time_stamp, request_method, url, status_code, response_size, query_parameters)"
    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
)

for _, row in df.iterrows():
    my_cursor.execute(insert_query, (
        row['ip'],
        row['timestamp'],
        row['method'],
        row['url'],
        row['status'],
        row['size'],
        row['query_parameters']
    ))

cnx.commit()

# Step 5: Generate visualizations

# Plot the number of requests per method
plt.figure(figsize=(10, 6))
sns.countplot(data=df, x='method')
plt.title('Number of Requests per Method')
plt.xlabel('HTTP Method')
plt.ylabel('Count')
plt.savefig('requests_per_method.png')
plt.show()

# Plot the distribution of response sizes
plt.figure(figsize=(10, 6))
sns.histplot(data=df, x='size', bins=30, kde=True)
plt.title('Distribution of Response Sizes')
plt.xlabel('Response Size (bytes)')
plt.ylabel('Frequency')
plt.savefig('response_size_distribution.png')
plt.show()

# Plot the number of requests per status code
plt.figure(figsize=(10, 6))
sns.countplot(data=df, x='status')
plt.title('Number of Requests per Status Code')
plt.xlabel('Status Code')
plt.ylabel('Count')
plt.savefig('requests_per_status_code.png')
plt.show()

# Plot the number of requests per hour
df['hour'] = df['timestamp'].dt.hour
plt.figure(figsize=(14, 8))
sns.countplot(data=df, x='hour')
plt.title('Number of Requests per Hour')
plt.xlabel('Hour of the Day')
plt.ylabel('Count')
plt.savefig('requests_per_hour.png')
plt.show()

# Plot the number of requests per day
df['day'] = df['timestamp'].dt.date
plt.figure(figsize=(14, 8))
sns.countplot(data=df, x='day')
plt.title('Number of Requests per Day')
plt.xlabel('Day')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.savefig('requests_per_day.png')
plt.show()

# Plot the top requested URLs
plt.figure(figsize=(14, 8))
top_urls = df['url'].value_counts().nlargest(10)
sns.barplot(x=top_urls.values, y=top_urls.index)
plt.title('Top Requested URLs')
plt.xlabel('Count')
plt.ylabel('URL')
plt.savefig('top_requested_urls.png')
plt.show()

# Plot the average response size per request method
plt.figure(figsize=(10, 6))
avg_response_size = df.groupby('method')['size'].mean().sort_values(ascending=False)
sns.barplot(x=avg_response_size.values, y=avg_response_size.index)
plt.title('Average Response Size per Request Method')
plt.xlabel('Average Response Size (bytes)')
plt.ylabel('HTTP Method')
plt.savefig('avg_response_size_per_method.png')
plt.show()

# Step 6: Document and present findings
# Save the plots and summary statistics in a report
with open('report.txt', 'w') as report:
    report.write("Summary Statistics:\n")
    report.write(df.describe().to_string())
    report.write("\n\n")
    
    report.write("Key Insights and Observations:\n")
    report.write("1. The number of requests per HTTP method shows the most common methods used.\n")
    report.write("2. The distribution of response sizes helps identify the range and frequency of response sizes.\n")
    report.write("3. The number of requests per status code indicates the success or failure of the requests.\n")
    report.write("4. The number of requests per hour shows the traffic patterns throughout the day.\n")
    report.write("5. The number of requests per day shows the traffic patterns across different days.\n")
    report.write("6. The top requested URLs highlight the most accessed resources.\n")
    report.write("7. The average response size per request method reveals the typical load associated with each method.\n")
    report.write("\n\n")

# Save the summary statistics to a CSV file
df.describe().to_csv('summary_statistics.csv')

# Save the plots as images
plt.figure(figsize=(10, 6))
sns.countplot(data=df, x='method')
plt.title('Number of Requests per Method')
plt.xlabel('HTTP Method')
plt.ylabel('Count')
plt.savefig('requests_per_method.png')
plt.show()

plt.figure(figsize=(10, 6))
sns.histplot(data=df, x='size', bins=30, kde=True)
plt.title('Distribution of Response Sizes')
plt.xlabel('Response Size (bytes)')
plt.ylabel('Frequency')
plt.savefig('response_size_distribution.png')
plt.show()

plt.figure(figsize=(10, 6))
sns.countplot(data=df, x='status')
plt.title('Number of Requests per Status Code')
plt.xlabel('Status Code')
plt.ylabel('Count')
plt.savefig('requests_per_status_code.png')
plt.show()

plt.figure(figsize=(14, 8))
sns.countplot(data=df, x='hour')
plt.title('Number of Requests per Hour')
plt.xlabel('Hour of the Day')
plt.ylabel('Count')
plt.savefig('requests_per_hour.png')
plt.show()

plt.figure(figsize=(14, 8))
sns.countplot(data=df, x='day')
plt.title('Number of Requests per Day')
plt.xlabel('Day')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.savefig('requests_per_day.png')
plt.show()

plt.figure(figsize=(14, 8))
top_urls = df['url'].value_counts().nlargest(10)
sns.barplot(x=top_urls.values, y=top_urls.index)
plt.title('Top Requested URLs')
plt.xlabel('Count')
plt.ylabel('URL')
plt.savefig('top_requested_urls.png')
plt.show()

plt.figure(figsize=(10, 6))
avg_response_size = df.groupby('method')['size'].mean().sort_values(ascending=False)
sns.barplot(x=avg_response_size.values, y=avg_response_size.index)
plt.title('Average Response Size per Request Method')
plt.xlabel('Average Response Size (bytes)')
plt.ylabel('HTTP Method')
plt.savefig('avg_response_size_per_method.png')
plt.show()
