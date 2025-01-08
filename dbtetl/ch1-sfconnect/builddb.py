import snowflake.connector
import os 
import json
import yaml
from dotenv import load_dotenv

# Load connection details from the YAML file
def load_connection_details(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)
    

# Load the .env file
load_dotenv()

# Access environment variables
ospassword = os.getenv("SNOWFLAKE_PASSWORD")

#print(f"User: {user}, Account: {account}")

# Connect to Snowflake
def connect_to_snowflake(connection_file):
    connection_details = load_connection_details(connection_file)
    connection = snowflake.connector.connect(
        user=connection_details.get('user'),
        password=ospassword,
        account=connection_details.get('account'),
        warehouse=connection_details.get('warehouse'),
        database=connection_details.get('database'),
        schema=connection_details.get('schema'),
        role=connection_details.get('role')
    )
    return connection


if __name__ == "__main__":
    connection_file = "dbtetl\snowflake_connections.yaml"  # Path to your connection file
    connection = connect_to_snowflake(connection_file)
    print("Connected to Snowflake")
    # Run a test query
    cursor = connection.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    print(f"Snowflake version: {cursor.fetchone()[0]}")
    cursor.close()
    connection.close()

    