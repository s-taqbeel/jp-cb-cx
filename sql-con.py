
import mysql.connector
from mysql.connector import errorcode

# Database connection details (replace with your actual values)
HOST = ""
PORT = 1433  # Check if this is the correct port for your MySQL server
DATABASE = ""
USERNAME = ""
PASSWORD = ""

print(HOST)
print(PORT)
print(DATABASE)
print(USERNAME)
print(PASSWORD)

# Establish connection
#import pdb;pdb.set_trace()
try:
    connection = mysql.connector.connect(
        host=HOST,
        port=PORT,
        database=DATABASE,
        user=USERNAME,
        password=PASSWORD,
        # connection_timeout=10,  # Add a timeout for the connection
       # trace=True  # Enable tracing
    )

    print("Connected to MySQL database successfully!")

# except mysql.connector.Error as err:
#     print("Error connecting to database:", err)
#     exit()
except mysql.connector.Error as err:
    print(f"Error: {err}")
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Error: Access denied. Check your username or password.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Error: Database does not exist.")
    elif err.errno == 2013:
        print("Error: Lost connection to MySQL server at 'reading initial communication packet', system error 54.")
    else:
        print(f"Error: {err}")
    exit()

# Sample aggregate query (replace with your desired query)
query = "SELECT COUNT(*) AS total_records FROM your_table_name"  # Example: Count records

try:
    cursor = connection.cursor()
    print("Running query:")
    print(query)  # Print the query before execution for clarity
    cursor.execute(query)
    result = cursor.fetchone()

    # Print the aggregate result
    print("Aggregate result:")
    print(result[0])  # Access the first element (index 0) of the single row returned by the query

except mysql.connector.Error as err:
    print("Error executing query:", err)

finally:
    if connection.is_connected():
        connection.cursor().close()
        connection.close()
        print("Connection closed.")
        
