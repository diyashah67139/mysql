# import os
# import ssl
# from dotenv import load_dotenv
# from flask import Flask, request, jsonify
# from maticalgos.historical import historical
# import pytds
# import datetime
# import pandas as pd
# import pyodbc
# import numpy as np
# from flask_cors import CORS

# app = Flask(__name__)
# CORS(app)
# # CORS(app,origins=["http://localhost:3000","https://674eb2927e66b7f220d5ebd1--bejewelled-marigold-b271f6.netlify.app/"])
# # CORS(app, resources={r"/api/*": {"origins": "*"}})

# # Load environment variables from .env file
# load_dotenv()


# # Access the environment variables
# MATICALGOS_USER = os.getenv('MATICALGOS_USER')
# MATICALGOS_PASS = os.getenv('MATICALGOS_PASS')
# DB_SERVER = os.getenv('DB_SERVER')
# DB_PORT = os.getenv('DB_PORT',"1433")
# DB_NAME = os.getenv('DB_NAME')
# DB_USER = os.getenv('DB_USER')
# DB_PASSWORD = os.getenv('DB_PASSWORD')

# # Initialize and login to Maticalgos
# ma = historical(MATICALGOS_USER)
# try:
#     login_response = ma.login(MATICALGOS_PASS)
#     print("Login successful!")
# except Exception as e:
#     print(f"Login failed: {e}")
#     login_response = None

# # SQL Server connection function
# def get_db_connection():
#     try:
#         conn = pyodbc.connect(
#             "DRIVER={{ODBC Driver 17 or SQL Server}};"
#             "SERVER={DB_SERVER};" 
#             "PORT={DB_PORT};"           
#             "DATABASE={DB_NAME};"
#             "UID={DB_USER};"
#             "PWD={DB_PASSWORD};"
#         )
#         print("Connected to the database!")
#         return conn
#     except Exception as e:
#         print(f"Database connection failed: {e}")
#         raise e


# @app.route('/')
# def home():
#     return jsonify({"message": "Hello, Welcome to Historical NSE Database!"})

# @app.route('/test-connection', methods=['GET'])
# def test_connection():
#     try:
#         # Establish the database connection
#         conn = get_db_connection()
        
#         # Create a cursor to interact with the database
#         cursor = conn.cursor()

#         # Execute a query to fetch the database version
#         cursor.execute("SELECT @@VERSION")  # Fetch SQL Server version
#         result = cursor.fetchone()  # Get the first row (which contains the version string)

#         # Close cursor and connection
#         cursor.close()
#         conn.close()

#         # Return the database version as the result
#         return jsonify({
#             "status": "success",
#             "message": "Database connected successfully!",
#             "result": result[0]  # result[0] contains the version string
#         })

#     except Exception as e:
#         # Handle any errors and return a meaningful message
#         return jsonify({
#             "status": "error",
#             "message": f"Failed to connect to the database: {str(e)}"
#         })


# @app.route('/get-data', methods=['POST'])
# def get_data():
#     conn = None
#     cursor = None
#     try:
#         # Get JSON data from request
#         data = request.get_json()
#         from_date = data.get('fromDate')
#         to_date = data.get('toDate')
#         selection = data.get('selection')

#         # Validate input
#         if not from_date or not to_date or not selection:
#             return jsonify({'error': 'Missing required parameters: fromDate, toDate, or selection'}), 400

#         # Dynamically set table name based on selection
#         table_name = f"{selection}Data"  # Example: "niftyData" or "bankniftyData"

#         # Connect to the database
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         # Fetch data
#         query = f"""
#         SELECT TOP 1000 * FROM {table_name}
#         WHERE date BETWEEN '{from_date}' AND '{to_date}'
#         """
#         cursor.execute(query)
#         rows = cursor.fetchall()

#         # Handle case when no data is found
#         if not rows:
#             print("no row")
#             return jsonify({'message': 'No data available'}), 200

#         # Convert result to a list of dictionaries
#         columns = ['close', 'date', 'high', 'low', 'oi', 'open', 'symbol', 'time', 'volume']
#         result = [dict(zip(columns, row)) for row in rows]

#         # Validate the result format before returning
#         if isinstance(result, list) and all(isinstance(item, dict) for item in result):
#             return jsonify(result)
#         else:
#             raise ValueError("Data format is incorrect. Expected a list of dictionaries.")

#     except Exception as e:
#         # General error handler
#         return jsonify({'error': str(e)}), 500

#     finally:
#         # Close the database connection
#         if 'cursor' in locals():
#             cursor.close()
#         if 'conn' in locals():
#             conn.close()

# @app.route('/insert-data', methods=['POST'])
# def insert_data():
#     data = request.json
#     from_date = data.get('fromDate')
#     to_date = data.get('toDate')
#     selection = data.get('selection')

#     # Validate input
#     if not from_date or not to_date or not selection:
#         return jsonify({"message": "Invalid input. Please provide fromDate, toDate, and selection."}), 400

#     try:
#         # Convert dates
#         from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d').date()
#         to_date = datetime.datetime.strptime(to_date, '%Y-%m-%d').date()

#         if from_date > to_date:
#             return jsonify({"message": "Start date must be less than end date."}), 400

#         # Call your existing data-fetching and insertion logic
#         inserted_dates, existing_dates = insert_data_into_db(from_date, to_date, selection)

#         return jsonify({
#             "message": f"Data inserted successfully for {selection} from {from_date} to {to_date}.",
#             "insertedDates": inserted_dates,
#             "existingDates": existing_dates
#         })
#     except Exception as e:
#         return jsonify({"message": f"Error: {str(e)}"}), 500


# def insert_data_into_db(from_date, to_date, symbol):
#     # Dynamically set table name based on the symbol
#     table_name = f"{symbol}Data"  # Example: "niftyData" or "bankniftyData"

#     try:
#         # Connect to the SQL Server
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         print("Connection successful!")

#         # Create table if it doesn't exist
#         create_table_query = f"""
#         IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
#         CREATE TABLE {table_name} (
#             [close] FLOAT,
#             [date] DATE,
#             [high] FLOAT,
#             [low] FLOAT,
#             [oi] FLOAT,
#             [open] FLOAT,
#             [symbol] NVARCHAR(50),
#             [time] NVARCHAR(10),
#             [volume] INT
#         )
#         """
#         cursor.execute(create_table_query)

#         # Check for existing dates in the database
#         check_query = f"""
#         SELECT DISTINCT date FROM {table_name}
#         WHERE date BETWEEN '{from_date}' AND '{to_date}'
#         """
#         cursor.execute(check_query)
#         existing_dates = {row[0] for row in cursor.fetchall()}

#         # Prepare INSERT query
#         insert_query = f"""
#         INSERT INTO {table_name} ([close], [date], [high], [low], [oi], [open], [symbol], [time], [volume])
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
#         """


#         # Loop through each day from start_date to end_date
#         current_date = from_date
#         inserted_dates = []
#         while current_date <= to_date:
#             # Skip weekends and already existing dates
#             if current_date.weekday() >= 5 or current_date in existing_dates:
#                 current_date += datetime.timedelta(days=1)
#                 continue

#             print(f"Fetching data for: {current_date}")
#             try:
#                 try:
#                     dataNifty = ma.get_data(symbol, current_date)
#                     print(f"Data fetched for {current_date}: {dataNifty}")
#                 except Exception as e:
#                     print(f"Failed to fetch data for {current_date}: {e}")

#                 # Ensure columns are numeric
#                 numeric_columns = ['close', 'high', 'low', 'oi', 'open']
#                 for col in numeric_columns:
#                     dataNifty[col] = pd.to_numeric(dataNifty[col], errors='coerce').fillna(0.0)

#                 # Insert rows
#                 for _, row in dataNifty.iterrows():
#                     try:
#                         cursor.execute(insert_query, (
#                             float(row['close']), row['date'], float(row['high']),
#                             float(row['low']), float(row['oi']), float(row['open']),
#                             row['symbol'], row['time'], row['volume']
#                         ))
#                     except Exception as ex:
#                         print(f"Skipping row due to error: {row}, Error: {ex}")

#                 # Commit for the current date
#                 conn.commit()
#                 inserted_dates.append(str(current_date))

#             except Exception as e:
#                 print(f"Error fetching/inserting data for {current_date}: {e}")

#             current_date += datetime.timedelta(days=1)

#         print("Data insertion completed!")
#         print(f"Inserted Dates: {inserted_dates}")
#         print(f"Skipped Existing Dates: {list(existing_dates)}")
#         return inserted_dates, list(existing_dates)

#     except pytds.DatabaseError as e:
#         print("Database error:", e)
#         return [],[]
#     except Exception as ex:
#         print("An error occurred:", ex)
#         return [],[]
#     finally:
#         # Close the connection
#         if 'cursor' in locals():
#             cursor.close()
#         if 'conn' in locals():
#             conn.close()
#             print("Connection closed.")
  
# @app.route('/delete-data', methods=['DELETE'])
# def delete_data():
#     try:
#         data = request.get_json()
#         from_date = data['fromDate']
#         to_date = data['toDate']
#         selection = data['selection']

#         # Dynamically set table name based on selection
#         table_name = f"{selection}Data"  # "niftyData" or "bankniftyData"

#         # Connect to SQL Server
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         # Delete data for the selected date range and symbol
#         delete_query = f"""
#         DELETE FROM {table_name}
#         WHERE date BETWEEN '{from_date}' AND '{to_date}'
#         """
#         cursor.execute(delete_query)
#         conn.commit()

#         return jsonify({'message': 'Data deleted successfully'})

#     except Exception as e:
#         return jsonify({'error': str(e)}), 500

#     finally:
#         # Close the database connection
#         cursor.close()
#         conn.close()

# @app.route('/export-to-sql', methods=['POST'])
# def export_to_sql():
    
#     # Get the data and SQL details from the request
#     data = request.json
#     from_date = data.get('fromDate')
#     to_date = data.get('toDate')
#     selection = data.get('selection') 
#     server = data.get('server')
#     database = data.get('database')
#     username = data.get('username')
#     password = data.get('password')
#     # Validate input
#     if not from_date or not to_date or not selection:
#         return jsonify({"message": "Invalid input. Please provide fromDate, toDate, and selection."}), 400
    
#     try:
#         # Convert dates
#         from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d').date()
#         to_date = datetime.datetime.strptime(to_date, '%Y-%m-%d').date()
        
#         if from_date > to_date:
#             return jsonify({"message": "Start date must be less than end date."}), 400
        
#         # Call your existing data-fetching and insertion logic
#         export_data_into_db(from_date, to_date, selection,server,database,username,password)
        
#         return jsonify({"message": f"Data inserted successfully for {selection} from {from_date} to {to_date}."})
#     except Exception as e:
#         return jsonify({"message": f"Error: {str(e)}"}), 500

        
# def export_data_into_db(from_date, to_date, symbol,server,database,username,password):
#     # Dynamically set table name based on the symbol
#     table_name = f"{symbol}Data" 
#     conn = None
#     cursor = None
#     try:
#         connection_string = (
#             f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#             f"SERVER={server};"  # Use the provided server
#             f"DATABASE={database};"  # Use the provided database
#             f"UID={username};"  # Use the provided username
#             f"PWD={password};"  # Use the provided password
#         )
#         # Connect to the SQL Server
#         conn = pyodbc.connect(connection_string)
#         cursor = conn.cursor()
#         print("Connection successful!")


#         # Create table if it doesn't exist
#         create_table_query = f"""
#         IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
#         CREATE TABLE {table_name} (
#             [close] FLOAT,
#             [date] DATE,
#             [high] FLOAT,
#             [low] FLOAT,
#             [oi] FLOAT,
#             [open] FLOAT,
#             [symbol] NVARCHAR(50),
#             [time] NVARCHAR(10),
#             [volume] INT
#         )
#         """
#         cursor.execute(create_table_query)

#         # Check for existing dates in the database
#         check_query = f"""
#         SELECT DISTINCT date FROM {table_name}
#         WHERE date BETWEEN '{from_date}' AND '{to_date}'
#         """
#         cursor.execute(check_query)
#         existing_dates = {row[0] for row in cursor.fetchall()}

#         # Prepare INSERT query
#         # Prepare INSERT query with 9 parameter placeholders
#         insert_query = f"""
#         INSERT INTO {table_name} ([close], [date], [high], [low], [oi], [open], [symbol], [time], [volume])
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
#         """


#         # Loop through each day from start_date to end_date
#         current_date = from_date
#         inserted_dates = []
#         while current_date <= to_date:
#             # Skip weekends and already existing dates
#             if current_date.weekday() >= 5 or current_date in existing_dates:
#                 current_date += datetime.timedelta(days=1)
#                 continue

#             print(f"Fetching data for: {current_date}")
#             try:
#                 # Fetch data for the current date
#                 dataNifty = ma.get_data(symbol, current_date)

#                 # Ensure columns are numeric
#                 numeric_columns = ['close', 'high', 'low', 'oi', 'open']
#                 for col in numeric_columns:
#                     dataNifty[col] = pd.to_numeric(dataNifty[col], errors='coerce').fillna(0.0)

#                 # Insert rows
#                 for _, row in dataNifty.iterrows():
#                     try:
#                         cursor.execute(insert_query, (
#                             float(row['close']), row['date'], float(row['high']),
#                             float(row['low']), float(row['oi']), float(row['open']),
#                             row['symbol'], row['time'], row['volume']
#                         ))
#                     except Exception as ex:
#                         print(f"Skipping row due to error: {row}, Error: {ex}")

#                 # Commit for the current date
#                 conn.commit()
#                 inserted_dates.append(str(current_date))

#             except Exception as e:
#                 print(f"Error fetching/inserting data for {current_date}: {e}")

#             current_date += datetime.timedelta(days=1)

#         print("Data insertion completed!")
#         print(f"Inserted Dates: {inserted_dates}")
#         print(f"Skipped Existing Dates: {list(existing_dates)}")

#     except pytds.DatabaseError as e:
#         print("Database error:", e)
#     except Exception as ex:
#         print("An error occurred:", ex)
#     finally:
#         # Close the connection
#         if 'cursor' in locals():
#             cursor.close()
#         if 'conn' in locals():
#             conn.close()
#             print("Connection closed.")
  

# # if __name__ == '__main__':
# #     app.run(debug=True, host='0.0.0.0', port=5001)

# if __name__ == '__main__':
#     app.run(debug=True)

import os
import ssl
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from maticalgos.historical import historical
# import pytds
import datetime
import pandas as pd
import pyodbc
import numpy as np
from flask_cors import CORS
import pymysql

app = Flask(__name__)
CORS(app)
# CORS(app,origins=["http://localhost:3000","https://674eb2927e66b7f220d5ebd1--bejewelled-marigold-b271f6.netlify.app/"])
# CORS(app, resources={r"/api/*": {"origins": "*"}})

# Load environment variables from .env file
load_dotenv()


# Access the environment variables
MATICALGOS_USER = os.getenv('MATICALGOS_USER')
MATICALGOS_PASS = os.getenv('MATICALGOS_PASS')
DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Initialize and login to Maticalgos
ma = historical(MATICALGOS_USER)
try:
    login_response = ma.login(MATICALGOS_PASS)
    print("Login successful!")
except Exception as e:
    print(f"Login failed: {e}")
    login_response = None

# SQL Server connection function
def get_db_connection():
    try:
        # conn = pyodbc.connect(
        #     f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        #     f"SERVER={DB_SERVER},1433;"
        #     f"DATABASE={DB_NAME};"
        #     f"UID={DB_USER};"
        #     f"PWD={DB_PASSWORD};"
        # )

        # Connection details
        conn = pymysql.connect(
            host=DB_SERVER,      # MySQL server address
            user=DB_USER,        # MySQL username
            password=DB_PASSWORD, # MySQL password
            database=DB_NAME,    # MySQL database name
            port=3306            # Default MySQL port
        )

        print("Connected to the database!")
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise e


@app.route('/')
def home():
    return jsonify({"message": "Hello, Welcome to Historical NSE Database!"})

@app.route('/test-connection', methods=['GET'])
def test_connection():
    try:
        # Establish the database connection
        conn = get_db_connection()

        # Create a cursor to interact with the database
        cursor = conn.cursor()

        # Execute a query to fetch the database version
        cursor.execute("SELECT @@VERSION")  # Fetch SQL Server version
        result = cursor.fetchone()  # Get the first row (which contains the version string)

        # Close cursor and connection
        cursor.close()
        conn.close()

        # Return the database version as the result
        return jsonify({
            "status": "success",
            "message": "Database connected successfully!",
            "result": result[0]  # result[0] contains the version string
        })

    except Exception as e:
        # Handle any errors and return a meaningful message
        return jsonify({
            "status": "error",
            "message": f"Failed to connect to the database: {str(e)}"
        })


@app.route('/get-data', methods=['POST'])
def get_data():
    conn = None
    cursor = None
    try:
        # Get JSON data from request
        data = request.get_json()
        from_date = data.get('fromDate')
        to_date = data.get('toDate')
        selection = data.get('selection')

        # Validate input
        if not from_date or not to_date or not selection:
            return jsonify({'error': 'Missing required parameters: fromDate, toDate, or selection'}), 400

        # Dynamically set table name based on selection
        table_name = f"{selection}Data"  # Example: "niftyData" or "bankniftyData"

        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch data
        query = f"""
        #SELECT TOP 1000 * FROM {table_name}
        #WHERE date BETWEEN '{from_date}' AND '{to_date}'
        SELECT * FROM `{table_name}`
        WHERE `date` BETWEEN '{from_date}' AND '{to_date}'
        LIMIT 1000;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # Handle case when no data is found
        if not rows:
            print("no row")
            return jsonify({'message': 'No data available'}), 200

        # Convert result to a list of dictionaries
        columns = ['close', 'date', 'high', 'low', 'oi', 'open', 'symbol', 'time', 'volume']
        result = [dict(zip(columns, row)) for row in rows]

        # Validate the result format before returning
        if isinstance(result, list) and all(isinstance(item, dict) for item in result):
            return jsonify(result)
        else:
            raise ValueError("Data format is incorrect. Expected a list of dictionaries.")

    except Exception as e:
        # General error handler
        return jsonify({'error': str(e)}), 500

    finally:
        # Close the database connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route('/insert-data', methods=['POST'])
def insert_data():
    data = request.json
    from_date = data.get('fromDate')
    to_date = data.get('toDate')
    selection = data.get('selection')

    # Validate input
    if not from_date or not to_date or not selection:
        return jsonify({"message": "Invalid input. Please provide fromDate, toDate, and selection."}), 400

    try:
        # Convert dates
        from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d').date()
        to_date = datetime.datetime.strptime(to_date, '%Y-%m-%d').date()

        if from_date > to_date:
            return jsonify({"message": "Start date must be less than end date."}), 400

        # Call your existing data-fetching and insertion logic
        inserted_dates, existing_dates = insert_data_into_db(from_date, to_date, selection)

        return jsonify({
            "message": f"Data inserted successfully for {selection} from {from_date} to {to_date}.",
            "insertedDates": inserted_dates,
            "existingDates": existing_dates
        })
    except Exception as e:
        return jsonify({"message": f"Error: {str(e)}"}), 500


# def insert_data_into_db(from_date, to_date, symbol):
#     # Dynamically set table name based on the symbol
#     table_name = f"{symbol}Data"  # Example: "niftyData" or "bankniftyData"

#     try:
#         # Connect to the SQL Server
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         print("Connection successful!")

#         # Create table if it doesn't exist
#         create_table_query = f"""

#         CREATE TABLE IF NOT EXISTS `{table_name}` (
#             close FLOAT,
#             date DATE,
#             high FLOAT,
#             low FLOAT,
#             oi FLOAT,
#             open FLOAT,
#             symbol VARCHAR(50),
#             time VARCHAR(10),
#             volume INT
#         )

#         """
#         cursor.execute(create_table_query)

#         # Check for existing dates in the database
#         check_query = f"""
#         SELECT DISTINCT `date` FROM `{table_name}`
#         WHERE `date` BETWEEN '{from_date}' AND '{to_date}'
#         """
#         cursor.execute(check_query)
#         existing_dates = {row[0] for row in cursor.fetchall()}

#         # Prepare INSERT query
#         insert_query = f"""
#         INSERT INTO {table_name} (close, date, high, low, oi, open, symbol, time, volume)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """


#         # Loop through each day from start_date to end_date
#         current_date = from_date
#         inserted_dates = []
#         while current_date <= to_date:
#             # Skip weekends and already existing dates
#             if current_date.weekday() >= 5 or current_date in existing_dates:
#                 current_date += datetime.timedelta(days=1)
#                 continue
#             dataNifty = pd.DataFrame()

#             print(f"Fetching data for: {current_date}")
#             try:
#                 try:
#                     dataNifty = ma.get_data(symbol, current_date)
#                     print(f"Raw data fetched for {current_date}: {dataNifty}")
#                     if dataNifty is None or dataNifty == '':
#                         print(f"No data returned for {current_date}")
#                         current_date += datetime.timedelta(days=1)
#                         continue
#                 except Exception as fetch_error:
#                     print(f"Failed to fetch data for {current_date}: {fetch_error}")
#                     current_date += datetime.timedelta(days=1)
#                     continue


#                 # Ensure columns are numeric
#                 numeric_columns = ['close', 'high', 'low', 'oi', 'open']
#                 for col in numeric_columns:
#                     dataNifty[col] = pd.to_numeric(dataNifty[col], errors='coerce').fillna(0.0)

#                 # Insert rows
#                 for _, row in dataNifty.iterrows():
#                     try:
#                         cursor.executemany(insert_query, (
#                             float(row['close']), row['date'], float(row['high']),
#                             float(row['low']), float(row['oi']), float(row['open']),
#                             row['symbol'], row['time'], int(row['volume'])
#                         ))
#                     except Exception as ex:
#                         print(f"Skipping row due to error: {row}, Error: {ex}")

#                 # Commit for the current date
#                 conn.commit()
#                 inserted_dates.append(str(current_date))

#             except Exception as e:
#                 print(f"Error fetching/inserting data for {current_date}: {e}")

#             current_date += datetime.timedelta(days=1)

#         print("Data insertion completed!")
#         print(f"Inserted Dates: {inserted_dates}")
#         print(f"Skipped Existing Dates: {list(existing_dates)}")
#         return inserted_dates, list(existing_dates)

#     except Exception as ex:
#         print("An error occurred:", ex)
#         return [],[]
#     finally:
#         # Close the connection
#         if 'cursor' in locals():
#             cursor.close()
#         if 'conn' in locals():
#             conn.close()
#             print("Connection closed.")
def insert_data_into_db(from_date, to_date, symbol):
    # Dynamically set table name based on the symbol
    table_name = f"{symbol}Data"  # Example: "niftyData" or "bankniftyData"

    try:
        # Connect to the MySQL database
        conn = get_db_connection()
        cursor = conn.cursor()
        print("Connection successful!")

        # Create table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            `close` FLOAT,
            `date` DATE,
            `high` FLOAT,
            `low` FLOAT,
            `oi` FLOAT,
            `open` FLOAT,
            `symbol` VARCHAR(50),
            `time` VARCHAR(10),
            `volume` INT
        )
        """
        cursor.execute(create_table_query)

        # Check for existing dates in the database
        check_query = f"""
        SELECT DISTINCT `date` FROM {table_name}
        WHERE `date` BETWEEN %s AND %s
        """
        cursor.execute(check_query, (from_date, to_date))
        existing_dates = {row[0] for row in cursor.fetchall()}

        # Prepare INSERT query
        insert_query = f"""
        INSERT INTO {table_name} (`close`, `date`, `high`, `low`, `oi`, `open`, `symbol`, `time`, `volume`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Loop through each day from start_date to end_date
        current_date = from_date
        inserted_dates = []
        while current_date <= to_date:
            # Skip weekends and already existing dates
            if current_date.weekday() >= 5 or current_date in existing_dates:
                current_date += datetime.timedelta(days=1)
                continue

            print(f"Fetching data for: {current_date}")
            try:
                try:
                    dataNifty = ma.get_data(symbol, current_date)
                    print(f"Data fetched for {current_date}: {dataNifty}")
                except Exception as e:
                    print(f"Failed to fetch data for {current_date}: {e}")
                    current_date += datetime.timedelta(days=1)
                    continue

                # Ensure columns are numeric
                numeric_columns = ['close', 'high', 'low', 'oi', 'open']
                for col in numeric_columns:
                    dataNifty[col] = pd.to_numeric(dataNifty[col], errors='coerce').fillna(0.0)

                # Insert rows
                for _, row in dataNifty.iterrows():
                    try:
                        cursor.execute(insert_query, (
                            float(row['close']), row['date'], float(row['high']),
                            float(row['low']), float(row['oi']), float(row['open']),
                            row['symbol'], row['time'], row['volume']
                        ))
                    except Exception as ex:
                        print(f"Skipping row due to error: {row}, Error: {ex}")

                # Commit for the current date
                conn.commit()
                inserted_dates.append(str(current_date))

            except Exception as e:
                print(f"Error fetching/inserting data for {current_date}: {e}")

            current_date += datetime.timedelta(days=1)

        print("Data insertion completed!")
        print(f"Inserted Dates: {inserted_dates}")
        print(f"Skipped Existing Dates: {list(existing_dates)}")
        return inserted_dates, list(existing_dates)

    # except mysql.connector.Error as e:
    #     print("Database error:", e)
    #     return [],[]
    except Exception as ex:
        print("An error occurred:", ex)
        return [],[]
    finally:
        # Close the connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            print("Connection closed.")

@app.route('/delete-data', methods=['DELETE'])
def delete_data():
    try:
        data = request.get_json()
        from_date = data['fromDate']
        to_date = data['toDate']
        selection = data['selection']

        # Dynamically set table name based on selection
        table_name = f"{selection}Data"  # "niftyData" or "bankniftyData"

        # Connect to SQL Server
        conn = get_db_connection()
        cursor = conn.cursor()

        # Delete data for the selected date range and symbol
        delete_query = f"""
        DELETE FROM `{table_name}`
        WHERE date BETWEEN '{from_date}' AND '{to_date}'
        """
        cursor.execute(delete_query)
        conn.commit()

        return jsonify({'message': 'Data deleted successfully'})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        # Close the database connection
        cursor.close()
        conn.close()

@app.route('/export-to-sql', methods=['POST'])
def export_to_sql():

    # Get the data and SQL details from the request
    data = request.json
    from_date = data.get('fromDate')
    to_date = data.get('toDate')
    selection = data.get('selection')
    server = data.get('server')
    database = data.get('database')
    username = data.get('username')
    password = data.get('password')
    # Validate input
    if not from_date or not to_date or not selection:
        return jsonify({"message": "Invalid input. Please provide fromDate, toDate, and selection."}), 400

    try:
        # Convert dates
        from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d').date()
        to_date = datetime.datetime.strptime(to_date, '%Y-%m-%d').date()

        if from_date > to_date:
            return jsonify({"message": "Start date must be less than end date."}), 400

        # Call your existing data-fetching and insertion logic
        export_data_into_db(from_date, to_date, selection,server,database,username,password)

        return jsonify({"message": f"Data inserted successfully for {selection} from {from_date} to {to_date}."})
    except Exception as e:
        return jsonify({"message": f"Error: {str(e)}"}), 500


def export_data_into_db(from_date, to_date, symbol,server,database,username,password):
    # Dynamically set table name based on the symbol
    table_name = f"{symbol}Data"
    conn = None
    cursor = None
    try:
        # connection_string = (
        #     f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        #     f"SERVER={server};"  # Use the provided server
        #     f"DATABASE={database};"  # Use the provided database
        #     f"UID={username};"  # Use the provided username
        #     f"PWD={password};"  # Use the provided password
        # )
        # # Connect to the SQL Server
        # conn = pyodbc.connect(connection_string)
        conn = pymysql.connect(
            host=server,      # MySQL server address
            user=username,        # MySQL username
            password=password, # MySQL password
            database=database,    # MySQL database name
            port=3306            # Default MySQL port
        )
        cursor = conn.cursor()
        print("Connection successful!")


        # Create table if it doesn't exist
        create_table_query = f"""
        # IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
        # CREATE TABLE {table_name} (
        #     [close] FLOAT,
        #     [date] DATE,
        #     [high] FLOAT,
        #     [low] FLOAT,
        #     [oi] FLOAT,
        #     [open] FLOAT,
        #     [symbol] NVARCHAR(50),
        #     [time] NVARCHAR(10),
        #     [volume] INT
        # )
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `close` FLOAT,
            `date` DATE,
            `high` FLOAT,
            `low` FLOAT,
            `oi` FLOAT,
            `open` FLOAT,
            `symbol` VARCHAR(50),
            `time` VARCHAR(10),
            `volume` INT
        )
        """
        cursor.execute(create_table_query)

        # Check for existing dates in the database
        check_query = f"""
        # SELECT DISTINCT date FROM {table_name}
        # WHERE date BETWEEN '{from_date}' AND '{to_date}'
        SELECT DISTINCT `date` FROM `{table_name}`
        WHERE `date` BETWEEN '{from_date}' AND '{to_date}'
        """
        cursor.execute(check_query)
        existing_dates = {row[0] for row in cursor.fetchall()}

        # Prepare INSERT query
        # Prepare INSERT query with 9 parameter placeholders
        insert_query = f"""
        INSERT INTO `{table_name}` (`close`, `date`, `high`, `low`, `oi`, `open`, `symbol`, `time`, `volume`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """


        # Loop through each day from start_date to end_date
        current_date = from_date
        inserted_dates = []
        while current_date <= to_date:
            # Skip weekends and already existing dates
            if current_date.weekday() >= 5 or current_date in existing_dates:
                current_date += datetime.timedelta(days=1)
                continue

            print(f"Fetching data for: {current_date}")
            try:
                # Fetch data for the current date
                dataNifty = ma.get_data(symbol, current_date)

                # Ensure columns are numeric
                numeric_columns = ['close', 'high', 'low', 'oi', 'open']
                for col in numeric_columns:
                    dataNifty[col] = pd.to_numeric(dataNifty[col], errors='coerce').fillna(0.0)

                # Insert rows
                for _, row in dataNifty.iterrows():
                    try:
                        cursor.execute(insert_query, (
                            float(row['close']), row['date'], float(row['high']),
                            float(row['low']), float(row['oi']), float(row['open']),
                            row['symbol'], row['time'], row['volume']
                        ))
                    except Exception as ex:
                        print(f"Skipping row due to error: {row}, Error: {ex}")

                # Commit for the current date
                conn.commit()
                inserted_dates.append(str(current_date))

            except Exception as e:
                print(f"Error fetching/inserting data for {current_date}: {e}")

            current_date += datetime.timedelta(days=1)

        print("Data insertion completed!")
        print(f"Inserted Dates: {inserted_dates}")
        print(f"Skipped Existing Dates: {list(existing_dates)}")

    # except pytds.DatabaseError as e:
    #     print("Database error:", e)
    except Exception as ex:
        print("An error occurred:", ex)
    finally:
        # Close the connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            print("Connection closed.")


# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5001)

if __name__ == '__main__':
    app.run(debug=True)
