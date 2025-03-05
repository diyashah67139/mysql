from flask import Flask, request, jsonify
from maticalgos.historical import historical
import datetime
import pandas as pd
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  

# Initialize Maticalgos with your credentials
ma = historical('diya.shah@finideas.com')
ma.login("425012")

@app.route('/fetch-data', methods=['POST'])
def fetch_data():
    try:
        # Receive JSON payload from C#
        params = request.json
        symbol = params.get("symbol")  # Example: "NIFTY"
        start_date_str = params.get("start_date")  # Example: "2023-01-01"
        end_date_str = params.get("end_date")  # Example: "2023-12-31"

        if not all([symbol, start_date_str, end_date_str]):
            return jsonify({"error": "Missing required parameters"}), 400

        # Convert string date to datetime.date object
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()

        # Initialize list to store data for all days
        all_data = []

        # Loop from start date to end date
        current_date = start_date
        while current_date <= end_date:
            if current_date.weekday() < 5:

                data = ma.get_data(symbol, current_date)

                # Check if data is a DataFrame
                if isinstance(data, pd.DataFrame):
                    if not data.empty:
                        json_data = data.to_dict(orient="records")  # Convert DataFrame to list of dictionaries
                        all_data.extend(json_data)
                elif isinstance(data, list): 
                    all_data.extend(data)
                elif isinstance(data, dict): 
                    all_data.append(data)

            current_date += datetime.timedelta(days=1)

        return jsonify({"data": all_data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
