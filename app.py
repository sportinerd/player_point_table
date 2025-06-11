# app.py

from flask import Flask, jsonify, request
import point_calculator # Import your calculation module
import logging 
import os 
from pymongo import MongoClient
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

app = Flask(__name__)

# Configure logging for Flask app
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Read values from .env
MONGODB_URI = os.getenv("MONGO_DB_URL")
MONGODB_CLIENT = os.getenv("MONGODB_DB_CLIENT")

# connect with database
client = MongoClient(MONGODB_URI)
db = client[MONGODB_CLIENT]

# Collections
# fixtures_collection = db["fixtures"]
# fixtures_by_team_collection = db["fixtures_by_team"]
fixtures_by_sportmonks = db["sportmonks_fixture_data"]

@app.route('/')
def home():
    app.logger.info("Root path '/' accessed.")
    return jsonify({
        "message": "Welcome to the Fantasy Football Player Points API!",
        "endpoints": {
            "calculate_points": "/api/v1/calculate_player_points"
        },
        "instructions": "Make a GET request to /api/v1/calculate_player_points to get the data. This may take a moment to process all matches."
    })

@app.route('/api/v1/calculate_player_points', methods=['GET'])
def get_player_points_api():
    app.logger.info("Received request for /api/v1/calculate_player_points")
    
    data, error_message = point_calculator.generate_all_player_points_data()
    
    if error_message:
        app.logger.error(f"Error during calculation: {error_message}")
        return jsonify({"error": error_message}), 500
    
    if not data and not error_message: 
        app.logger.warning("Calculation resulted in no data, but no explicit error message.")
        return jsonify({"message": "No player point data generated. Check server logs for warnings."}), 200

    app.logger.info(f"Successfully processed request. Returning data for {len(data)} matches.")
    if data:
        for eachMatch in data:
            eachMatch_dict= eachMatch
            match_id = eachMatch_dict["MatchAPUIDs"]
            home_api, away_api = match_id.split("_vs_")
            fixture_found_query = {"home_team.api_team_id":int(home_api),"away_team.api_team_id":int(away_api)}
            print(fixture_found_query)
            result = fixtures_by_sportmonks.find_one(fixture_found_query)
            current_fixture_api_id = None
            print("result")
            print(result)
            if result is not None:
                current_fixture_api_id = result.get("api_fixture_id")
                print("Result found and is not None -> ",current_fixture_api_id)
                eachMatch["fixture_api_id"] = current_fixture_api_id 


    return jsonify(data), 200

if __name__ == '__main__':
    if not os.path.exists(point_calculator.DATA_DIR):
        try:
            os.makedirs(point_calculator.DATA_DIR)
            app.logger.info(f"Created data directory: {point_calculator.DATA_DIR}")
        except OSError as e:
            app.logger.error(f"Could not create data directory {point_calculator.DATA_DIR}: {e}")

    app.logger.info("Starting Flask development server on http://127.0.0.1:5001")
    app.run(debug=True, host='0.0.0.0', port=5001)