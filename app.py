# app.py

from fastapi import FastAPI, HTTPException
import point_calculator # Import your calculation module
import logging
import os
from dotenv import load_dotenv # Still useful for other potential env vars
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
# Load environment variables (if any, other than MongoDB)
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on startup
    logging.info("Application startup...")
    # Ensure point_calculator.DATA_DIR is defined in your point_calculator module
    # if you want this directory creation logic to work.
    if hasattr(point_calculator, 'DATA_DIR') and point_calculator.DATA_DIR:
        if not os.path.exists(point_calculator.DATA_DIR):
            try:
                os.makedirs(point_calculator.DATA_DIR)
                logging.info(f"Created data directory: {point_calculator.DATA_DIR}")
            except OSError as e:
                logging.error(f"Could not create data directory {point_calculator.DATA_DIR}: {e}")
    else:
        logging.warning("point_calculator.DATA_DIR is not defined or is empty. Skipping data directory creation check.")
    yield
    # Code to run on shutdown (if any)
    logging.info("Application shutdown...")


app = FastAPI(lifespan=lifespan)
app.add_middleware(
     CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)
@app.get('/')
async def home():
    logging.info("Root path '/' accessed.")
    return {
        "message": "Welcome to the Fantasy Football Player Points API!",
        "endpoints": {
            "calculate_points": "/api/v1/calculate_player_points"
        },
        "instructions": "Make a GET request to /api/v1/calculate_player_points to get the data. This may take a moment to process all matches."
    }

@app.get('/api/v1/calculate_player_points')
async def get_player_points_api():
    logging.info("Received request for /api/v1/calculate_player_points")

    # Assuming point_calculator.generate_all_player_points_data() is synchronous.
    # Ensure this function exists and works as expected in your point_calculator.py
    try:
        data, error_message = point_calculator.generate_all_player_points_data()
    except AttributeError:
        logging.error("The function 'generate_all_player_points_data' was not found in 'point_calculator' module.")
        raise HTTPException(status_code=500, detail="Server configuration error: Point calculation function missing.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during point calculation: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")


    if error_message:
        logging.error(f"Error during calculation: {error_message}")
        raise HTTPException(status_code=500, detail=error_message)

    if not data and not error_message:
        logging.warning("Calculation resulted in no data, but no explicit error message.")
        return {"message": "No player point data generated. Check server logs for warnings."}

    logging.info(f"Successfully processed request. Returning data for {len(data) if data else 0} potential matches/items.")
    
    return data

# To run this application:
# 1. Save it as app.py (or main.py, then adjust uvicorn command).
# 2. Make sure you have FastAPI and Uvicorn installed in your venv:
#    pip install fastapi "uvicorn[standard]" python-dotenv
# 3. Ensure your 'point_calculator.py' (and '.env' file, if used for other settings)
#    are in the same directory or accessible via Python's import path.
#    And that 'point_calculator.py' has 'DATA_DIR' (if used) and 'generate_all_player_points_data'.
# 4. Run from your terminal (ensure venv is active):
#    uvicorn app:app --reload --host 0.0.0.0 --port 5001