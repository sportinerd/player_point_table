import pandas as pd
import numpy as np
import json
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from scipy.stats import poisson
import sys
import os
import io # Added for parsing fixture string
import csv # Added for parsing fixture string
from typing import Dict, List, Any, FrozenSet, Tuple # Added for type hinting

# --- Configuration & Constants ---
DATA_DIR = 'data'
HTML_ODDS_FP = os.path.join(DATA_DIR, 'fifa_club_wc_odds.html')
MD_ODDS_FP = os.path.join(DATA_DIR, 'fifa_club_wc_odds.md')
CS_JSON_FP = os.path.join(DATA_DIR, 'correct_score.json')
PLAYER_STATS_FP = os.path.join(DATA_DIR, 'merged_mapped_players.xlsx')

# FDR Calculation Weights
OUTRIGHT_COMPONENT_WEIGHTS = {
    'base_strength_from_odds': 0.70,
    'venue_impact': 0.20,
    'fatigue': 0.10
}
FINAL_FDR_WEIGHTS = {
    'outright': 0.40,
    'correct_score': 0.60
}

# Player Points Calculation
AVERAGE_TOTAL_GOALS_IN_MATCH = 2.7
MAX_POISSON_GOALS = 7

# --- Team Name Mapping (Ensure this is comprehensive) ---
TEAM_NAME_MAPPING = {
    "Real Madrid": "Real Madrid CF", "Manchester City": "Manchester City FC",
    "Bayern Munich": "FC Bayern München", "PSG": "Paris Saint-Germain",
    "Inter": "FC Internazionale Milano", "Chelsea": "Chelsea FC",
    "Atl. Madrid": "Atlético de Madrid", "Dortmund": "Borussia Dortmund",
    "Juventus": "Juventus FC", "FC Porto": "FC Porto",
    "Flamengo RJ": "CR Flamengo", "Benfica": "SL Benfica",
    "Palmeiras": "SE Palmeiras", "Boca Juniors": "CA Boca Juniors",
    "River Plate": "CA River Plate", "Botafogo RJ": "Botafogo FR",
    "Fluminense": "Fluminense FC", "Al Hilal": "Al Hilal SFC",
    "Inter Miami": "Inter Miami CF", "Salzburg": "FC Salzburg",
    "Los Angeles FC": "LAFC", "Seattle Sounders": "Seattle Sounders FC",
    "Al Ahly": "Al Ahly FC", "Pachuca": "CF Pachuca",
    "Urawa Reds": "Urawa Red Diamonds", "Ulsan Hyundai": "Ulsan HD FC",
    "Al Ain": "Al Ain FC", "Monterrey": "CF Monterrey",
    "Esperance Tunis": "Espérance Sportive de Tunis", "ES Tunis": "Espérance Sportive de Tunis", # From fixture data
    "Wydad Athletic": "Wydad AC", "Wydad Casablanca": "Wydad AC", # From fixture data
    "Mamelodi Sundowns": "Mamelodi Sundowns FC", "Auckland City": "Auckland City FC",

    # Canonical names (mapping to themselves for completeness)
    "Real Madrid CF": "Real Madrid CF", "Manchester City FC": "Manchester City FC",
    "FC Bayern München": "FC Bayern München", "Paris Saint-Germain": "Paris Saint-Germain",
    "Paris Saint Germain": "Paris Saint-Germain", # Variation from fixture list
    "FC Internazionale Milano": "FC Internazionale Milano", "Chelsea FC": "Chelsea FC",
    "Atlético de Madrid": "Atlético de Madrid", "Borussia Dortmund": "Borussia Dortmund",
    "Juventus FC": "Juventus FC", "CR Flamengo": "CR Flamengo",
    "SL Benfica": "SL Benfica", "SE Palmeiras": "SE Palmeiras",
    "CA Boca Juniors": "CA Boca Juniors", "CA River Plate": "CA River Plate",
    "Botafogo FR": "Botafogo FR", "Fluminense FC": "Fluminense FC",
    "Al Hilal SFC": "Al Hilal SFC", "Inter Miami CF": "Inter Miami CF",
    "FC Salzburg": "FC Salzburg", "Seattle Sounders FC": "Seattle Sounders FC",
    "Al Ahly FC": "Al Ahly FC", "CF Pachuca": "CF Pachuca",
    "Urawa Red Diamonds": "Urawa Red Diamonds", "Ulsan HD FC": "Ulsan HD FC",
    "Al Ain FC": "Al Ain FC", "CF Monterrey": "CF Monterrey",
    "Espérance Sportive de Tunis": "Espérance Sportive de Tunis",
    "Wydad AC": "Wydad AC", "Mamelodi Sundowns FC": "Mamelodi Sundowns FC",
    "Auckland City FC": "Auckland City FC", "LAFC": "LAFC",

    # Potential JSON variations & Fixture variations
    "Al Ahly SC": "Al Ahly FC", "Paris SG": "Paris Saint-Germain",
    "Atletico Madrid": "Atlético de Madrid", # Direct name from fixture list
    "Man City": "Manchester City FC", "Bayern": "FC Bayern München",
    "Inter Milan": "FC Internazionale Milano", "Atl Madrid": "Atlético de Madrid",
    "Porto": "FC Porto", # From fixture list
    "Botafogo": "Botafogo FR", # From fixture list
    "Flamengo": "CR Flamengo", # From fixture list
    "Ulsan HD": "Ulsan HD FC", # From fixture list
    "Sociedade Esportiva Palmeiras": "SE Palmeiras",
    "Botafogo de Futebol e Regatas": "Botafogo FR",
    "Fluminense Football Club": "Fluminense FC",
    "Red Bull Salzburg": "FC Salzburg",
    "Al-Hilal": "Al Hilal SFC",
}

# --- Team Details ---
TEAM_DETAILS = {
    "Al Ahly FC": {"short_code": "AHL", "api_id": 460,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Al%20Ahly%20FC%20round.png"},
    "Al Ain FC": {"short_code": "AAN", "api_id": 7780,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Al%20Ain%20FC%20round.png"},
    "Al Hilal SFC": {"short_code": "HIL", "api_id": 7011,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Al%20Hilal%20round.png"},
    "Atlético de Madrid": {"short_code": "ATM", "api_id": 7980,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Atl%C3%A9tico%20de%20Madrid%20round.png"},
    "Auckland City FC": {"short_code": "AFC", "api_id": 1022,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Auckland%20City%20FC%20round.png"},
    "SL Benfica": {"short_code": "BEN", "api_id": 605,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/SL%20Benfica%20round.png"},
    "CA Boca Juniors": {"short_code": "BOC", "api_id": 587,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/CA%20Boca%20Juniors%20round.png"},
    "Borussia Dortmund": {"short_code": "BVB", "api_id": 68,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Borussia%20Dortmund%20round.png"},
    "Botafogo FR": {"short_code": "BOT", "api_id": 2864,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Botafogo%20round.png"},
    "Chelsea FC": {"short_code": "CHE", "api_id": 18,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Chelsea%20FC%20round.png"},
    "Espérance Sportive de Tunis": {"short_code": "EST", "api_id": 5832,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Esp%C3%A9rance%20Sportive%20de%20Tunis%20round.png"},
    "FC Bayern München": {"short_code": "BAY", "api_id": 503,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/FC%20Bayern%20M%C3%BCnchen%20round.png"},
    "CR Flamengo": {"short_code": "FLA", "api_id": 1024,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/CR%20Flamengo%20round.png"},
    "Fluminense FC": {"short_code": "FLU", "api_id": 1095,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Fluminense%20FC%20round.png"},
    "FC Internazionale Milano": {"short_code": "INT", "api_id": 2930,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/FC%20Internazionale%20Milano%20round.png"},
    "Inter Miami CF": {"short_code": "MIA", "api_id": 239235,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Inter%20Miami%20CF%20round.png"},
    "Juventus FC": {"short_code": "JUV", "api_id": 625,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Juventus%20FC%20round.png"},
    "LAFC": {"short_code": "LAF", "api_id": 147671,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/LAFC%20round.png"},
    "Mamelodi Sundowns FC": {"short_code": "MSF", "api_id": 6755,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Mamelodi%20Sundowns%20FC%20round.png"},
    "Manchester City FC": {"short_code": "MCI", "api_id": 9,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Manchester%20City%20FC%20round.png"},
    "CF Monterrey": {"short_code": "MON", "api_id": 2662,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/CF%20Monterrey%20round.png"},
    "CF Pachuca": {"short_code": "PAC", "api_id": 10036,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/CF%20Pachuca%20round.png"},
    "SE Palmeiras": {"short_code": "PAL", "api_id": 3422,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/SE%20Palmeiras%20round.png"},
    "Paris Saint-Germain": {"short_code": "PSG", "api_id": 591,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Paris%20Saint-Germain%20round.png"},
    "FC Porto": {"short_code": "POR", "api_id": 652,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/FC%20Porto%20round.png"},
    "Real Madrid CF": {"short_code": "RMA", "api_id": 3468,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Real%20Madrid%20C.%20F.%20round.png"},
    "CA River Plate": {"short_code": "RIV", "api_id": 10002,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/CA%20River%20Plate%20round.png"},
    "FC Salzburg": {"short_code": "SAL", "api_id": 49,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/FC%20Salzburg%20round.png"},
    "Seattle Sounders FC": {"short_code": "SEA", "api_id": 2649,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Seattle%20Sounders%20FC%20round.png"},
    "Ulsan HD FC": {"short_code": "UHD", "api_id": 5839,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Ulsan%20HD%20round.png"},
    "Urawa Red Diamonds": {"short_code": "URD", "api_id": 280,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Urawa%20Red%20Diamonds%20round.png"},
    "Wydad AC": {"short_code": "WAC", "api_id": 2846,"image":"https://fantasyfootball.sgp1.cdn.digitaloceanspaces.com/cwc%20team%20logo/Wydad%20AC%20round.png"}
}
DEFAULT_TEAM_DETAIL = {"short_code": "N/A", "api_id": None, "image": None}

# Ensure all TEAM_DETAILS keys are in TEAM_NAME_MAPPING
for team_name_detail_key in TEAM_DETAILS.keys():
    if team_name_detail_key not in TEAM_NAME_MAPPING:
        TEAM_NAME_MAPPING[team_name_detail_key] = team_name_detail_key

TIER_DISPLAY_MAPPING = {
    "Very Easy": "Very Easy", "Easy": "Easy", "Moderate": "Average Difficulty",
    "Hard": "Difficult", "Very Hard": "Very Difficult",
}

# --- Fixture ID and GW Data Handling (from clean_sheet_calculator.py approach) ---
FIXTURE_ID_GW_LOOKUP: Dict[Tuple[str, str, str], Dict[str, str]] = {} # Key: (canonical_home, canonical_away, date_str YYYY-MM-DD), Value: {"fixture_id": ..., "GW": ...}

# Raw fixture data string (tab-separated) - same as in clean_sheet_calculator.py
# This is the authoritative source for fixture_id and GW
FULL_FIXTURE_DATA_RAW = """fixture_id	stage_name	starting_at	home_team_name	away_team_name	group_name	home_team_id	away_team_id	GW
67cfda1c36a76522457ee1b9	Group Stage	2025-06-15 0:00:00	Al Ahly	Inter Miami	Group A	67b8be4865db8d4ef5b05df6	67b8be4e65db8d4ef5b05f49	1
67cfda6736a76522457eeda4	Group Stage	2025-06-15 16:00:00	FC Bayern München	Auckland City	Group C	67b8be4865db8d4ef5b05dfd	67b8be4965db8d4ef5b05e3d	1
67cfda1b36a76522457ee1b8	Group Stage	2025-06-15 19:00:00	Paris Saint Germain	Atlético Madrid	Group B	67b8be4865db8d4ef5b05e0c	67b8be4c65db8d4ef5b05f03	1
67cfda1e36a76522457ee5a2	Group Stage	2025-06-15 22:00:00	Palmeiras	Porto	Group A	67b8be4b65db8d4ef5b05e92	67b8be4865db8d4ef5b05e1b	1
67cfda2436a76522457ee5a7	Group Stage	2025-06-16 2:00:00	Botafogo	Seattle Sounders	Group B	67b8be4a65db8d4ef5b05e7f	67b8be4a65db8d4ef5b05e6e	1
67cfda3836a76522457ee5b4	Group Stage	2025-06-16 19:00:00	Chelsea	Los Angeles FC	Group D	67b8be4565db8d4ef5b05d90	683d905b988d77e1048fd503	1
67cfda4c36a76522457ee9a9	Group Stage	2025-06-16 22:00:00	Boca Juniors	Benfica	Group C	67b8be4865db8d4ef5b05e0a	67b8be4865db8d4ef5b05e12	1
67cfda5236a76522457ee9af	Group Stage	2025-06-17 1:00:00	Flamengo	ES Tunis	Group D	67b8be4965db8d4ef5b05e3e	683e0419988d77e1048fd51c	1
67cfda4136a76522457ee9a2	Group Stage	2025-06-17 16:00:00	Fluminense	Borussia Dortmund	Group F	67b8be4965db8d4ef5b05e43	67b8be4665db8d4ef5b05db2	1
67cfda6236a76522457eeda1	Group Stage	2025-06-17 19:00:00	River Plate	Urawa Reds	Group E	67b8be4d65db8d4ef5b05f16	67b8be4765db8d4ef5b05dd3	1
67cfda4436a76522457ee9a4	Group Stage	2025-06-17 22:00:00	Ulsan HD	Mamelodi Sundowns	Group F	67b8be4c65db8d4ef5b05ed6	67b8be4c65db8d4ef5b05ef1	1
67cfda3c36a76522457ee5b7	Group Stage	2025-06-18 1:00:00	Monterrey	Inter	Group E	67b8be4a65db8d4ef5b05e6f	67b8be4a65db8d4ef5b05e82	1
67cfda3b36a76522457ee5b6	Group Stage	2025-06-18 16:00:00	Manchester City	Wydad Casablanca	Group G	67b8be4565db8d4ef5b05d87	67b8be4a65db8d4ef5b05e7e	1
67cfda7336a76522457eedac	Group Stage	2025-06-18 19:00:00	Real Madrid	Al Hilal	Group H	67b8be4b65db8d4ef5b05e95	67b8be4c65db8d4ef5b05ef8	1
67cfda3f36a76522457ee9a1	Group Stage	2025-06-18 22:00:00	Pachuca	Salzburg	Group H	67b8be4d65db8d4ef5b05f17	67b8be4565db8d4ef5b05da6	1
67cfda2936a76522457ee5aa	Group Stage	2025-06-19 1:00:00	Al Ain	Juventus	Group G	67b8be4c65db8d4ef5b05f00	67b8be4865db8d4ef5b05e15	1
67cfda2136a76522457ee5a4	Group Stage	2025-06-19 16:00:00	Palmeiras	Al Ahly	Group A	67b8be4b65db8d4ef5b05e92	67b8be4865db8d4ef5b05df6	2
67cfda2036a76522457ee5a3	Group Stage	2025-06-19 19:00:00	Inter Miami	Porto	Group A	67b8be4e65db8d4ef5b05f49	67b8be4865db8d4ef5b05e1b	2
67cfda1836a76522457ee1b6	Group Stage	2025-06-19 22:00:00	Seattle Sounders	Atlético Madrid	Group B	67b8be4a65db8d4ef5b05e6e	67b8be4c65db8d4ef5b05f03	2
67cfda2336a76522457ee5a6	Group Stage	2025-06-20 1:00:00	Paris Saint Germain	Botafogo	Group B	67b8be4865db8d4ef5b05e0c	67b8be4a65db8d4ef5b05e7f	2
67cfda4d36a76522457ee9aa	Group Stage	2025-06-20 16:00:00	Benfica	Auckland City	Group C	67b8be4865db8d4ef5b05e12	67b8be4965db8d4ef5b05e3d	2
67cfda6836a76522457eeda5	Group Stage	2025-06-20 18:00:00	Flamengo	Chelsea	Group D	67b8be4965db8d4ef5b05e3e	67b8be4565db8d4ef5b05d90	2
67cfda4f36a76522457ee9ac	Group Stage	2025-06-20 22:00:00	Los Angeles FC	ES Tunis	Group D	683d905b988d77e1048fd503	683e0419988d77e1048fd51c	2
67cfda4936a76522457ee9a7	Group Stage	2025-06-21 1:00:00	FC Bayern München	Boca Juniors	Group C	67b8be4865db8d4ef5b05dfd	67b8be4865db8d4ef5b05e0a	2
67cfda3e36a76522457ee9a0	Group Stage	2025-06-21 16:00:00	Mamelodi Sundowns	Borussia Dortmund	Group F	67b8be4c65db8d4ef5b05ef1	67b8be4665db8d4ef5b05db2	2
67cfda5436a76522457ee9b0	Group Stage	2025-06-21 19:00:00	Inter	Urawa Reds	Group E	67b8be4a65db8d4ef5b05e82	67b8be4765db8d4ef5b05dd3	2
67cfda3936a76522457ee5b5	Group Stage	2025-06-21 22:00:00	Fluminense	Ulsan HD	Group F	67b8be4965db8d4ef5b05e43	67b8be4c65db8d4ef5b05ed6	2
67cfda7136a76522457eedab	Group Stage	2025-06-22 1:00:00	River Plate	Monterrey	Group E	67b8be4d65db8d4ef5b05f16	67b8be4a65db8d4ef5b05e6f	2
67cfda6e36a76522457eeda9	Group Stage	2025-06-22 16:00:00	Juventus	Wydad Casablanca	Group G	67b8be4865db8d4ef5b05e15	67b8be4a65db8d4ef5b05e7e	2
67cfda6b36a76522457eeda7	Group Stage	2025-06-22 19:00:00	Real Madrid	Pachuca	Group H	67b8be4b65db8d4ef5b05e95	67b8be4d65db8d4ef5b05f17	2
67cfda4236a76522457ee9a3	Group Stage	2025-06-22 22:00:00	Salzburg	Al Hilal	Group H	67b8be4565db8d4ef5b05da6	67b8be4c65db8d4ef5b05ef8	2
67cfda6536a76522457eeda3	Group Stage	2025-06-23 1:00:00	Manchester City	Al Ain	Group G	67b8be4565db8d4ef5b05d87	67b8be4c65db8d4ef5b05f00	2
67cfda1636a76522457ee1b5	Group Stage	2025-06-23 19:00:00	Seattle Sounders	Paris Saint Germain	Group B	67b8be4a65db8d4ef5b05e6e	67b8be4865db8d4ef5b05e0c	3
67cfda1936a76522457ee1b7	Group Stage	2025-06-23 19:00:00	Atlético Madrid	Botafogo	Group B	67b8be4c65db8d4ef5b05f03	67b8be4a65db8d4ef5b05e7f	3
67cfda1336a76522457ee1b3	Group Stage	2025-06-24 1:00:00	Inter Miami	Palmeiras	Group A	67b8be4e65db8d4ef5b05f49	67b8be4b65db8d4ef5b05e92	3
67cfda1536a76522457ee1b4	Group Stage	2025-06-24 1:00:00	Porto	Al Ahly	Group A	67b8be4865db8d4ef5b05e1b	67b8be4865db8d4ef5b05df6	3
67cfda5536a76522457ee9b1	Group Stage	2025-06-24 19:00:00	Auckland City	Boca Juniors	Group C	67b8be4965db8d4ef5b05e3d	67b8be4865db8d4ef5b05e0a	3
67cfda5836a76522457ee9b3	Group Stage	2025-06-24 19:00:00	Benfica	FC Bayern München	Group C	67b8be4865db8d4ef5b05e12	67b8be4865db8d4ef5b05dfd	3
67cfda4a36a76522457ee9a8	Group Stage	2025-06-25 1:00:00	Los Angeles FC	Flamengo	Group D	683d905b988d77e1048fd503	67b8be4965db8d4ef5b05e3e	3
67cfda6136a76522457eeda0	Group Stage	2025-06-25 1:00:00	ES Tunis	Chelsea	Group D	683e0419988d77e1048fd51c	67b8be4565db8d4ef5b05d90	3
67cfda5136a76522457ee9ae	Group Stage	2025-06-25 19:00:00	Mamelodi Sundowns	Fluminense	Group F	67b8be4c65db8d4ef5b05ef1	67b8be4965db8d4ef5b05e43	3
67cfda4636a76522457ee9a5	Group Stage	2025-06-25 19:00:00	Borussia Dortmund	Ulsan HD	Group F	67b8be4665db8d4ef5b05db2	67b8be4c65db8d4ef5b05ed6	3
67cfda2736a76522457ee5a9	Group Stage	2025-06-26 1:00:00	Urawa Reds	Monterrey	Group E	67b8be4765db8d4ef5b05dd3	67b8be4a65db8d4ef5b05e6f	3
67cfda3636a76522457ee5b3	Group Stage	2025-06-26 1:00:00	Inter	River Plate	Group E	67b8be4a65db8d4ef5b05e82	67b8be4d65db8d4ef5b05f16	3
67cfda4736a76522457ee9a6	Group Stage	2025-06-26 19:00:00	Juventus	Manchester City	Group G	67b8be4865db8d4ef5b05e15	67b8be4565db8d4ef5b05d87	3
67cfda5736a76522457ee9b2	Group Stage	2025-06-26 19:00:00	Wydad Casablanca	Al Ain	Group G	67b8be4a65db8d4ef5b05e7e	67b8be4c65db8d4ef5b05f00	3
67cfda5d36a76522457eebcf	Group Stage	2025-06-27 1:00:00	Al Hilal	Pachuca	Group H	67b8be4c65db8d4ef5b05ef8	67b8be4d65db8d4ef5b05f17	3
67cfda6d36a76522457eeda8	Group Stage	2025-06-27 1:00:00	Salzburg	Real Madrid	Group H	67b8be4565db8d4ef5b05da6	67b8be4b65db8d4ef5b05e95	3"""

# --- Helper Functions ---

def get_canonical_team_name_robust(name_from_source: str, mapping: Dict[str, str]) -> str:
    """Gets the canonical team name using the provided mapping. Enhanced for robustness."""
    name_from_source_stripped = name_from_source.strip()
    if not name_from_source_stripped:
        return "N/A_EmptyName"

    # Direct match
    if name_from_source_stripped in mapping:
        return mapping[name_from_source_stripped]
    if name_from_source in mapping: # try original if stripped didn't match
        return mapping[name_from_source]

    # Check if it's already a canonical name (value in mapping)
    if name_from_source_stripped in mapping.values():
        return name_from_source_stripped
    if name_from_source in mapping.values():
        return name_from_source

    # Case-insensitive match
    name_lower = name_from_source_stripped.lower()
    for map_key, canonical_val in mapping.items():
        if name_lower == map_key.lower():
            return canonical_val

    # Normalized comparison (remove common suffixes and punctuation)
    suffixes_to_remove = [" fc", " cf", " rj", " fr", " hd", " sc", " ac", " c.f.", " c. f.", " c f", " de ", " e "] # Note leading space
    punctuation_to_remove = ".()-&" # Added hyphen and ampersand

    temp_name_norm = name_lower
    for suffix in suffixes_to_remove:
        temp_name_norm = temp_name_norm.replace(suffix, "")
    for punc in punctuation_to_remove:
        temp_name_norm = temp_name_norm.replace(punc, "")
    temp_name_norm = temp_name_norm.strip().replace(" ", "") # Also remove internal spaces for comparison

    for map_key, canonical_val in mapping.items():
        map_key_norm = map_key.lower()
        for suffix in suffixes_to_remove:
            map_key_norm = map_key_norm.replace(suffix, "")
        for punc in punctuation_to_remove:
            map_key_norm = map_key_norm.replace(punc, "")
        map_key_norm = map_key_norm.strip().replace(" ", "")

        if temp_name_norm == map_key_norm:
            return canonical_val

        # Also check against normalized canonical value
        canonical_val_norm = canonical_val.lower()
        for suffix in suffixes_to_remove:
            canonical_val_norm = canonical_val_norm.replace(suffix, "")
        for punc in punctuation_to_remove:
            canonical_val_norm = canonical_val_norm.replace(punc, "")
        canonical_val_norm = canonical_val_norm.strip().replace(" ", "")
        if temp_name_norm == canonical_val_norm:
            return canonical_val

    # Fallback: if user provided TEAM_DETAILS and name_from_source matches a key there
    if name_from_source_stripped in TEAM_DETAILS:
        # This implies name_from_source_stripped is already canonical if it's a key in TEAM_DETAILS
        # Let's ensure it's also in TEAM_NAME_MAPPING pointing to itself
        if name_from_source_stripped not in mapping or mapping[name_from_source_stripped] != name_from_source_stripped:
             print(f"Warning: Team name '{name_from_source_stripped}' is a TEAM_DETAILS key but missing self-map in TEAM_NAME_MAPPING. Adding.")
             mapping[name_from_source_stripped] = name_from_source_stripped # Auto-correct mapping
        return name_from_source_stripped


    print(f"Warning (Canonical Name): Team name '{name_from_source}' (normalized: '{temp_name_norm}') not reliably mapped, using '{name_from_source_stripped}'.")
    return name_from_source_stripped


def _populate_fixture_id_gw_lookup(raw_data_string: str, team_mapping: Dict[str, str]):
    """Parses raw tab-separated fixture data string and populates FIXTURE_ID_GW_LOOKUP."""
    global FIXTURE_ID_GW_LOOKUP
    FIXTURE_ID_GW_LOOKUP = {} # Reset
    data_io = io.StringIO(raw_data_string)
    reader = csv.reader(data_io, delimiter='\t')
    try:
        header = next(reader) # Skips the header row
    except StopIteration:
        print("Error: Fixture ID/GW data string is empty or header is missing.")
        return

    # Expected header: fixture_id	stage_name	starting_at	home_team_name	away_team_name	group_name	home_team_id	away_team_id	GW
    col_indices = {name: i for i, name in enumerate(header)}

    required_cols = ['fixture_id', 'home_team_name', 'away_team_name', 'starting_at', 'GW']
    if not all(col in col_indices for col in required_cols):
        print(f"Error: Missing one or more required columns in fixture data header for ID/GW lookup. Expected: {required_cols}")
        return

    for i, row in enumerate(reader):
        if len(row) < len(header): # Ensure enough columns as per header
            print(f"Skipping malformed fixture row {i+2} for ID/GW lookup (length mismatch): {row}")
            continue

        try:
            fixture_id_fixture = row[col_indices['fixture_id']].strip()
            home_team_original = row[col_indices['home_team_name']].strip()
            away_team_original = row[col_indices['away_team_name']].strip()
            starting_at_str = row[col_indices['starting_at']].strip()
            gw_fixture = row[col_indices['GW']].strip()

            if not all([fixture_id_fixture, home_team_original, away_team_original, starting_at_str, gw_fixture]):
                print(f"Skipping row {i+2} due to missing essential data for ID/GW lookup: {row}")
                continue

            # Extract date part (YYYY-MM-DD) from "YYYY-MM-DD HH:MM:SS"
            fixture_date_str = starting_at_str.split(" ")[0]
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", fixture_date_str):
                print(f"Skipping row {i+2} due to invalid date format in 'starting_at': {starting_at_str}")
                continue

            canonical_home = get_canonical_team_name_robust(home_team_original, team_mapping)
            canonical_away = get_canonical_team_name_robust(away_team_original, team_mapping)

            if "N/A" in canonical_home or "N/A" in canonical_away:
                 print(f"Warning (Fixture ID/GW Populate): Could not map teams for row {i+2}: {home_team_original} vs {away_team_original}. Skipping.")
                 continue

            lookup_key = (canonical_home, canonical_away, fixture_date_str)
            if lookup_key in FIXTURE_ID_GW_LOOKUP:
                print(f"Warning: Duplicate key {lookup_key} in FIXTURE_ID_GW_LOOKUP. Overwriting with fixture_id {fixture_id_fixture}.")

            FIXTURE_ID_GW_LOOKUP[lookup_key] = {
                "fixture_id": fixture_id_fixture,
                "GW": gw_fixture
            }
        except IndexError:
            print(f"Skipping malformed fixture row {i+2} for ID/GW lookup (index error): {row}")
            continue
        except Exception as e:
            print(f"Error processing fixture row {i+2} for ID/GW lookup: {row} - {e}")
            continue

    print(f"INFO: Fixture ID/GW lookup populated with {len(FIXTURE_ID_GW_LOOKUP)} entries.")

# Populate the lookup at script start
_populate_fixture_id_gw_lookup(FULL_FIXTURE_DATA_RAW, TEAM_NAME_MAPPING)


def parse_html_for_odds(file_path):
    teams_data = []
    if not os.path.exists(file_path):
        print(f"Info (HTML Outright): File not found: {file_path}")
        return teams_data
    try:
        with open(file_path, 'r', encoding='utf-8') as f: html_content = f.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        for row in soup.select('div[data-testid="outrights-table-row"]'):
            team_name_el = row.select_one('div[data-testid="outrights-participant-name"] p')
            odds_el = row.select_one('div[data-testid="add-to-coupon-button"] p')
            if team_name_el and odds_el:
                team_name, odds_text = team_name_el.get_text(strip=True), odds_el.get_text(strip=True)
                try:
                    if odds_text.startswith('+'): odds_val = float(odds_text[1:]) / 100 + 1
                    elif odds_text.startswith('-'): odds_val = 100 / float(odds_text[1:]) + 1
                    else: odds_val = float(odds_text)
                    teams_data.append({'raw_team_name': team_name, 'decimal_odds': odds_val})
                except ValueError: print(f"Warning (HTML Outright): Invalid odds '{odds_text}' for '{team_name}'.")
    except Exception as e: print(f"Error parsing HTML outright odds {file_path}: {e}")
    return teams_data

def parse_markdown_for_odds(file_path):
    teams_data = []
    if not os.path.exists(file_path):
        print(f"Info (MD Outright): File not found: {file_path}")
        return teams_data
    try:
        with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
        pattern = re.compile(r"!\[(?:.*?)\]\(https?://.*?\)\s*\n*\s*(.*?)\s*\n*\s*(?:\d+)\s*\n*\s*([+-]\d+)", re.MULTILINE) # Adjusted regex
        for raw_name, odds_text in pattern.findall(content):
            try:
                if odds_text.strip().startswith('+'):
                    odds_val = float(odds_text.strip()[1:]) / 100 + 1
                    teams_data.append({'raw_team_name': raw_name.strip(), 'decimal_odds': odds_val})
                elif odds_text.strip().startswith('-'):
                    odds_val = 100 / float(odds_text.strip()[1:]) + 1
                    teams_data.append({'raw_team_name': raw_name.strip(), 'decimal_odds': odds_val})
                else: print(f"Warning (MD Outright): Odds '{odds_text}' for '{raw_name.strip()}' not recognized (must start with + or -).")
            except ValueError: print(f"Warning (MD Outright): Invalid odds '{odds_text}' for '{raw_name.strip()}'.")
    except Exception as e: print(f"Error parsing Markdown outright odds {file_path}: {e}")
    return teams_data

def get_tournament_outright_odds_data(html_fp, md_fp, team_map):
    raw_data = parse_html_for_odds(html_fp)
    source = "HTML"
    if not raw_data:
        print("Info: HTML outright odds failed. Trying Markdown...")
        raw_data = parse_markdown_for_odds(md_fp)
        source = "Markdown"
    if not raw_data:
        print("Warning: No outright odds from HTML or Markdown.")
        return pd.DataFrame()

    processed_odds = []
    seen_canonical_names = set()
    for item in raw_data:
        raw_name, dec_odds = item['raw_team_name'], item['decimal_odds']
        # Use robust mapping for names from odds sources
        canonical_name = get_canonical_team_name_robust(raw_name, team_map)
        if canonical_name.startswith("N/A_") or canonical_name == raw_name and raw_name not in team_map.values():
             print(f"Warning (Outright Map): Raw name '{raw_name}' (from {source}) mapped to '{canonical_name}', might be an issue.")

        if canonical_name in seen_canonical_names: continue
        seen_canonical_names.add(canonical_name)
        if dec_odds <= 1.0:
            print(f"Warning (Outright Odds): Invalid odds {dec_odds} for {raw_name} (canonical: {canonical_name}).")
            continue
        processed_odds.append({'team_name_canonical': canonical_name, 'implied_prob': 1 / dec_odds, 'raw_parsed_name': raw_name, 'parser_source': source})
    return pd.DataFrame(processed_odds) if processed_odds else pd.DataFrame()


def normalize_tournament_implied_probs(df_odds, all_fixture_teams_canonical):
    default_strength, strength_scores = 10.0, {}
    if df_odds.empty or 'implied_prob' not in df_odds.columns:
        print("Warning: Outright odds DataFrame for strengths is empty. Assigning default strength.")
        for team in all_fixture_teams_canonical: strength_scores[team] = default_strength
        return strength_scores

    df_valid = df_odds[df_odds['implied_prob'] > 0].copy()
    if df_valid.empty:
        print("Warning: No valid outright probabilities for strengths. Assigning default strength.")
        for team in all_fixture_teams_canonical: strength_scores[team] = default_strength
        return strength_scores

    total_implied_prob = df_valid['implied_prob'].sum()
    df_valid['norm_prob'] = df_valid['implied_prob'] / total_implied_prob if total_implied_prob else 0
    max_norm_prob = df_valid['norm_prob'].max()
    df_valid['strength_metric'] = (df_valid['norm_prob'] / max_norm_prob) * 90 + 10 if max_norm_prob > 0 else default_strength
    strength_scores = pd.Series(df_valid.strength_metric.values, index=df_valid.team_name_canonical).to_dict()

    for team_c in all_fixture_teams_canonical:
        if team_c not in strength_scores:
            print(f"Warning (Strength): Team '{team_c}' not in outright odds. Default strength {default_strength} assigned.")
            strength_scores[team_c] = default_strength
    return strength_scores

def get_venue_impact(home_team_canonical, away_team_canonical, stadium):
    HOME_VENUES = {"Hard Rock Stadium, Miami Gardens, FL": "Inter Miami CF", "Lumen Field, Seattle, WA": "Seattle Sounders FC"}
    venue_team_canonical = None
    for venue_stadium, venue_home_team in HOME_VENUES.items():
        if stadium.strip().lower() == venue_stadium.strip().lower():
            venue_team_canonical = get_canonical_team_name_robust(venue_home_team, TEAM_NAME_MAPPING) # Map venue team too
            break

    if venue_team_canonical == home_team_canonical: return -12, 8
    if venue_team_canonical == away_team_canonical: return 8, -12
    return 0, 0

def calculate_fatigue_impact(team_canonical, match_date_obj, last_match_info, cross_country_travel=False):
    if not last_match_info or not last_match_info.get('date'): return 0
    rest_days = (match_date_obj - last_match_info['date']).days
    if rest_days < 0 :
        print(f"Warning (Fatigue): Negative rest days for {team_canonical}. Max fatigue assigned.")
        return 15
    if rest_days >= 7: fatigue = -10
    elif rest_days >= 5: fatigue = -5
    elif rest_days >= 3: fatigue = 0
    elif rest_days == 2: fatigue = 8
    else: fatigue = 15
    return fatigue + 5 if cross_country_travel else fatigue

def calculate_outright_fdr_components(fixture, team_strengths, match_history_context):
    home_c, away_c, date_dt, stadium = fixture['home_team_canonical'], fixture['away_team_canonical'], fixture['date_dt'], fixture['stadium']
    home_strength, away_strength = team_strengths.get(home_c, 10.0), team_strengths.get(away_c, 10.0)
    h_base_fdr_component, a_base_fdr_component = away_strength, home_strength
    ven_h_impact, ven_a_impact = get_venue_impact(home_c, away_c, stadium)

    east_coasts = ["Hard Rock Stadium, Miami Gardens, FL", "MetLife Stadium, East Rutherford, NJ", "Lincoln Financial Field, Philadelphia, PA", "GEODIS Park, Nashville, TN", "Bank of America Stadium, Charlotte, NC", "Mercedes-Benz Stadium, Atlanta, GA", "Inter&Co Stadium, Orlando, FL", "Audi Field, Washington, D.C.", "Camping World Stadium, Orlando, FL", "TQL Stadium, Cincinnati, OH"]
    west_coasts = ["Lumen Field, Seattle, WA", "Rose Bowl Stadium, Pasadena, CA"]

    east_coasts_lower = [s.lower().strip() for s in east_coasts]
    west_coasts_lower = [s.lower().strip() for s in west_coasts]
    current_stadium_lower = stadium.lower().strip()

    cc_h, cc_a = False, False
    last_h_info, last_a_info = match_history_context.get(home_c), match_history_context.get(away_c)
    if last_h_info and last_h_info.get('venue'):
        last_h_venue_lower = last_h_info['venue'].lower().strip()
        cc_h = (current_stadium_lower in east_coasts_lower and last_h_venue_lower in west_coasts_lower) or \
               (current_stadium_lower in west_coasts_lower and last_h_venue_lower in east_coasts_lower)
    if last_a_info and last_a_info.get('venue'):
        last_a_venue_lower = last_a_info['venue'].lower().strip()
        cc_a = (current_stadium_lower in east_coasts_lower and last_a_venue_lower in west_coasts_lower) or \
               (current_stadium_lower in west_coasts_lower and last_a_venue_lower in east_coasts_lower)

    fat_h_impact = calculate_fatigue_impact(home_c, date_dt, last_h_info, cc_h)
    fat_a_impact = calculate_fatigue_impact(away_c, date_dt, last_a_info, cc_a)

    h_fdr_out = (OUTRIGHT_COMPONENT_WEIGHTS['base_strength_from_odds'] * h_base_fdr_component +
                 OUTRIGHT_COMPONENT_WEIGHTS['venue_impact'] * ven_h_impact +
                 OUTRIGHT_COMPONENT_WEIGHTS['fatigue'] * fat_h_impact)
    a_fdr_out = (OUTRIGHT_COMPONENT_WEIGHTS['base_strength_from_odds'] * a_base_fdr_component +
                 OUTRIGHT_COMPONENT_WEIGHTS['venue_impact'] * ven_a_impact +
                 OUTRIGHT_COMPONENT_WEIGHTS['fatigue'] * fat_a_impact)
    h_fdr_out_scaled = np.clip(h_fdr_out / 1.5 + 25, 1, 99)
    a_fdr_out_scaled = np.clip(a_fdr_out / 1.5 + 25, 1, 99)
    return {'home_fdr_outright': h_fdr_out_scaled, 'away_fdr_outright': a_fdr_out_scaled,
            'home_strength_metric': home_strength, 'away_strength_metric': away_strength,
            'venue_impact_home': ven_h_impact, 'venue_impact_away': ven_a_impact,
            'fatigue_impact_home': fat_h_impact, 'fatigue_impact_away': fat_a_impact}

def create_base_fixtures_with_canonical_names(team_map: Dict[str, str], fixture_id_gw_provider: Dict[Tuple[str,str,str], Dict[str,str]]):
    # This list contains stadium, specific time formatting needed for existing logic
    user_provided_fixtures_raw = [
        {'home_team': 'Al Ahly FC', 'away_team': 'Inter Miami CF', 'date': '2025-06-15', 'time': '12:00 AM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'A'}, # Adjusted time from 0:00:00
        {'home_team': 'SE Palmeiras', 'away_team': 'FC Porto', 'date': '2025-06-15', 'time': '10:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'A'}, # Adjusted from 22:00
        {'home_team': 'Paris Saint-Germain', 'away_team': 'Atlético de Madrid', 'date': '2025-06-15', 'time': '07:00 PM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'B'}, # Adjusted from 19:00
        {'home_team': 'Botafogo FR', 'away_team': 'Seattle Sounders FC', 'date': '2025-06-16', 'time': '02:00 AM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'B'}, # Date is 16th, time 2 AM
        {'home_team': 'FC Bayern München', 'away_team': 'Auckland City FC', 'date': '2025-06-15', 'time': '04:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'C'}, # Adjusted from 16:00
        {'home_team': 'CA Boca Juniors', 'away_team': 'SL Benfica', 'date': '2025-06-16', 'time': '10:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'C'}, # Adjusted from 22:00
        {'home_team': 'CR Flamengo', 'away_team': 'Espérance Sportive de Tunis', 'date': '2025-06-17', 'time': '01:00 AM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'D'}, # Date is 17th, time 1 AM
        {'home_team': 'Chelsea FC', 'away_team': 'LAFC', 'date': '2025-06-16', 'time': '07:00 PM', 'stadium': 'Mercedes-Benz Stadium, Atlanta, GA', 'group': 'D'}, # Adjusted from 19:00
        {'home_team': 'CA River Plate', 'away_team': 'Urawa Red Diamonds', 'date': '2025-06-17', 'time': '07:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'E'}, # Adjusted from 19:00
        {'home_team': 'CF Monterrey', 'away_team': 'FC Internazionale Milano', 'date': '2025-06-18', 'time': '01:00 AM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'E'}, # Date is 18th, time 1 AM
        {'home_team': 'Fluminense FC', 'away_team': 'Borussia Dortmund', 'date': '2025-06-17', 'time': '04:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'F'}, # Adjusted from 16:00
        {'home_team': 'Ulsan HD FC', 'away_team': 'Mamelodi Sundowns FC', 'date': '2025-06-17', 'time': '10:00 PM', 'stadium': 'Inter&Co Stadium, Orlando, FL', 'group': 'F'}, # Adjusted from 22:00
        {'home_team': 'Manchester City FC', 'away_team': 'Wydad AC', 'date': '2025-06-18', 'time': '04:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'G'}, # Adjusted from 16:00
        {'home_team': 'Al Ain FC', 'away_team': 'Juventus FC', 'date': '2025-06-19', 'time': '01:00 AM', 'stadium': 'Audi Field, Washington, D.C.', 'group': 'G'}, # Date is 19th, time 1 AM
        {'home_team': 'Real Madrid CF', 'away_team': 'Al Hilal SFC', 'date': '2025-06-18', 'time': '07:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'H'}, # Adjusted from 19:00
        {'home_team': 'CF Pachuca', 'away_team': 'FC Salzburg', 'date': '2025-06-18', 'time': '10:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'H'}, # Adjusted from 22:00
        # GW2 STARTS
        {'home_team': 'SE Palmeiras', 'away_team': 'Al Ahly FC', 'date': '2025-06-19', 'time': '04:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'A'},
        {'home_team': 'Inter Miami CF', 'away_team': 'FC Porto', 'date': '2025-06-19', 'time': '07:00 PM', 'stadium': 'Mercedes-Benz Stadium, Atlanta, GA', 'group': 'A'},
        {'home_team': 'Paris Saint-Germain', 'away_team': 'Botafogo FR', 'date': '2025-06-20', 'time': '01:00 AM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'B'},
        {'home_team': 'Seattle Sounders FC', 'away_team': 'Atlético de Madrid', 'date': '2025-06-19', 'time': '10:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'B'},
        {'home_team': 'FC Bayern München', 'away_team': 'CA Boca Juniors', 'date': '2025-06-21', 'time': '01:00 AM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'C'},
        {'home_team': 'SL Benfica', 'away_team': 'Auckland City FC', 'date': '2025-06-20', 'time': '04:00 PM', 'stadium': 'Inter&Co Stadium, Orlando, FL', 'group': 'C'},
        {'home_team': 'CR Flamengo', 'away_team': 'Chelsea FC', 'date': '2025-06-20', 'time': '06:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'D'}, # Adjusted from 18:00
        {'home_team': 'LAFC', 'away_team': 'Espérance Sportive de Tunis', 'date': '2025-06-20', 'time': '10:00 PM', 'stadium': 'GEODIS Park, Nashville, TN', 'group': 'D'},
        {'home_team': 'CA River Plate', 'away_team': 'CF Monterrey', 'date': '2025-06-22', 'time': '01:00 AM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'E'},
        {'home_team': 'FC Internazionale Milano', 'away_team': 'Urawa Red Diamonds', 'date': '2025-06-21', 'time': '07:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'E'},
        {'home_team': 'Fluminense FC', 'away_team': 'Ulsan HD FC', 'date': '2025-06-21', 'time': '10:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'F'},
        {'home_team': 'Mamelodi Sundowns FC', 'away_team': 'Borussia Dortmund', 'date': '2025-06-21', 'time': '04:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'F'},
        {'home_team': 'Manchester City FC', 'away_team': 'Al Ain FC', 'date': '2025-06-23', 'time': '01:00 AM', 'stadium': 'Mercedes-Benz Stadium, Atlanta, GA', 'group': 'G'},
        {'home_team': 'Juventus FC', 'away_team': 'Wydad AC', 'date': '2025-06-22', 'time': '04:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'G'},
        {'home_team': 'Real Madrid CF', 'away_team': 'CF Pachuca', 'date': '2025-06-22', 'time': '07:00 PM', 'stadium': 'Bank of America Stadium, Charlotte, NC', 'group': 'H'},
        {'home_team': 'FC Salzburg', 'away_team': 'Al Hilal SFC', 'date': '2025-06-22', 'time': '10:00 PM', 'stadium': 'Audi Field, Washington, D.C.', 'group': 'H'},
        # GW3 STARTS
        {'home_team': 'FC Porto', 'away_team': 'Al Ahly FC', 'date': '2025-06-24', 'time': '01:00 AM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'A'},
        {'home_team': 'Inter Miami CF', 'away_team': 'SE Palmeiras', 'date': '2025-06-24', 'time': '01:00 AM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'A'},
        {'home_team': 'Atlético de Madrid', 'away_team': 'Botafogo FR', 'date': '2025-06-23', 'time': '07:00 PM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'B'},
        {'home_team': 'Seattle Sounders FC', 'away_team': 'Paris Saint-Germain', 'date': '2025-06-23', 'time': '07:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'B'},
        {'home_team': 'Auckland City FC', 'away_team': 'CA Boca Juniors', 'date': '2025-06-24', 'time': '07:00 PM', 'stadium': 'GEODIS Park, Nashville, TN', 'group': 'C'}, # Adjusted from 19:00 (PM)
        {'home_team': 'SL Benfica', 'away_team': 'FC Bayern München', 'date': '2025-06-24', 'time': '07:00 PM', 'stadium': 'Bank of America Stadium, Charlotte, NC', 'group': 'C'}, # Adjusted from 19:00 (PM)
        {'home_team': 'Espérance Sportive de Tunis', 'away_team': 'Chelsea FC', 'date': '2025-06-25', 'time': '01:00 AM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'D'},
        {'home_team': 'LAFC', 'away_team': 'CR Flamengo', 'date': '2025-06-25', 'time': '01:00 AM', 'stadium': 'Camping World Stadium, Orlando, FL', 'group': 'D'},
        {'home_team': 'Urawa Red Diamonds', 'away_team': 'CF Monterrey', 'date': '2025-06-26', 'time': '01:00 AM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'E'},
        {'home_team': 'FC Internazionale Milano', 'away_team': 'CA River Plate', 'date': '2025-06-26', 'time': '01:00 AM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'E'},
        {'home_team': 'Borussia Dortmund', 'away_team': 'Ulsan HD FC', 'date': '2025-06-25', 'time': '07:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'F'}, # Adjusted from 19:00 (PM)
        {'home_team': 'Mamelodi Sundowns FC', 'away_team': 'Fluminense FC', 'date': '2025-06-25', 'time': '07:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'F'}, # Adjusted from 19:00 (PM)
        {'home_team': 'Wydad AC', 'away_team': 'Al Ain FC', 'date': '2025-06-26', 'time': '07:00 PM', 'stadium': 'Audi Field, Washington, D.C.', 'group': 'G'}, # Adjusted from 19:00 (PM)
        {'home_team': 'Juventus FC', 'away_team': 'Manchester City FC', 'date': '2025-06-26', 'time': '07:00 PM', 'stadium': 'Camping World Stadium, Orlando, FL', 'group': 'G'}, # Adjusted from 19:00 (PM)
        {'home_team': 'Al Hilal SFC', 'away_team': 'CF Pachuca', 'date': '2025-06-27', 'time': '01:00 AM', 'stadium': 'GEODIS Park, Nashville, TN', 'group': 'H'},
        {'home_team': 'FC Salzburg', 'away_team': 'Real Madrid CF', 'date': '2025-06-27', 'time': '01:00 AM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'H'}
    ]
    # IMPORTANT: The times in user_provided_fixtures_raw were manually adjusted to be standard AM/PM.
    # The FULL_FIXTURE_DATA_RAW uses 24-hour format. Ensure your datetime parsing can handle the user_provided_fixtures_raw times.
    # The date ('YYYY-MM-DD') is used for lookup.

    processed_fixtures = []
    for fix_data in user_provided_fixtures_raw:
        home_raw, away_raw = str(fix_data['home_team']).strip(), str(fix_data['away_team']).strip()

        # Use robust mapping for teams in user_provided_fixtures_raw
        home_c = get_canonical_team_name_robust(home_raw, team_map)
        away_c = get_canonical_team_name_robust(away_raw, team_map)

        if home_c.startswith("N/A_") or home_raw not in team_map and home_c == home_raw and home_c not in TEAM_DETAILS:
            print(f"Warning (Fixture Map Base): Raw home team '{home_raw}' (mapped to {home_c}) may need attention in TEAM_NAME_MAPPING.")
        if away_c.startswith("N/A_") or away_raw not in team_map and away_c == away_raw and away_c not in TEAM_DETAILS:
            print(f"Warning (Fixture Map Base): Raw away team '{away_raw}' (mapped to {away_c}) may need attention in TEAM_NAME_MAPPING.")

        date_s, time_s = fix_data['date'], fix_data['time']
        if not home_c or not away_c or home_c == away_c:
            print(f"Warning: Skipping fixture due to mapping issue or same teams: {fix_data}"); continue

        try:
            # Parse date and time from user_provided_fixtures_raw
            # Ensure the time format here ('%I:%M %p') matches your user_provided_fixtures_raw
            date_obj_only = datetime.strptime(date_s, '%Y-%m-%d')
            dt_obj = datetime.strptime(f"{date_s} {time_s}", '%Y-%m-%d %I:%M %p')

            # Lookup fixture_id and GW using canonical names and date_s (YYYY-MM-DD)
            # The key for fixture_id_gw_provider is (canonical_home, canonical_away, date_str YYYY-MM-DD)
            # from the FULL_FIXTURE_DATA_RAW source.
            # We assume user_provided_fixtures_raw home/away order is the one we want to match against.
            fixture_extra_info = fixture_id_gw_provider.get(
                (home_c, away_c, date_s), # Key: (home_canonical, away_canonical, YYYY-MM-DD)
                {"fixture_id": f"NO_ID_FOR_{home_c}_vs_{away_c}_{date_s}", "GW": "N/A_GW"} # Fallback
            )

            # If not found, try reversing home/away for the lookup key, as FULL_FIXTURE_DATA_RAW might have a different order
            # for some matches than user_provided_fixtures_raw (though ideally they should align for "home" vs "away")
            if fixture_extra_info["fixture_id"].startswith("NO_ID_FOR_"):
                reversed_lookup = fixture_id_gw_provider.get(
                    (away_c, home_c, date_s),
                    None
                )
                if reversed_lookup:
                    print(f"Info: Found fixture_id/GW for {home_c} vs {away_c} on {date_s} by reversing team order in lookup.")
                    fixture_extra_info = reversed_lookup


            processed_fixtures.append({
                'home_team_canonical': home_c, 'away_team_canonical': away_c,
                'date_str': date_s, 'time_str': time_s,
                'stadium': fix_data['stadium'], 'group': fix_data['group'],
                'date_dt': date_obj_only, 'datetime_obj': dt_obj,
                'fixture_id': fixture_extra_info['fixture_id'], # Added
                'GW': fixture_extra_info['GW']                  # Added
            })
        except ValueError as e:
            print(f"Error parsing fixture date/time {fix_data}: {e}. Time provided: '{time_s}'. Skipping.")
            print(f"Ensure date is YYYY-MM-DD and time is HH:MM AM/PM (e.g., 08:00 PM or 12:00 AM).")

    all_unique_fixtures_sorted = sorted(processed_fixtures, key=lambda x: x['datetime_obj'])
    final_fixtures_list, final_unique_keys = [], set()
    for fix in all_unique_fixtures_sorted:
        # Using fixture_id for uniqueness check now
        key = fix['fixture_id']
        if key not in final_unique_keys or key.startswith("NO_ID_FOR_"): # If fallback ID, allow potential duplicates by old key
            if key.startswith("NO_ID_FOR_"): # For fallback IDs, use old uniqueness
                old_key_for_no_id = (fix['home_team_canonical'], fix['away_team_canonical'], fix['date_str'])
                if old_key_for_no_id not in final_unique_keys:
                    final_fixtures_list.append(fix)
                    final_unique_keys.add(old_key_for_no_id) # Add old key for this specific case
            else: # Proper fixture_id found
                final_fixtures_list.append(fix)
                final_unique_keys.add(key)

    print(f"Created {len(final_fixtures_list)} unique base fixtures with fixture_id and GW.")
    if any(f['fixture_id'].startswith("NO_ID_FOR_") for f in final_fixtures_list):
        print("Warning: Some fixtures could not be matched to a fixture_id from the provided data source.")
    return final_fixtures_list


def create_last_match_dates_history(sorted_fixtures_canonical):
    history_list_for_each_match, team_last_match_tracker = [], { tc: None for fix in sorted_fixtures_canonical for tc in (fix['home_team_canonical'], fix['away_team_canonical'])}
    for fix in sorted_fixtures_canonical:
        home_c, away_c = fix['home_team_canonical'], fix['away_team_canonical']
        history_list_for_each_match.append({home_c: team_last_match_tracker[home_c].copy() if team_last_match_tracker[home_c] else None, away_c: team_last_match_tracker[away_c].copy() if team_last_match_tracker[away_c] else None})
        team_last_match_tracker[home_c], team_last_match_tracker[away_c] = {'date': fix['date_dt'], 'venue': fix['stadium']}, {'date': fix['date_dt'], 'venue': fix['stadium']}
    return history_list_for_each_match

def parse_cs_match_string_for_canonical_teams(match_str, team_map):
    separators = [' vs ', ' - ', ' @ ']
    parts = None
    for sep in separators:
        if sep in match_str: parts = match_str.split(sep, 1); break
    if not parts:
        parts_re = re.split(r'\s{2,}', match_str, 1)
        if len(parts_re) < 2:
            mid = len(match_str) // 2
            space_after, space_before = match_str.find(' ', mid), match_str.rfind(' ', 0, mid)
            idx = space_after if space_after != -1 and (space_before == -1 or abs(mid - space_after) < abs(mid - space_before)) else (space_before if space_before != -1 else (mid if len(match_str) > 10 else -1))
            parts = [match_str[:idx].strip(), match_str[idx+1:].strip()] if idx > 0 and idx < len(match_str)-1 else [match_str.strip(), "Unknown"]
        else: parts = parts_re

    h_raw, a_raw = (parts[0].strip() if len(parts) > 0 else "Unknown"), (parts[1].strip() if len(parts) > 1 else "Unknown")
    # Use robust mapping for teams from correct score JSON
    h_canonical = get_canonical_team_name_robust(h_raw, team_map)
    a_canonical = get_canonical_team_name_robust(a_raw, team_map)

    if h_canonical.startswith("N/A_") or (h_raw != "Unknown" and h_canonical == h_raw and h_raw not in team_map.values()): print(f"Warning (CS Map): Home team '{h_raw}' -> '{h_canonical}' from CS JSON needs mapping check.")
    if a_canonical.startswith("N/A_") or (a_raw != "Unknown" and a_canonical == a_raw and a_raw not in team_map.values()): print(f"Warning (CS Map): Away team '{a_raw}' -> '{a_canonical}' from CS JSON needs mapping check.")

    return (None, None) if "Unknown" in [h_canonical, a_canonical] or h_canonical.startswith("N/A_") or a_canonical.startswith("N/A_") else (h_canonical, a_canonical)


def load_correct_score_data_for_fdr(json_fp, team_map):
    cs_data_lookup = {}
    if not os.path.exists(json_fp):
        print(f"Info (CS Load): File not found: {json_fp}.")
        return cs_data_lookup
    try:
        with open(json_fp, 'r', encoding='utf-8') as f: data = json.load(f)
        if 'matches' not in data: print(f"Error: 'matches' key not in {json_fp}"); return {}

        loaded_count = 0
        for entry in data.get('matches', []):
            if not all(k in entry for k in ['match', 'date', 'correct_score_odds']): continue
            h_canonical, a_canonical = parse_cs_match_string_for_canonical_teams(entry['match'], team_map)

            if not h_canonical or not a_canonical:
                print(f"Warning (CS Load): Skip '{entry['match']}' due to team mapping issues."); continue
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", entry['date']):
                print(f"Warning (CS Load): Skip '{entry['match']}' due to invalid date format: {entry['date']}"); continue

            cs_data_lookup[(h_canonical, a_canonical, entry['date'])] = entry['correct_score_odds']
            loaded_count +=1
        print(f"✅ Loaded {loaded_count} matches from CS JSON: {json_fp}")
    except Exception as e: print(f"Error loading CS data from {json_fp}: {e}")
    return cs_data_lookup


def calculate_correct_score_fdr_values(cs_odds_dict):
    if not cs_odds_dict or not isinstance(cs_odds_dict, dict) or not cs_odds_dict: return 50.0, 50.0, 0.333, 0.334, 0.333
    total_inv_odds, valid_odds_parsed = 0.0, []
    for score_str, odd_val in cs_odds_dict.items():
        try:
            odd_f = float(odd_val)
            if odd_f > 1.0:
                parts = str(score_str).split('-')
                if len(parts) == 2:
                    valid_odds_parsed.append({'h_g': int(parts[0]), 'a_g': int(parts[1]), 'inv_odd': 1.0 / odd_f})
                    total_inv_odds += (1.0 / odd_f)
        except: continue
    if total_inv_odds <= 1e-6 or not valid_odds_parsed: return 50.0, 50.0, 0.333, 0.334, 0.333
    P_h, P_d, P_a = 0.0, 0.0, 0.0
    for item in valid_odds_parsed:
        prob_norm = item['inv_odd'] / total_inv_odds
        if item['h_g'] > item['a_g']: P_h += prob_norm
        elif item['h_g'] < item['a_g']: P_a += prob_norm
        else: P_d += prob_norm
    sum_probs = P_h + P_d + P_a
    if sum_probs > 1e-6: P_h /= sum_probs; P_d /= sum_probs; P_a /= sum_probs
    else: P_h, P_d, P_a = 0.333, 0.334, 0.333
    h_exp_pts, a_exp_pts = 3.0 * P_h + 1.0 * P_d, 3.0 * P_a + 1.0 * P_d
    h_fdr_cs, a_fdr_cs = np.clip(100.0 - (h_exp_pts / 3.0) * 100.0, 1, 99), np.clip(100.0 - (a_exp_pts / 3.0) * 100.0, 1, 99)
    return h_fdr_cs, a_fdr_cs, P_h, P_d, P_a

def calculate_match_afd_dfd_from_cs_odds(cs_odds_dict):
    if not cs_odds_dict or not isinstance(cs_odds_dict, dict) or not cs_odds_dict: return None, None, None, None
    total_inv_odds, valid_odds_parsed = 0.0, []
    for score_str, odd_val in cs_odds_dict.items():
        try:
            odd_f = float(odd_val)
            if odd_f > 1.0:
                parts = str(score_str).split('-')
                if len(parts) == 2:
                    valid_odds_parsed.append({'h_g': int(parts[0]), 'a_g': int(parts[1]), 'inv_odd': 1.0 / odd_f})
                    total_inv_odds += (1.0 / odd_f)
        except: continue
    if total_inv_odds <= 1e-6 or not valid_odds_parsed: return None, None, None, None
    xG_h, xG_a = 0.0, 0.0
    for item in valid_odds_parsed:
        prob_norm = item['inv_odd'] / total_inv_odds
        xG_h += item['h_g'] * prob_norm; xG_a += item['a_g'] * prob_norm
    h_afd, h_dfd = (100.0 / xG_h) if xG_h > 0.01 else 999.0, xG_a * 100.0
    a_afd, a_dfd = (100.0 / xG_a) if xG_a > 0.01 else 999.0, xG_h * 100.0
    return round(h_afd,1), round(h_dfd,1), round(a_afd,1), round(a_dfd,1)

def determine_match_tiers_and_competitiveness(home_fdr, away_fdr, tier_map_display_dict):
    fdr_diff, lower_fdr_is_home = abs(home_fdr - away_fdr), home_fdr <= away_fdr
    if fdr_diff <= 6: r_low, r_high, comp_label = "Hard", "Hard", "Minimal"
    elif fdr_diff <= 15: r_low, r_high, comp_label = "Moderate", "Hard", "Medium"
    elif fdr_diff <= 20: r_low, r_high, comp_label = "Easy", "Hard", "Large"
    elif fdr_diff <= 30: r_low, r_high, comp_label = "Easy", "Very Hard", "Substantial"
    else: r_low, r_high, comp_label = "Very Easy", "Very Hard", "Massive"
    f_low, f_high = tier_map_display_dict.get(r_low, "Average Difficulty"), tier_map_display_dict.get(r_high, "Difficult")
    h_tier, a_tier = (f_low if lower_fdr_is_home else f_high), (f_high if lower_fdr_is_home else f_low)
    return h_tier, a_tier, round(fdr_diff, 1), comp_label

def get_player_position_category(position_str):
    pos_l = str(position_str).lower()
    if 'goalkeeper' in pos_l: return 'Goalkeeper'
    if 'defender' in pos_l or 'back' in pos_l: return 'Defender'
    if 'midfield' in pos_l: return 'Midfielder'
    if 'forward' in pos_l or 'striker' in pos_l or 'winger' in pos_l: return 'Forward'
    print(f"Warning: Unknown position '{position_str}', defaulted to Forward.")
    return 'Forward'

def calculate_player_points_for_specific_score(p_stats, team_goals, team_conceded, team_goals_season, team_assists_season):
    pos_cat = p_stats['PositionCategory']
    points = 2.0
    player_goals_season = p_stats['Goals']
    player_assists_season = p_stats['Assists']
    if team_goals_season > 0 and team_goals > 0:
        player_expected_goals_this_match = (player_goals_season / team_goals_season) * team_goals
    else: player_expected_goals_this_match = 0.0
    if team_assists_season > 0 and team_goals > 0:
        player_expected_assists_this_match = (player_assists_season / team_assists_season) * team_goals
        player_expected_assists_this_match = max(0, min(player_expected_assists_this_match, team_goals - player_expected_goals_this_match))
    else: player_expected_assists_this_match = 0.0
    goal_points_map = {'Goalkeeper': 10, 'Defender': 6, 'Midfielder': 5, 'Forward': 4}
    points += player_expected_goals_this_match * goal_points_map.get(pos_cat, 4)
    points += player_expected_assists_this_match * 3.0
    if team_conceded == 0:
        if pos_cat in ['Goalkeeper', 'Defender']: points += 4.0
        elif pos_cat == 'Midfielder': points += 1.0
    if pos_cat in ['Goalkeeper', 'Defender']: points -= (team_conceded // 2) * 1.0
    return points

def estimate_xg_from_fdr_outrights(h_fdr, a_fdr, avg_goals=AVERAGE_TOTAL_GOALS_IN_MATCH):
    if pd.isna(h_fdr) or pd.isna(a_fdr): return avg_goals / 2, avg_goals / 2
    h_proxy, a_proxy = 1/(h_fdr+0.1), 1/(a_fdr+0.1)
    total_proxy = h_proxy + a_proxy
    h_ratio = h_proxy / total_proxy if total_proxy > 1e-9 else 0.5
    return max(0.1, h_ratio*avg_goals), max(0.1, (1-h_ratio)*avg_goals)

def get_score_probabilities_poisson(xg_h, xg_a, max_g=MAX_POISSON_GOALS):
    probs, h_probs, a_probs = {}, [poisson.pmf(i, xg_h) for i in range(max_g+1)], [poisson.pmf(i, xg_a) for i in range(max_g+1)]
    for hg in range(max_g+1):
        for ag in range(max_g+1): probs[f"{hg}-{ag}"] = h_probs[hg] * a_probs[ag]
    total_p = sum(probs.values())
    return {s: p/total_p for s,p in probs.items()} if total_p > 1e-9 else {"0-0":1.0}

# --- Main Calculation Logic Function ---
def generate_all_player_points_data():
    print("--- Starting FIFA Club World Cup 2025 Analysis (Calculation Engine v2) ---")

    # --- FDR Calculations ---
    print("--- Calculating Fixture Difficulty Ratings (FDRs) ---")
    # create_base_fixtures now uses the global FIXTURE_ID_GW_LOOKUP
    all_base_fixtures = create_base_fixtures_with_canonical_names(TEAM_NAME_MAPPING, FIXTURE_ID_GW_LOOKUP)
    if not all_base_fixtures:
        print("CRITICAL: No base fixtures loaded in calculation engine.")
        return None, "No base fixtures loaded."

    all_involved_teams_canonical = set(t for fix in all_base_fixtures for t in (fix['home_team_canonical'], fix['away_team_canonical']))
    df_outright_odds_data = get_tournament_outright_odds_data(HTML_ODDS_FP, MD_ODDS_FP, TEAM_NAME_MAPPING)
    team_strength_metrics = normalize_tournament_implied_probs(df_outright_odds_data, all_involved_teams_canonical)
    match_history_contexts = create_last_match_dates_history(all_base_fixtures)
    cs_odds_lookup_for_fdr = load_correct_score_data_for_fdr(CS_JSON_FP, TEAM_NAME_MAPPING)

    fdr_results_list = []
    for i, fixture_details in enumerate(all_base_fixtures):
        history_ctx = match_history_contexts[i]
        home_c, away_c, date_s = fixture_details['home_team_canonical'], fixture_details['away_team_canonical'], fixture_details['date_str']

        # Get fixture_id and GW from fixture_details
        fixture_id_val = fixture_details.get('fixture_id', 'N/A_ID')
        gw_val = fixture_details.get('GW', 'N/A_GW')

        outright_calcs = calculate_outright_fdr_components(fixture_details, team_strength_metrics, history_ctx)
        h_fdr_out, a_fdr_out = outright_calcs['home_fdr_outright'], outright_calcs['away_fdr_outright']

        method, h_fdr_cs, a_fdr_cs, P_h, P_d, P_a, h_afd, h_dfd, a_afd, a_dfd = "OutrightFDR", None,None,None,None,None,None,None,None,None
        fixture_key_cs_order1 = (home_c, away_c, date_s); fixture_key_cs_order2 = (away_c, home_c, date_s)
        cs_match_odds = cs_odds_lookup_for_fdr.get(fixture_key_cs_order1) or cs_odds_lookup_for_fdr.get(fixture_key_cs_order2)

        if cs_match_odds:
            h_fdr_cs_calc,a_fdr_cs_calc,P_h_calc,P_d_calc,P_a_calc = calculate_correct_score_fdr_values(cs_match_odds)
            h_afd_calc,h_dfd_calc,a_afd_calc,a_dfd_calc = calculate_match_afd_dfd_from_cs_odds(cs_match_odds)
            if fixture_key_cs_order2 in cs_odds_lookup_for_fdr and fixture_key_cs_order1 not in cs_odds_lookup_for_fdr :
                 h_fdr_cs, a_fdr_cs, P_h, P_d, P_a = a_fdr_cs_calc, h_fdr_cs_calc, P_a_calc, P_d_calc, P_h_calc
                 h_afd, h_dfd, a_afd, a_dfd = a_afd_calc, a_dfd_calc, h_afd_calc, h_dfd_calc
            else:
                 h_fdr_cs, a_fdr_cs, P_h, P_d, P_a = h_fdr_cs_calc, a_fdr_cs_calc, P_h_calc, P_d_calc, P_a_calc
                 h_afd, h_dfd, a_afd, a_dfd = h_afd_calc, h_dfd_calc, a_afd_calc, a_dfd_calc
            final_h_fdr = FINAL_FDR_WEIGHTS['outright']*h_fdr_out + FINAL_FDR_WEIGHTS['correct_score']*h_fdr_cs
            final_a_fdr = FINAL_FDR_WEIGHTS['outright']*a_fdr_out + FINAL_FDR_WEIGHTS['correct_score']*a_fdr_cs
            method = "CombinedFDR"
        else: final_h_fdr, final_a_fdr = h_fdr_out, a_fdr_out

        final_h_fdr, final_a_fdr = np.clip(final_h_fdr,1,99), np.clip(final_a_fdr,1,99)
        h_tier, a_tier, fdr_diff, comp_label = determine_match_tiers_and_competitiveness(final_h_fdr, final_a_fdr, TIER_DISPLAY_MAPPING)
        home_details, away_details = TEAM_DETAILS.get(home_c,DEFAULT_TEAM_DETAIL), TEAM_DETAILS.get(away_c,DEFAULT_TEAM_DETAIL)
        home_team_api_id, away_team_api_id = home_details.get('api_id'), away_details.get('api_id')

        fdr_results_list.append({
            'fixture_id': fixture_id_val, # Added
            'GW': gw_val,                 # Added
            'date_str':date_s, 'time_str':fixture_details['time_str'], 'group':fixture_details['group'], 'stadium':fixture_details['stadium'],
            'home_team_canonical':home_c, 'home_team_short_code':home_details['short_code'], 'home_team_api_id':home_team_api_id, 'home_team_logo':home_details['image'],
            'away_team_canonical':away_c, 'away_team_short_code':away_details['short_code'], 'away_team_api_id':away_team_api_id, 'away_team_logo':away_details['image'],
            'final_home_fdr':round(final_h_fdr,1), 'final_away_fdr':round(final_a_fdr,1),
            'home_tier_display':h_tier, 'away_tier_display':a_tier, 'fdr_difference':fdr_diff, 'match_competitiveness_label':comp_label,
            'fdr_calculation_method':method, 'home_fdr_outright':round(h_fdr_out,1) if h_fdr_out is not None else None,
            'away_fdr_outright':round(a_fdr_out,1) if a_fdr_out is not None else None, 'home_fdr_cs':round(h_fdr_cs,1) if h_fdr_cs is not None else None,
            'away_fdr_cs':round(a_fdr_cs,1) if a_fdr_cs is not None else None, 'home_strength_metric':round(outright_calcs['home_strength_metric'],1),
            'away_strength_metric':round(outright_calcs['away_strength_metric'],1), 'venue_impact_home':outright_calcs['venue_impact_home'],
            'venue_impact_away':outright_calcs['venue_impact_away'], 'fatigue_impact_home':outright_calcs['fatigue_impact_home'],
            'fatigue_impact_away':outright_calcs['fatigue_impact_away'], 'prob_home_win_cs':P_h, 'prob_draw_cs':P_d, 'prob_away_win_cs':P_a,
            'home_afd_cs':h_afd, 'home_dfd_cs':h_dfd, 'away_afd_cs':a_afd, 'away_dfd_cs':a_dfd
        })
    fdr_final_df = pd.DataFrame(fdr_results_list)
    if fdr_final_df.empty:
        print("CRITICAL: No FDR results generated in calculation engine.")
        return None, "No FDR results generated."

    # --- Player Points Calculations ---
    print("--- Calculating Player Fantasy Points ---")
    try:
        player_df_raw = pd.read_excel(PLAYER_STATS_FP, sheet_name='Sheet1')
        print(f"Info: Columns found in '{PLAYER_STATS_FP}': {player_df_raw.columns.tolist()}")
        player_team_column_name = 'Team Name'
        if player_team_column_name not in player_df_raw.columns:
            player_team_column_name = 'Team'
            if player_team_column_name not in player_df_raw.columns:
                err_msg = f"CRITICAL: Excel file '{PLAYER_STATS_FP}' missing team column (tried 'Team Name' and 'Team')."
                print(err_msg); return None, err_msg

        print(f"Info: Using column '{player_team_column_name}' for player teams from '{PLAYER_STATS_FP}'.")
        player_df_raw['Team_Canonical'] = player_df_raw[player_team_column_name].apply(
            lambda x: get_canonical_team_name_robust(str(x).strip(), TEAM_NAME_MAPPING)
        )

        player_api_id_col = 'Player API ID'
        player_id_col_excel = 'player_id' # This seems to be your internal MongoDB ID from the merge script
        player_display_name_col = 'player_display_name'
        player_price_col = 'player_price'
        player_image_col = 'player_image'

        cols_from_excel_to_ensure = [
            player_api_id_col, player_id_col_excel, player_display_name_col,
            player_price_col, player_image_col, 'Position', 'Goals', 'Assists', 'Player Name'
        ]
        for col_name in cols_from_excel_to_ensure:
            if col_name not in player_df_raw.columns:
                print(f"Warning: Column '{col_name}' not found in '{PLAYER_STATS_FP}'. It will be created with null values.")
                player_df_raw[col_name] = None
            elif col_name in [player_api_id_col, player_id_col_excel]:
                player_df_raw[col_name] = player_df_raw[col_name].astype(str).str.strip().replace({'nan': None, 'None': None, '':None, 'NA':None})
            elif col_name == player_price_col:
                if pd.api.types.is_numeric_dtype(player_df_raw[col_name]):
                    player_df_raw[col_name] = player_df_raw[col_name].astype(object).where(player_df_raw[col_name].notna(), None)
                else:
                    player_df_raw[col_name] = player_df_raw[col_name].astype(str).str.strip().replace({'nan': None, 'None': None, '':None, 'NA':None})

        player_df = player_df_raw.copy()
        essential_cols_check = ['Position', 'Goals', 'Assists', 'Player Name', 'Team_Canonical']
        for col in essential_cols_check:
            if col not in player_df.columns or player_df[col].isnull().all():
                 err_msg = f"CRITICAL: Essential column '{col}' is missing or all null in '{PLAYER_STATS_FP}' after processing."
                 print(err_msg); return None, err_msg

        player_df['PositionCategory'] = player_df['Position'].apply(get_player_position_category)
        player_df['Goals'] = pd.to_numeric(player_df['Goals'], errors='coerce').fillna(0).astype(int)
        player_df['Assists'] = pd.to_numeric(player_df['Assists'], errors='coerce').fillna(0).astype(int)

    except FileNotFoundError:
        err_msg = f"CRITICAL: Player stats file not found at '{PLAYER_STATS_FP}'."
        print(err_msg); return None, err_msg
    except Exception as e:
        err_msg = f"CRITICAL: Could not load player stats from '{PLAYER_STATS_FP}': {e}."
        print(err_msg); return None, err_msg

    team_goals_season_overall = player_df.groupby('Team_Canonical')['Goals'].sum().to_dict()
    team_assists_season_overall = player_df.groupby('Team_Canonical')['Assists'].sum().to_dict()

    player_points_results_list = []
    for idx, fdr_match_row in fdr_final_df.iterrows():
        home_c, away_c, date_s = fdr_match_row['home_team_canonical'], fdr_match_row['away_team_canonical'], fdr_match_row['date_str']

        # Get fixture_id and GW for this match
        fixture_id_val = fdr_match_row['fixture_id']
        gw_val = fdr_match_row['GW']
        match_id_str = f"{home_c} vs {away_c} ({date_s})" # Human-readable identifier

        home_team_api_id_for_match = fdr_match_row['home_team_api_id']
        away_team_api_id_for_match = fdr_match_row['away_team_api_id']
        
        # Details for player's team and opponent team
        home_team_details = TEAM_DETAILS.get(home_c, DEFAULT_TEAM_DETAIL)
        away_team_details = TEAM_DETAILS.get(away_c, DEFAULT_TEAM_DETAIL)

        current_match_home_players = player_df[player_df['Team_Canonical'] == home_c]
        current_match_away_players = player_df[player_df['Team_Canonical'] == away_c]

        score_probs, points_calc_method = {}, ""
        fixture_key_cs_order1 = (home_c, away_c, date_s)
        fixture_key_cs_order2 = (away_c, home_c, date_s)
        cs_match_odds = cs_odds_lookup_for_fdr.get(fixture_key_cs_order1) or cs_odds_lookup_for_fdr.get(fixture_key_cs_order2)

        if cs_match_odds:
            raw_probs = {}
            scores_flipped_for_calc = fixture_key_cs_order2 in cs_odds_lookup_for_fdr and fixture_key_cs_order1 not in cs_odds_lookup_for_fdr
            for score, odd_val in cs_match_odds.items():
                try:
                    s_str, o_f = str(score), float(odd_val)
                    if o_f > 0 and re.match(r"^\d+-\d+$", s_str):
                        raw_probs[s_str] = 1.0 / o_f
                except: continue
            total_raw_p = sum(raw_probs.values())
            if total_raw_p > 1e-9:
                if scores_flipped_for_calc:
                     score_probs = {f"{s.split('-')[1]}-{s.split('-')[0]}": p/total_raw_p for s,p in raw_probs.items()}
                else:
                     score_probs = {s: p/total_raw_p for s,p in raw_probs.items()}
            else: score_probs = {"0-0":1.0}
            points_calc_method = "CS_Odds"
            if not score_probs or (len(score_probs) == 1 and "0-0" in score_probs and total_raw_p <= 1e-9) :
                 h_fdr, a_fdr = fdr_match_row['home_fdr_outright'], fdr_match_row['away_fdr_outright'] # Use outright FDR for xG
                 xg_h, xg_a = estimate_xg_from_fdr_outrights(h_fdr, a_fdr)
                 score_probs = get_score_probabilities_poisson(xg_h, xg_a)
                 points_calc_method = f"Poisson_Fallback_InvalidCS (xG:{xg_h:.1f}-{xg_a:.1f})"
        else:
            h_fdr, a_fdr = fdr_match_row['home_fdr_outright'], fdr_match_row['away_fdr_outright'] # Use outright FDR for xG
            xg_h, xg_a = estimate_xg_from_fdr_outrights(h_fdr, a_fdr)
            score_probs = get_score_probabilities_poisson(xg_h, xg_a)
            points_calc_method = f"Poisson (xG:{xg_h:.1f}-{xg_a:.1f})"

        if not score_probs: print(f"Warning: No score probabilities for {match_id_str}. Skipping players."); continue

        team_h_goals_s, team_h_assists_s = team_goals_season_overall.get(home_c,1) or 1, team_assists_season_overall.get(home_c,1) or 1
        # home_team_details is already defined above
        for _, p_row in current_match_home_players.iterrows():
            exp_pts = sum(calculate_player_points_for_specific_score(p_row, int(s.split('-')[0]), int(s.split('-')[1]), team_h_goals_s, team_h_assists_s) * prob for s,prob in score_probs.items() if '-' in s)
            player_points_results_list.append({
                'fixture_id': fixture_id_val, 
                'GW': gw_val,                 
                'MatchIdentifier': match_id_str,
                'Date': date_s,
                'Player Name': p_row.get('Player Name'),
                'Team Name': home_c,
                'Team API ID': home_team_details.get('api_id'),
                'Team Short Code': home_team_details.get('short_code'),
                'OpponentTeamApiId': away_team_api_id_for_match,
                'OpponentTeamShortCode': away_team_details.get('short_code'), # MODIFIED: Opponent is away_team
                'Player API ID': p_row.get(player_api_id_col),
                'player_id': p_row.get(player_id_col_excel),
                'player_display_name': p_row.get(player_display_name_col),
                'player_price': p_row.get(player_price_col),
                'player_image': p_row.get(player_image_col),
                'ExpectedPoints': exp_pts,
                'PointsCalcMethod': points_calc_method
            })

        team_a_goals_s, team_a_assists_s = team_goals_season_overall.get(away_c,1) or 1, team_assists_season_overall.get(away_c,1) or 1
        # away_team_details is already defined above
        for _, p_row in current_match_away_players.iterrows():
            exp_pts = sum(calculate_player_points_for_specific_score(p_row, int(s.split('-')[1]), int(s.split('-')[0]), team_a_goals_s, team_a_assists_s) * prob for s,prob in score_probs.items() if '-' in s)
            player_points_results_list.append({
                'fixture_id': fixture_id_val, 
                'GW': gw_val,                 
                'MatchIdentifier': match_id_str,
                'Date': date_s,
                'Player Name': p_row.get('Player Name'),
                'Team Name': away_c,
                'Team API ID': away_team_details.get('api_id'),
                'Team Short Code': away_team_details.get('short_code'),
                'OpponentTeamApiId': home_team_api_id_for_match,
                'OpponentTeamShortCode': home_team_details.get('short_code'), # MODIFIED: Opponent is home_team
                'Player API ID': p_row.get(player_api_id_col),
                'player_id': p_row.get(player_id_col_excel),
                'player_display_name': p_row.get(player_display_name_col),
                'player_price': p_row.get(player_price_col),
                'player_image': p_row.get(player_image_col),
                'ExpectedPoints': exp_pts,
                'PointsCalcMethod': points_calc_method
            })

    if not player_points_results_list:
        print("Warning: No player points were calculated.")
        return [], "No player points calculated."
    player_points_df = pd.DataFrame(player_points_results_list)
    if player_points_df.empty:
        print("Warning: Player points DataFrame is empty after processing. No data to return.")
        return [], "Player points DataFrame is empty after processing."

    player_points_df['ExpectedPoints'] = pd.to_numeric(player_points_df['ExpectedPoints'], errors='coerce').fillna(0.0)
    player_points_df['BonusPoints'] = 0
    
    group_cols_for_bonus = ['fixture_id', 'GW']
    if 'fixture_id' in player_points_df.columns and player_points_df['fixture_id'].astype(str).str.contains("N/A_ID", na=False).any():
        print("Warning: Fallback fixture_ids detected. Using MatchIdentifier for bonus point grouping uniqueness.")
        group_cols_for_bonus = ['MatchIdentifier']


    for _, match_group_df in player_points_df.groupby(group_cols_for_bonus):
        sorted_match_players = match_group_df.sort_values('ExpectedPoints', ascending=False)
        if len(sorted_match_players) >= 1: player_points_df.loc[sorted_match_players.index[0], 'BonusPoints'] = 3
        if len(sorted_match_players) >= 2: player_points_df.loc[sorted_match_players.index[1], 'BonusPoints'] = 2
        if len(sorted_match_players) >= 3: player_points_df.loc[sorted_match_players.index[2], 'BonusPoints'] = 1

    player_points_df['TotalPoints'] = round(player_points_df['ExpectedPoints'] + player_points_df['BonusPoints'], 2)

    grouped_data = []
    # MODIFIED: Added 'OpponentTeamShortCode' to output columns
    player_output_columns = [
        'Player Name', 'Team Name', 'Team API ID', 'Team Short Code', 
        'OpponentTeamApiId', 'OpponentTeamShortCode', 
        'Player API ID', 'player_id', player_display_name_col, player_price_col, player_image_col, 'TotalPoints'
    ]
    for col in player_output_columns: 
        if col not in player_points_df.columns:
            print(f"Final Check Warning: Column '{col}' missing from player_points_df. Adding with None.")
            player_points_df[col] = None
        else: 
            if pd.api.types.is_numeric_dtype(player_points_df[col]):
                 player_points_df[col] = player_points_df[col].astype(object).where(player_points_df[col].notna(), None)
            elif player_points_df[col].dtype == 'object':
                 player_points_df[col] = player_points_df[col].replace({np.nan: None, 'nan': None, 'None': None, '':None, 'NA':None})


    for (fix_id_grp, gw_grp, match_id_grp, date_grp), group_df_iterable in player_points_df.groupby(['fixture_id', 'GW', 'MatchIdentifier', 'Date']):
        group_df = group_df_iterable.copy()
        for col in player_output_columns:
            if col in group_df.columns:
                if pd.api.types.is_numeric_dtype(group_df[col]):
                    group_df[col] = group_df[col].astype(object).where(group_df[col].notna(), None)
                elif group_df[col].dtype == 'object':
                    group_df[col] = group_df[col].replace({np.nan: None, 'nan': None, 'None': None, '':None, 'NA':None})
            else: group_df[col] = None

        match_info = {
            "fixture_id": fix_id_grp,        
            "GW": gw_grp,                    
            "MatchIdentifier": match_id_grp, 
            "Date": date_grp,                
            "players": group_df[player_output_columns].to_dict(orient='records')
        }
        grouped_data.append(match_info)

    print("\nPlayer points calculated using methods for matches (from point_calculator):")
    if 'PointsCalcMethod' in player_points_df.columns:
        if 'fixture_id' in player_points_df.columns and 'GW' in player_points_df.columns and 'MatchIdentifier' in player_points_df.columns:
            method_counts_df = player_points_df.drop_duplicates(subset=['fixture_id', 'GW', 'MatchIdentifier'])
            method_counts = method_counts_df['PointsCalcMethod'].value_counts()
            if not method_counts.empty:
                print(method_counts)
            else: print("No methods recorded or no matches processed for method counts (after drop_duplicates).")
        else:
            print("One or more key columns ('fixture_id', 'GW', 'MatchIdentifier') missing for method count.")

    else: print("Could not log method counts as 'PointsCalcMethod' column was not in the final player DataFrame.")

    print(f"Successfully generated grouped player point data for {len(grouped_data)} matches in calculation engine.")
    return grouped_data, None



