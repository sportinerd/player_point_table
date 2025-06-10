# point_calculator.py

import pandas as pd
import numpy as np
import json
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from scipy.stats import poisson
import sys
import os

# --- Configuration & Constants ---
DATA_DIR = 'data' 
HTML_ODDS_FP = os.path.join(DATA_DIR, 'fifa_club_wc_odds.html')
MD_ODDS_FP = os.path.join(DATA_DIR, 'fifa_club_wc_odds.md')
CS_JSON_FP = os.path.join(DATA_DIR, 'correct_score.json')
PLAYER_STATS_FP = os.path.join(DATA_DIR, 'merged_mapped_players.xlsx') # Make sure this file has the 'player_id' column

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

# --- Team Name Mapping ---
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
    "Esperance Tunis": "Espérance Sportive de Tunis", "Wydad Athletic": "Wydad AC",
    "Mamelodi Sundowns": "Mamelodi Sundowns FC", "Auckland City": "Auckland City FC",

    "Real Madrid CF": "Real Madrid CF", "Manchester City FC": "Manchester City FC",
    "FC Bayern München": "FC Bayern München", "Paris Saint-Germain": "Paris Saint-Germain",
    "FC Internazionale Milano": "FC Internazionale Milano", "Chelsea FC": "Chelsea FC",
    "Atlético de Madrid": "Atlético de Madrid", "Borussia Dortmund": "Borussia Dortmund",
    "Juventus FC": "Juventus FC", "CR Flamengo": "CR Flamengo", "FC Porto": "FC Porto",
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

    "Al Ahly SC": "Al Ahly FC", "Paris SG": "Paris Saint-Germain",
    "Atletico Madrid": "Atlético de Madrid", "Man City": "Manchester City FC",
    "Bayern": "FC Bayern München", "Inter Milan": "FC Internazionale Milano",
    "Atl Madrid": "Atlético de Madrid",

    "Sociedade Esportiva Palmeiras": "SE Palmeiras",
    "Botafogo de Futebol e Regatas": "Botafogo FR",
    "Fluminense Football Club": "Fluminense FC",
    "Ulsan HD": "Ulsan HD FC",
    "Red Bull Salzburg": "FC Salzburg",
    "Al-Hilal": "Al Hilal SFC",
    "Wydad Casablanca": "Wydad AC"
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

_canonical_names_for_self_map = set(TEAM_NAME_MAPPING.values())
for canonical_name in TEAM_DETAILS.keys():
    _canonical_names_for_self_map.add(canonical_name)
for name in _canonical_names_for_self_map:
    if name not in TEAM_NAME_MAPPING or TEAM_NAME_MAPPING[name] != name:
        TEAM_NAME_MAPPING[name] = name

TIER_DISPLAY_MAPPING = {
    "Very Easy": "Very Easy", "Easy": "Easy", "Moderate": "Average Difficulty",
    "Hard": "Difficult", "Very Hard": "Very Difficult",
}

# --- Helper Functions ---
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
        pattern = re.compile(r"!\[(.*?)\]\(https?://.*?\)\s*\n*\s*(?:.*?)\s*\n*\s*(?:\d+)\s*\n*\s*(\+\d+)", re.MULTILINE)
        for raw_name, odds_text in pattern.findall(content):
            try:
                if odds_text.strip().startswith('+'):
                    odds_val = float(odds_text.strip()[1:]) / 100 + 1
                    teams_data.append({'raw_team_name': raw_name.strip(), 'decimal_odds': odds_val})
                else: print(f"Warning (MD Outright): Odds '{odds_text}' for '{raw_name.strip()}' not recognized.")
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
        canonical_name = team_map.get(raw_name, raw_name)
        if raw_name not in team_map and canonical_name == raw_name:
             print(f"Warning (Outright Map): Raw name '{raw_name}' (from {source}) not in TEAM_NAME_MAPPING.")
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
    venue_team_canonical = HOME_VENUES.get(stadium)
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
    cc_h, cc_a = False, False
    last_h_info, last_a_info = match_history_context.get(home_c), match_history_context.get(away_c)
    if last_h_info and last_h_info.get('venue'):
        cc_h = (stadium in east_coasts and last_h_info['venue'] in west_coasts) or \
               (stadium in west_coasts and last_h_info['venue'] in east_coasts)
    if last_a_info and last_a_info.get('venue'):
        cc_a = (stadium in east_coasts and last_a_info['venue'] in west_coasts) or \
               (stadium in west_coasts and last_a_info['venue'] in east_coasts)
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

def create_base_fixtures_with_canonical_names(team_map):
    user_provided_fixtures_raw = [
        {'home_team': 'Al Ahly FC', 'away_team': 'Inter Miami CF', 'date': '2025-06-14', 'time': '8:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'A'},
        {'home_team': 'SE Palmeiras', 'away_team': 'FC Porto', 'date': '2025-06-15', 'time': '6:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'A'},
        {'home_team': 'Paris Saint-Germain', 'away_team': 'Atlético de Madrid', 'date': '2025-06-15', 'time': '12:00 PM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'B'},
        {'home_team': 'Botafogo FR', 'away_team': 'Seattle Sounders FC', 'date': '2025-06-15', 'time': '7:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'B'},
        {'home_team': 'FC Bayern München', 'away_team': 'Auckland City FC', 'date': '2025-06-15', 'time': '12:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'C'},
        {'home_team': 'CA Boca Juniors', 'away_team': 'SL Benfica', 'date': '2025-06-16', 'time': '6:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'C'},
        {'home_team': 'CR Flamengo', 'away_team': 'Espérance Sportive de Tunis', 'date': '2025-06-16', 'time': '9:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'D'},
        {'home_team': 'Chelsea FC', 'away_team': 'LAFC', 'date': '2025-06-16', 'time': '3:00 PM', 'stadium': 'Mercedes-Benz Stadium, Atlanta, GA', 'group': 'D'},
        {'home_team': 'CA River Plate', 'away_team': 'Urawa Red Diamonds', 'date': '2025-06-17', 'time': '12:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'E'},
        {'home_team': 'CF Monterrey', 'away_team': 'FC Internazionale Milano', 'date': '2025-06-17', 'time': '6:00 PM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'E'},
        {'home_team': 'Fluminense FC', 'away_team': 'Borussia Dortmund', 'date': '2025-06-17', 'time': '12:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'F'},
        {'home_team': 'Ulsan HD FC', 'away_team': 'Mamelodi Sundowns FC', 'date': '2025-06-17', 'time': '6:00 PM', 'stadium': 'Inter&Co Stadium, Orlando, FL', 'group': 'F'},
        {'home_team': 'Manchester City FC', 'away_team': 'Wydad AC', 'date': '2025-06-18', 'time': '12:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'G'},
        {'home_team': 'Al Ain FC', 'away_team': 'Juventus FC', 'date': '2025-06-18', 'time': '9:00 PM', 'stadium': 'Audi Field, Washington, D.C.', 'group': 'G'},
        {'home_team': 'Real Madrid CF', 'away_team': 'Al Hilal SFC', 'date': '2025-06-18', 'time': '3:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'H'},
        {'home_team': 'CF Pachuca', 'away_team': 'FC Salzburg', 'date': '2025-06-18', 'time': '6:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'H'},
        {'home_team': 'SE Palmeiras', 'away_team': 'Al Ahly FC', 'date': '2025-06-19', 'time': '12:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'A'},
        {'home_team': 'Inter Miami CF', 'away_team': 'FC Porto', 'date': '2025-06-19', 'time': '3:00 PM', 'stadium': 'Mercedes-Benz Stadium, Atlanta, GA', 'group': 'A'},
        {'home_team': 'Paris Saint-Germain', 'away_team': 'Botafogo FR', 'date': '2025-06-19', 'time': '6:00 PM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'B'},
        {'home_team': 'Seattle Sounders FC', 'away_team': 'Atlético de Madrid', 'date': '2025-06-19', 'time': '3:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'B'},
        {'home_team': 'FC Bayern München', 'away_team': 'CA Boca Juniors', 'date': '2025-06-20', 'time': '9:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'C'},
        {'home_team': 'SL Benfica', 'away_team': 'Auckland City FC', 'date': '2025-06-20', 'time': '12:00 PM', 'stadium': 'Inter&Co Stadium, Orlando, FL', 'group': 'C'},
        {'home_team': 'CR Flamengo', 'away_team': 'Chelsea FC', 'date': '2025-06-20', 'time': '2:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'D'},
        {'home_team': 'LAFC', 'away_team': 'Espérance Sportive de Tunis', 'date': '2025-06-20', 'time': '5:00 PM', 'stadium': 'GEODIS Park, Nashville, TN', 'group': 'D'},
        {'home_team': 'CA River Plate', 'away_team': 'CF Monterrey', 'date': '2025-06-21', 'time': '6:00 PM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'E'},
        {'home_team': 'FC Internazionale Milano', 'away_team': 'Urawa Red Diamonds', 'date': '2025-06-21', 'time': '12:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'E'},
        {'home_team': 'Fluminense FC', 'away_team': 'Ulsan HD FC', 'date': '2025-06-21', 'time': '6:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'F'},
        {'home_team': 'Mamelodi Sundowns FC', 'away_team': 'Borussia Dortmund', 'date': '2025-06-21', 'time': '12:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'F'},
        {'home_team': 'Manchester City FC', 'away_team': 'Al Ain FC', 'date': '2025-06-22', 'time': '9:00 PM', 'stadium': 'Mercedes-Benz Stadium, Atlanta, GA', 'group': 'G'},
        {'home_team': 'Juventus FC', 'away_team': 'Wydad AC', 'date': '2025-06-22', 'time': '12:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'G'},
        {'home_team': 'Real Madrid CF', 'away_team': 'CF Pachuca', 'date': '2025-06-22', 'time': '3:00 PM', 'stadium': 'Bank of America Stadium, Charlotte, NC', 'group': 'H'},
        {'home_team': 'FC Salzburg', 'away_team': 'Al Hilal SFC', 'date': '2025-06-22', 'time': '6:00 PM', 'stadium': 'Audi Field, Washington, D.C.', 'group': 'H'},
        {'home_team': 'FC Porto', 'away_team': 'Al Ahly FC', 'date': '2025-06-23', 'time': '9:00 PM', 'stadium': 'MetLife Stadium, East Rutherford, NJ', 'group': 'A'},
        {'home_team': 'Inter Miami CF', 'away_team': 'SE Palmeiras', 'date': '2025-06-23', 'time': '9:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'A'},
        {'home_team': 'Atlético de Madrid', 'away_team': 'Botafogo FR', 'date': '2025-06-23', 'time': '12:00 PM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'B'},
        {'home_team': 'Seattle Sounders FC', 'away_team': 'Paris Saint-Germain', 'date': '2025-06-23', 'time': '12:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'B'},
        {'home_team': 'Auckland City FC', 'away_team': 'CA Boca Juniors', 'date': '2025-06-24', 'time': '2:00 PM', 'stadium': 'GEODIS Park, Nashville, TN', 'group': 'C'},
        {'home_team': 'SL Benfica', 'away_team': 'FC Bayern München', 'date': '2025-06-24', 'time': '3:00 PM', 'stadium': 'Bank of America Stadium, Charlotte, NC', 'group': 'C'},
        {'home_team': 'Espérance Sportive de Tunis', 'away_team': 'Chelsea FC', 'date': '2025-06-24', 'time': '9:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'D'},
        {'home_team': 'LAFC', 'away_team': 'CR Flamengo', 'date': '2025-06-24', 'time': '9:00 PM', 'stadium': 'Camping World Stadium, Orlando, FL', 'group': 'D'},
        {'home_team': 'Urawa Red Diamonds', 'away_team': 'CF Monterrey', 'date': '2025-06-25', 'time': '6:00 PM', 'stadium': 'Rose Bowl Stadium, Pasadena, CA', 'group': 'E'},
        {'home_team': 'FC Internazionale Milano', 'away_team': 'CA River Plate', 'date': '2025-06-25', 'time': '6:00 PM', 'stadium': 'Lumen Field, Seattle, WA', 'group': 'E'},
        {'home_team': 'Borussia Dortmund', 'away_team': 'Ulsan HD FC', 'date': '2025-06-25', 'time': '3:00 PM', 'stadium': 'TQL Stadium, Cincinnati, OH', 'group': 'F'},
        {'home_team': 'Mamelodi Sundowns FC', 'away_team': 'Fluminense FC', 'date': '2025-06-25', 'time': '3:00 PM', 'stadium': 'Hard Rock Stadium, Miami Gardens, FL', 'group': 'F'},
        {'home_team': 'Wydad AC', 'away_team': 'Al Ain FC', 'date': '2025-06-26', 'time': '3:00 PM', 'stadium': 'Audi Field, Washington, D.C.', 'group': 'G'},
        {'home_team': 'Juventus FC', 'away_team': 'Manchester City FC', 'date': '2025-06-26', 'time': '3:00 PM', 'stadium': 'Camping World Stadium, Orlando, FL', 'group': 'G'},
        {'home_team': 'Al Hilal SFC', 'away_team': 'CF Pachuca', 'date': '2025-06-26', 'time': '8:00 PM', 'stadium': 'GEODIS Park, Nashville, TN', 'group': 'H'},
        {'home_team': 'FC Salzburg', 'away_team': 'Real Madrid CF', 'date': '2025-06-26', 'time': '9:00 PM', 'stadium': 'Lincoln Financial Field, Philadelphia, PA', 'group': 'H'}
    ]
    processed_fixtures = []
    for fix_data in user_provided_fixtures_raw:
        home_raw, away_raw = str(fix_data['home_team']).strip(), str(fix_data['away_team']).strip()
        home_c, away_c = team_map.get(home_raw, home_raw), team_map.get(away_raw, away_raw)
        if home_raw not in team_map: print(f"Warning (Fixture Map): Raw home team '{home_raw}' not in TEAM_NAME_MAPPING.")
        if away_raw not in team_map: print(f"Warning (Fixture Map): Raw away team '{away_raw}' not in TEAM_NAME_MAPPING.")
        date_s, time_s = fix_data['date'], fix_data['time']
        if not home_c or not away_c or home_c == away_c: print(f"Warning: Skipping fixture due to mapping issue: {fix_data}"); continue
        try:
            date_obj_only = datetime.strptime(date_s, '%Y-%m-%d')
            dt_obj = datetime.strptime(f"{date_s} {time_s}", '%Y-%m-%d %I:%M %p')
            processed_fixtures.append({'home_team_canonical': home_c, 'away_team_canonical': away_c, 'date_str': date_s, 'time_str': time_s, 'stadium': fix_data['stadium'], 'group': fix_data['group'], 'date_dt': date_obj_only, 'datetime_obj': dt_obj})
        except ValueError as e: print(f"Error parsing fixture {fix_data}: {e}. Skipping.")
    all_unique_fixtures_sorted = sorted(processed_fixtures, key=lambda x: x['datetime_obj'])
    final_fixtures_list, final_unique_keys = [], set()
    for fix in all_unique_fixtures_sorted:
        key = (fix['home_team_canonical'], fix['away_team_canonical'], fix['date_str'])
        if key not in final_unique_keys:
            final_fixtures_list.append(fix)
            final_unique_keys.add(key)
    print(f"Created {len(final_fixtures_list)} unique base fixtures.")
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
    h_canonical, a_canonical = team_map.get(h_raw, h_raw), team_map.get(a_raw, a_raw)
    if h_raw not in team_map and h_raw != "Unknown": print(f"Warning (CS Map): Home team '{h_raw}' -> '{h_canonical}'.")
    if a_raw not in team_map and a_raw != "Unknown": print(f"Warning (CS Map): Away team '{a_raw}' -> '{a_canonical}'.")
    return (None, None) if "Unknown" in [h_canonical, a_canonical] else (h_canonical, a_canonical)

def load_correct_score_data_for_fdr(json_fp, team_map):
    cs_data_lookup = {}
    if not os.path.exists(json_fp):
        print(f"Info (CS Load): File not found: {json_fp}.")
        return cs_data_lookup
    try:
        with open(json_fp, 'r', encoding='utf-8') as f: data = json.load(f)
        if 'matches' not in data: print(f"Error: 'matches' key not in {json_fp}"); return {}
        for entry in data.get('matches', []):
            if not all(k in entry for k in ['match', 'date', 'correct_score_odds']): continue
            h_canonical, a_canonical = parse_cs_match_string_for_canonical_teams(entry['match'], team_map)
            if not h_canonical or not a_canonical: print(f"Warning (CS Load): Skip '{entry['match']}'."); continue
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", entry['date']): continue
            cs_data_lookup[(h_canonical, a_canonical, entry['date'])] = entry['correct_score_odds']
        print(f"✅ Loaded {len(cs_data_lookup)} matches from CS JSON: {json_fp}")
    except Exception as e: print(f"Error loading CS data: {e}")
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
    print(f"Warning: Unknown position '{position_str}', as Forward.")
    return 'Forward'

def calculate_player_points_for_specific_score(p_stats, team_goals, team_conceded, team_goals_season, team_assists_season):
    pos_cat = p_stats['PositionCategory']
    points = 2.0 # Appearance points
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
    probs, h_probs, a_probs = {}, [poisson.pmf(i, xg_h) for i in range(max_g)], [poisson.pmf(i, xg_a) for i in range(max_g)]
    for hg in range(max_g):
        for ag in range(max_g): probs[f"{hg}-{ag}"] = h_probs[hg] * a_probs[ag]
    total_p = sum(probs.values())
    return {s: p/total_p for s,p in probs.items()} if total_p > 1e-9 else {"0-0":1.0}

# --- Main Calculation Logic Function ---
def generate_all_player_points_data():
    print("--- Starting FIFA Club World Cup 2025 Analysis (Calculation Engine) ---")
    
    print("--- Calculating Fixture Difficulty Ratings (FDRs) ---")
    all_base_fixtures = create_base_fixtures_with_canonical_names(TEAM_NAME_MAPPING)
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
        outright_calcs = calculate_outright_fdr_components(fixture_details, team_strength_metrics, history_ctx)
        h_fdr_out, a_fdr_out = outright_calcs['home_fdr_outright'], outright_calcs['away_fdr_outright']
        method, h_fdr_cs, a_fdr_cs, P_h, P_d, P_a, h_afd, h_dfd, a_afd, a_dfd = "OutrightFDR", None,None,None,None,None,None,None,None,None
        
        fixture_key_cs_order1 = (home_c, away_c, date_s)
        fixture_key_cs_order2 = (away_c, home_c, date_s)
        cs_match_odds = cs_odds_lookup_for_fdr.get(fixture_key_cs_order1)
        if not cs_match_odds: cs_match_odds = cs_odds_lookup_for_fdr.get(fixture_key_cs_order2)

        if cs_match_odds:
            h_fdr_cs_calc,a_fdr_cs_calc,P_h_calc,P_d_calc,P_a_calc = calculate_correct_score_fdr_values(cs_match_odds)
            h_afd_calc,h_dfd_calc,a_afd_calc,a_dfd_calc = calculate_match_afd_dfd_from_cs_odds(cs_match_odds)
            h_fdr_cs, a_fdr_cs, P_h, P_d, P_a = h_fdr_cs_calc, a_fdr_cs_calc, P_h_calc, P_d_calc, P_a_calc
            h_afd, h_dfd, a_afd, a_dfd = h_afd_calc, h_dfd_calc, a_afd_calc, a_dfd_calc
            final_h_fdr = FINAL_FDR_WEIGHTS['outright']*h_fdr_out + FINAL_FDR_WEIGHTS['correct_score']*h_fdr_cs_calc
            final_a_fdr = FINAL_FDR_WEIGHTS['outright']*a_fdr_out + FINAL_FDR_WEIGHTS['correct_score']*a_fdr_cs_calc
            method = "CombinedFDR"
        else: final_h_fdr, final_a_fdr = h_fdr_out, a_fdr_out
            
        final_h_fdr, final_a_fdr = np.clip(final_h_fdr,1,99), np.clip(final_a_fdr,1,99)
        h_tier, a_tier, fdr_diff, comp_label = determine_match_tiers_and_competitiveness(final_h_fdr, final_a_fdr, TIER_DISPLAY_MAPPING)
        home_details, away_details = TEAM_DETAILS.get(home_c,DEFAULT_TEAM_DETAIL), TEAM_DETAILS.get(away_c,DEFAULT_TEAM_DETAIL)
        
        home_team_api_id = home_details.get('api_id', 'NA')
        away_team_api_id = away_details.get('api_id', 'NA')
        match_api_uids = f"{home_team_api_id}_vs_{away_team_api_id}"

        fdr_results_list.append({'date_str':date_s, 'time_str':fixture_details['time_str'], 'group':fixture_details['group'], 'stadium':fixture_details['stadium'],
            'home_team_canonical':home_c, 'home_team_short_code':home_details['short_code'], 'home_team_api_id':home_details['api_id'], 'home_team_logo':home_details['image'],
            'away_team_canonical':away_c, 'away_team_short_code':away_details['short_code'], 'away_team_api_id':away_details['api_id'], 'away_team_logo':away_details['image'],
            'match_api_uids': match_api_uids,
            'final_home_fdr':round(final_h_fdr,1), 'final_away_fdr':round(final_a_fdr,1), 'home_tier_display':h_tier, 'away_tier_display':a_tier,
            'fdr_difference':fdr_diff, 'match_competitiveness_label':comp_label, 'fdr_calculation_method':method,
            'home_fdr_outright':round(h_fdr_out,1) if h_fdr_out is not None else None, 'away_fdr_outright':round(a_fdr_out,1) if a_fdr_out is not None else None,
            'home_fdr_cs':round(h_fdr_cs,1) if h_fdr_cs is not None else None, 'away_fdr_cs':round(a_fdr_cs,1) if a_fdr_cs is not None else None,
            'home_strength_metric':round(outright_calcs['home_strength_metric'],1), 'away_strength_metric':round(outright_calcs['away_strength_metric'],1),
            'venue_impact_home':outright_calcs['venue_impact_home'], 'venue_impact_away':outright_calcs['venue_impact_away'],
            'fatigue_impact_home':outright_calcs['fatigue_impact_home'], 'fatigue_impact_away':outright_calcs['fatigue_impact_away'],
            'prob_home_win_cs':P_h, 'prob_draw_cs':P_d, 'prob_away_win_cs':P_a,
            'home_afd_cs':h_afd, 'home_dfd_cs':h_dfd, 'away_afd_cs':a_afd, 'away_dfd_cs':a_dfd})

    fdr_final_df = pd.DataFrame(fdr_results_list)
    if fdr_final_df.empty:
        print("CRITICAL: No FDR results generated in calculation engine.")
        return None, "No FDR results generated."
    
    print("--- Calculating Player Fantasy Points ---")
    try:
        player_df_raw = pd.read_excel(PLAYER_STATS_FP, sheet_name='Sheet1')
        player_team_column_name = 'Team Name' 
        if player_team_column_name not in player_df_raw.columns:
            player_team_column_name = 'Team' 
            if player_team_column_name not in player_df_raw.columns:
                err_msg = f"CRITICAL: Excel file '{PLAYER_STATS_FP}' missing team column (tried 'Team Name' and 'Team'). Available: {player_df_raw.columns.tolist()}"
                print(err_msg)
                return None, err_msg
        print(f"Info: Using column '{player_team_column_name}' for player teams from '{PLAYER_STATS_FP}'.")

        player_df_raw['Team_Canonical'] = player_df_raw[player_team_column_name].apply(lambda x: TEAM_NAME_MAPPING.get(str(x).strip(), str(x).strip()))
        
        player_api_id_col = 'Player API ID' # As in your previous script
        player_id_col_excel = 'player_id' # The new column from your screenshot

        # Handle Player API ID
        if player_api_id_col not in player_df_raw.columns:
            print(f"Warning: Column '{player_api_id_col}' not found in '{PLAYER_STATS_FP}'. Player API IDs will be null.")
            player_df_raw[player_api_id_col] = None
        else:
             player_df_raw[player_api_id_col] = player_df_raw[player_api_id_col].astype(str).replace('nan', None).replace('None', None)

        # Handle the new player_id column
        if player_id_col_excel not in player_df_raw.columns:
            print(f"Warning: Column '{player_id_col_excel}' not found in '{PLAYER_STATS_FP}'. Your custom 'player_id' will be null.")
            player_df_raw[player_id_col_excel] = None
        else:
            player_df_raw[player_id_col_excel] = player_df_raw[player_id_col_excel].astype(str).replace('nan', None).replace('None', None)
        
        player_df = player_df_raw.copy()
        required_player_cols = ['Position', 'Goals', 'Assists', 'Player Name', player_api_id_col, player_id_col_excel]
        for col in required_player_cols:
            if col not in player_df.columns:
                err_msg = f"CRITICAL: Player stats Excel file '{PLAYER_STATS_FP}' missing required column: '{col}'. Available: {player_df.columns.tolist()}"
                print(err_msg)
                return None, err_msg
        
        player_df['PositionCategory'] = player_df['Position'].apply(get_player_position_category)
        player_df['Goals'] = pd.to_numeric(player_df['Goals'], errors='coerce').fillna(0).astype(int)
        player_df['Assists'] = pd.to_numeric(player_df['Assists'], errors='coerce').fillna(0).astype(int)

    except FileNotFoundError:
        err_msg = f"CRITICAL: Player stats file not found at '{PLAYER_STATS_FP}'."
        print(err_msg); return None, err_msg
    except KeyError as e:
        err_msg = f"CRITICAL: Column missing in '{PLAYER_STATS_FP}': {e}."
        print(err_msg); return None, err_msg
    except Exception as e:
        err_msg = f"CRITICAL: Could not load player stats: {e}."
        print(err_msg); return None, err_msg

    team_goals_season_overall = player_df.groupby('Team_Canonical')['Goals'].sum().to_dict()
    team_assists_season_overall = player_df.groupby('Team_Canonical')['Assists'].sum().to_dict()
    
    player_points_results_list = []
    for idx, fdr_match_row in fdr_final_df.iterrows():
        home_c, away_c, date_s = fdr_match_row['home_team_canonical'], fdr_match_row['away_team_canonical'], fdr_match_row['date_str']
        match_id_str = f"{home_c} vs {away_c} ({date_s})"
        match_api_uids_str = fdr_match_row['match_api_uids'] 

        current_match_home_players = player_df[player_df['Team_Canonical'] == home_c]
        current_match_away_players = player_df[player_df['Team_Canonical'] == away_c]
        
        score_probs, points_calc_method = {}, ""
        fixture_key_cs_order1 = (home_c, away_c, date_s)
        fixture_key_cs_order2 = (away_c, home_c, date_s)
        cs_match_odds = cs_odds_lookup_for_fdr.get(fixture_key_cs_order1)
        if not cs_match_odds: cs_match_odds = cs_odds_lookup_for_fdr.get(fixture_key_cs_order2)

        if cs_match_odds:
            raw_probs = {}
            for score, odd_val in cs_match_odds.items():
                try: s_str, o_f = str(score), float(odd_val); raw_probs[s_str] = 1.0 / o_f if o_f > 0 and re.match(r"^\d+-\d+$", s_str) else raw_probs.get(s_str, 0)
                except: continue
            total_raw_p = sum(raw_probs.values())
            score_probs = {s: p/total_raw_p for s,p in raw_probs.items()} if total_raw_p > 1e-9 else {"0-0":1.0}
            points_calc_method = "CS_Odds"
            if not score_probs or (len(score_probs) == 1 and "0-0" in score_probs and total_raw_p <= 1e-9) :
                 h_fdr, a_fdr = fdr_match_row['home_fdr_outright'], fdr_match_row['away_fdr_outright']
                 xg_h, xg_a = estimate_xg_from_fdr_outrights(h_fdr, a_fdr)
                 score_probs = get_score_probabilities_poisson(xg_h, xg_a)
                 points_calc_method = f"Poisson_Fallback_InvalidCS (xG:{xg_h:.1f}-{xg_a:.1f})"
        else:
            h_fdr, a_fdr = fdr_match_row['home_fdr_outright'], fdr_match_row['away_fdr_outright']
            xg_h, xg_a = estimate_xg_from_fdr_outrights(h_fdr, a_fdr)
            score_probs = get_score_probabilities_poisson(xg_h, xg_a)
            points_calc_method = f"Poisson (xG:{xg_h:.1f}-{xg_a:.1f})"
            
        if not score_probs: print(f"Warning: No score probabilities for {match_id_str}. Skipping players."); continue

        team_h_goals_s, team_h_assists_s = team_goals_season_overall.get(home_c,1) or 1, team_assists_season_overall.get(home_c,1) or 1
        for _, p_row in current_match_home_players.iterrows():
            exp_pts = sum(calculate_player_points_for_specific_score(p_row, *map(int,s.split('-')), team_h_goals_s, team_h_assists_s) * prob for s,prob in score_probs.items() if '-' in s)
            team_api_id = TEAM_DETAILS.get(home_c, {}).get('api_id')
            player_points_results_list.append({
                'MatchIdentifier':match_id_str, 
                'MatchAPUIDs': match_api_uids_str, 
                'Date':date_s, 
                'Player Name':p_row['Player Name'], 
                'Team Name':home_c, 
                'Team API ID': team_api_id, 
                'Player API ID': p_row.get(player_api_id_col), # From original mapping
                'player_id': p_row.get(player_id_col_excel), # The new ID from Excel
                'ExpectedPoints':exp_pts, 
                'PointsCalcMethod':points_calc_method
            })
        
        team_a_goals_s, team_a_assists_s = team_goals_season_overall.get(away_c,1) or 1, team_assists_season_overall.get(away_c,1) or 1
        for _, p_row in current_match_away_players.iterrows():
            exp_pts = sum(calculate_player_points_for_specific_score(p_row, *map(int,s.split('-')[::-1]), team_a_goals_s, team_a_assists_s) * prob for s,prob in score_probs.items() if '-' in s)
            team_api_id = TEAM_DETAILS.get(away_c, {}).get('api_id')
            player_points_results_list.append({
                'MatchIdentifier':match_id_str, 
                'MatchAPUIDs': match_api_uids_str, 
                'Date':date_s, 
                'Player Name':p_row['Player Name'], 
                'Team Name':away_c, 
                'Team API ID': team_api_id, 
                'Player API ID': p_row.get(player_api_id_col), 
                'player_id': p_row.get(player_id_col_excel), # The new ID from Excel
                'ExpectedPoints':exp_pts, 
                'PointsCalcMethod':points_calc_method
            })

    if not player_points_results_list:
        print("Warning: No player points were calculated.")
        return [], "No player points calculated."

    player_points_df = pd.DataFrame(player_points_results_list)
    player_points_df['BonusPoints'] = 0
    for match_id_val in player_points_df['MatchIdentifier'].unique():
        match_players_indices = player_points_df[player_points_df['MatchIdentifier'] == match_id_val].sort_values('ExpectedPoints', ascending=False).index
        if len(match_players_indices) >= 1: player_points_df.loc[match_players_indices[0], 'BonusPoints'] = 3
        if len(match_players_indices) >= 2: player_points_df.loc[match_players_indices[1], 'BonusPoints'] = 2
        if len(match_players_indices) >= 3: player_points_df.loc[match_players_indices[2], 'BonusPoints'] = 1
    player_points_df['TotalPoints'] = round(player_points_df['ExpectedPoints'] + player_points_df['BonusPoints'], 2)
    
    grouped_data = []
    for (match_id, match_api_uids_val, date_val), group in player_points_df.groupby(['MatchIdentifier', 'MatchAPUIDs', 'Date']):
        match_info = {
            "MatchIdentifier": match_id,
            "MatchAPUIDs": match_api_uids_val,
            "Date": date_val,
            "players": group[['Player Name', 'Team Name', 'Team API ID', 'Player API ID', 'player_id', 'TotalPoints']].to_dict(orient='records')
        }
        grouped_data.append(match_info)

    print("\nPlayer points calculated using methods for matches (from point_calculator):")
    if 'PointsCalcMethod' in player_points_df.columns:
        print(player_points_df.drop_duplicates(subset=['MatchIdentifier'])['PointsCalcMethod'].value_counts())
    else:
        print("Could not log method counts as 'PointsCalcMethod' column was not in the final player DataFrame.")

    print(f"Successfully generated grouped player point data for {len(grouped_data)} matches in calculation engine.")
    return grouped_data, None