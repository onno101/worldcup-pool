"""Populate the simulation Lakebase branch with realistic World Cup 2026 data.

Scenario: We're midway through the Round of 16.
- All 72 group-stage matches: FINISHED with scores & goal events
- All 16 R32 (Round of 32) matches: FINISHED
- 4 of 8 R16 matches: FINISHED, rest SCHEDULED
- QF/SF/3rd/Final: still SCHEDULED
- 150 users with predictions for all finished + some upcoming matches
- 150 tournament predictions (winner + top scorer picks)
"""

from __future__ import annotations

import json
import os
import random
import uuid
from datetime import datetime, timezone
from urllib.parse import quote_plus

from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text

# ── Connection ──────────────────────────────────────────────────────

EP_NAME = os.environ.get(
    "LAKEBASE_ENDPOINT",
    "projects/worldcup-pool/branches/simulation/endpoints/primary",
)
DB_NAME = "databricks_postgres"

w = WorkspaceClient()
cred = w.postgres.generate_database_credential(endpoint=EP_NAME)
user = w.current_user.me().user_name
ep = w.postgres.get_endpoint(name=EP_NAME)
host = ep.status.hosts.host

url = (
    f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(cred.token)}"
    f"@{host}:5432/{quote_plus(DB_NAME)}"
)
engine = create_engine(url, connect_args={"sslmode": "require", "connect_timeout": 30})

# ── Team data ───────────────────────────────────────────────────────

GROUPS = {
    "GROUP_A": ["MEX", "KOR", "CZE", "RSA"],
    "GROUP_B": ["CAN", "SUI", "QAT", "BIH"],
    "GROUP_C": ["BRA", "MAR", "HAI", "SCO"],
    "GROUP_D": ["USA", "TUR", "AUS", "PAR"],
    "GROUP_E": ["GER", "ECU", "CIV", "CUW"],
    "GROUP_F": ["NED", "JPN", "SWE", "TUN"],
    "GROUP_G": ["BEL", "EGY", "IRN", "NZL"],
    "GROUP_H": ["ESP", "URU", "KSA", "CPV"],
    "GROUP_I": ["FRA", "NOR", "SEN", "IRQ"],
    "GROUP_J": ["ARG", "ALG", "AUT", "JOR"],
    "GROUP_K": ["POR", "COL", "UZB", "COD"],
    "GROUP_L": ["ENG", "CRO", "GHA", "PAN"],
}

TEAM_NAMES = {
    "MEX": "Mexico", "KOR": "South Korea", "CZE": "Czechia", "RSA": "South Africa",
    "CAN": "Canada", "SUI": "Switzerland", "QAT": "Qatar", "BIH": "Bosnia-Herzegovina",
    "BRA": "Brazil", "MAR": "Morocco", "HAI": "Haiti", "SCO": "Scotland",
    "USA": "United States", "TUR": "Turkey", "AUS": "Australia", "PAR": "Paraguay",
    "GER": "Germany", "ECU": "Ecuador", "CIV": "Ivory Coast", "CUW": "Curaçao",
    "NED": "Netherlands", "JPN": "Japan", "SWE": "Sweden", "TUN": "Tunisia",
    "BEL": "Belgium", "EGY": "Egypt", "IRN": "Iran", "NZL": "New Zealand",
    "ESP": "Spain", "URU": "Uruguay", "KSA": "Saudi Arabia", "CPV": "Cape Verde Islands",
    "FRA": "France", "NOR": "Norway", "SEN": "Senegal", "IRQ": "Iraq",
    "ARG": "Argentina", "ALG": "Algeria", "AUT": "Austria", "JOR": "Jordan",
    "POR": "Portugal", "COL": "Colombia", "UZB": "Uzbekistan", "COD": "Congo DR",
    "ENG": "England", "CRO": "Croatia", "GHA": "Ghana", "PAN": "Panama",
}

# Strength tiers for realistic results (higher = better)
TEAM_STRENGTH = {
    "BRA": 92, "FRA": 91, "ARG": 90, "ENG": 89, "ESP": 88,
    "GER": 87, "POR": 86, "NED": 85, "BEL": 84, "USA": 83,
    "CRO": 82, "URU": 81, "COL": 80, "MEX": 79, "JPN": 78,
    "MAR": 77, "SUI": 76, "TUR": 75, "KOR": 74, "EGY": 73,
    "SEN": 72, "CAN": 71, "AUS": 70, "ECU": 69, "NOR": 68,
    "SWE": 67, "AUT": 66, "CIV": 65, "ALG": 64, "IRN": 63,
    "GHA": 62, "CZE": 61, "SCO": 60, "TUN": 59, "BIH": 58,
    "PAN": 55, "PAR": 54, "NZL": 53, "UZB": 52, "KSA": 51,
    "JOR": 50, "IRQ": 49, "QAT": 48, "HAI": 45, "CPV": 44,
    "COD": 43, "RSA": 42, "CUW": 35,
}

# Famous/realistic scorers per team
TEAM_SCORERS = {
    "BRA": ["Vinícius Jr.", "Rodrygo", "Endrick", "Raphinha"],
    "FRA": ["Mbappé", "Griezmann", "Thuram", "Dembélé"],
    "ARG": ["Messi", "Álvarez", "Lautaro Martínez", "Di María"],
    "ENG": ["Bellingham", "Kane", "Saka", "Foden"],
    "ESP": ["Yamal", "Morata", "Olmo", "Williams"],
    "GER": ["Musiala", "Havertz", "Wirtz", "Sané"],
    "POR": ["Ronaldo", "B. Silva", "Leão", "Félix"],
    "NED": ["Gakpo", "Depay", "Simons", "Malen"],
    "BEL": ["Lukaku", "De Bruyne", "Doku", "Trossard"],
    "USA": ["Pulisic", "Weah", "Balogun", "Reyna"],
    "CRO": ["Kramarić", "Petković", "Budimir", "Sučić"],
    "URU": ["Núñez", "Suárez", "Valverde", "De Arrascaeta"],
    "COL": ["Luis Díaz", "Ríos", "Borré", "Córdoba"],
    "MEX": ["Giménez", "Vega", "Lozano", "Romo"],
    "JPN": ["Mitoma", "Kubo", "Kamada", "Doan"],
    "MAR": ["En-Nesyri", "Ziyech", "Hakimi", "Boufal"],
    "SUI": ["Embolo", "Ndoye", "Amdouni", "Shaqiri"],
    "TUR": ["Yıldız", "Aktürkoğlu", "Ünder", "Güler"],
    "KOR": ["Son Heung-min", "Hwang Hee-chan", "Lee Kang-in", "Cho Gue-sung"],
    "EGY": ["Salah", "Trezeguet", "Marmoush", "Mostafa"],
    "SEN": ["Dia", "Sarr", "Diedhiou", "Diatta"],
    "CAN": ["David", "Davies", "Larin", "Buchanan"],
    "AUS": ["Duke", "Maclaren", "Irvine", "Goodwin"],
    "ECU": ["Valencia", "Caicedo", "Sarmiento", "Mena"],
    "NOR": ["Haaland", "Ødegaard", "Sørloth", "Nusa"],
    "SWE": ["Isak", "Kulusevski", "Gyökeres", "Elanga"],
    "AUT": ["Arnautović", "Gregoritsch", "Sabitzer", "Baumgartner"],
    "CIV": ["Haller", "Gradel", "Boly", "Pépé"],
    "ALG": ["Mahrez", "Bennacer", "Bounedjah", "Belaïli"],
    "IRN": ["Taremi", "Azmoun", "Jahanbakhsh", "Ghoddos"],
    "GHA": ["Kudus", "Ayew", "Williams", "Sulemana"],
    "CZE": ["Schick", "Hložek", "Kuchta", "Červ"],
    "SCO": ["Adams", "Dykes", "McTominay", "McGinn"],
    "TUN": ["Khazri", "Msakni", "Jaziri", "Sliti"],
    "BIH": ["Džeko", "Demirović", "Prevljak", "Gazibegović"],
    "PAN": ["Fajardo", "Bárcenas", "Blackburn", "Murillo"],
    "PAR": ["Enciso", "Sanabria", "Almirón", "Romero"],
    "NZL": ["Wood", "Singh", "Waine", "Garbett"],
    "UZB": ["Shomurodov", "Jaloliddinov", "Kuvvatov", "Abdullaev"],
    "KSA": ["Al-Dawsari", "Al-Shehri", "Kanno", "Al-Buraikan"],
    "JOR": ["Al-Tamari", "Al-Naimat", "Al-Rawabdeh", "Bani Ateyah"],
    "IRQ": ["Muhanad Ali", "Basim Abbas", "Amjad Attwan", "Dhurgham"],
    "QAT": ["Afif", "Al-Haydos", "Almoez Ali", "Hatem"],
    "HAI": ["Duverger", "Pierrot", "Bazile", "Alceus"],
    "CPV": ["Garry Rodrigues", "Ryan Mendes", "Júlio Tavares", "Stopira"],
    "COD": ["Bakambu", "Wissa", "Mbemba", "Kakuta"],
    "RSA": ["Zwane", "Tau", "Shalulile", "Foster"],
    "CUW": ["Bacuna", "Hooi", "Breinburg", "Janga"],
}

# ── Simulated group stage results ──────────────────────────────────

# Pre-determined final group standings (1st, 2nd, 3rd, 4th)
# These determine who qualifies for knockout rounds
GROUP_STANDINGS = {
    "GROUP_A": ["MEX", "KOR", "CZE", "RSA"],
    "GROUP_B": ["SUI", "CAN", "BIH", "QAT"],
    "GROUP_C": ["BRA", "MAR", "SCO", "HAI"],
    "GROUP_D": ["USA", "TUR", "AUS", "PAR"],
    "GROUP_E": ["GER", "ECU", "CIV", "CUW"],
    "GROUP_F": ["NED", "JPN", "SWE", "TUN"],
    "GROUP_G": ["BEL", "EGY", "IRN", "NZL"],
    "GROUP_H": ["ESP", "URU", "KSA", "CPV"],
    "GROUP_I": ["FRA", "NOR", "SEN", "IRQ"],
    "GROUP_J": ["ARG", "AUT", "ALG", "JOR"],
    "GROUP_K": ["POR", "COL", "UZB", "COD"],
    "GROUP_L": ["ENG", "CRO", "GHA", "PAN"],
}

# Top 2 from each group + best 3rd-place teams qualify for R32 (32 teams)
# With 12 groups × 2 = 24 auto-qualifiers + 8 best 3rd-place teams = 32
BEST_THIRD_PLACE = ["CZE", "AUS", "CIV", "SWE", "SEN", "ALG", "UZB", "GHA"]

# All 32 qualifying teams
QUALIFIERS_1ST = [GROUP_STANDINGS[g][0] for g in sorted(GROUP_STANDINGS)]
QUALIFIERS_2ND = [GROUP_STANDINGS[g][1] for g in sorted(GROUP_STANDINGS)]
R32_TEAMS = QUALIFIERS_1ST + QUALIFIERS_2ND + BEST_THIRD_PLACE

# R32 matchups (1A vs 3C/D/E, 2A vs 2B, etc.) — creative but plausible
R32_MATCHUPS = [
    # Match order follows kickoff_utc order from DB
    ("MEX", "SEN"),    # 537417 Jun28
    ("BRA", "GHA"),    # 537423 Jun29
    ("SUI", "AUS"),    # 537415 Jun29
    ("NED", "ALG"),    # 537418 Jun30
    ("GER", "CRO"),    # 537424 Jun30
    ("USA", "CAN"),    # 537416 Jun30
    ("BEL", "SWE"),    # 537425 Jul1
    ("ESP", "CZE"),    # 537426 Jul1
    ("FRA", "UZB"),    # 537422 Jul1
    ("ARG", "NOR"),    # 537421 Jul2
    ("POR", "EGY"),    # 537420 Jul2
    ("ENG", "COL"),    # 537419 Jul2
    ("KOR", "ECU"),    # 537429 Jul3
    ("MAR", "TUR"),    # 537428 Jul3
    ("JPN", "AUT"),    # 537427 Jul3
    ("URU", "CIV"),    # 537430 Jul4
]

# R32 results
R32_RESULTS = [
    # (home_score, away_score, winner) — winner = advancing team
    (2, 1, "MEX"),     # Mexico edges Senegal
    (3, 0, "BRA"),     # Brazil cruises past Ghana
    (1, 1, "SUI"),     # Switzerland wins on pens
    (2, 0, "NED"),     # Netherlands handles Algeria
    (1, 2, "CRO"),     # Croatia upsets Germany!
    (0, 0, "USA"),     # USA wins on pens in home crowd thriller
    (3, 1, "BEL"),     # Belgium too strong for Sweden
    (4, 0, "ESP"),     # Spain demolishes Czechia
    (3, 0, "FRA"),     # France cruises
    (2, 1, "ARG"),     # Argentina edges Norway (Haaland consolation goal)
    (2, 0, "POR"),     # Portugal comfortable
    (1, 1, "ENG"),     # England wins pens vs Colombia (classic)
    (1, 2, "ECU"),     # Ecuador shocks South Korea!
    (0, 1, "TUR"),     # Turkey edges Morocco
    (2, 1, "JPN"),     # Japan continues their giant-killing form
    (1, 0, "URU"),     # Uruguay grinds past Ivory Coast
]

# R16 matchups (winners from R32)
R16_MATCHUPS = [
    # First 4 are FINISHED, last 4 SCHEDULED
    ("MEX", "BRA"),    # 537376 Jul4
    ("NED", "CRO"),    # 537375 Jul4
    ("USA", "BEL"),    # 537377 Jul5
    ("ESP", "SUI"),    # 537378 Jul6
    ("FRA", "ARG"),    # 537379 Jul6 — SCHEDULED (the big one!)
    ("POR", "ENG"),    # 537380 Jul7 — SCHEDULED
    ("ECU", "TUR"),    # 537381 Jul7 — SCHEDULED
    ("JPN", "URU"),    # 537382 Jul7 — SCHEDULED
]

R16_RESULTS = [
    # First 4 finished
    (1, 3, "BRA"),     # Brazil demolishes Mexico
    (2, 2, "NED"),     # Netherlands wins on pens vs Croatia
    (2, 1, "USA"),     # USA edges Belgium! Home crowd goes wild
    (3, 1, "ESP"),     # Spain dominates Switzerland
    # Last 4 not yet played
    None, None, None, None,
]

# ── R32 match IDs (in kickoff order) ───────────────────────────────

R32_MATCH_IDS = [
    "b06c363d-4638-404e-8699-73c487def18a",  # 537417 Jun28
    "a94062c8-240b-40bd-b249-e0a298aef57d",  # 537423 Jun29
    "0d41a24b-2f7e-45cd-aabd-282f09777ca5",  # 537415 Jun29
    "c6d44e2d-acf3-4e5f-bc1f-4b2c7a65189e",  # 537418 Jun30
    "a5382fa7-fb9b-496e-8936-86112e9a844f",  # 537424 Jun30
    "e381eb46-d104-4bbe-87b4-8b20e2e8a7e1",  # 537416 Jun30
    "e4a0b4cf-c93b-490b-928a-2647b8202353",  # 537425 Jul1
    "614f47e9-cbd3-451f-bd4a-7331724c043d",  # 537426 Jul1
    "705175a2-5f9a-4679-aeac-4676ffb6eb7d",  # 537422 Jul1
    "7fac3930-e995-4842-a3d9-dc6c5da56f5e",  # 537421 Jul2
    "ff16fe7b-84c9-46ee-ad45-2851cbafd6b6",  # 537420 Jul2
    "8c34f2e2-132d-4976-aaea-32d0b016b83d",  # 537419 Jul2
    "72c63952-cb1e-4dd1-a4af-c410baeaf2aa",  # 537429 Jul3
    "54b36e9f-ffef-4f4e-92ba-6265f1bdd6b9",  # 537428 Jul3
    "9b0a3e0c-f71d-4543-aef9-14f20e2e618b",  # 537427 Jul3
    "4e86b480-a99d-472c-b61b-f651fbe8cb7f",  # 537430 Jul4
]

R16_MATCH_IDS = [
    "96199ca7-34c6-4526-a17c-5324ef29fbce",  # 537376 Jul4
    "6ffc1744-0e24-4e16-9f64-2d46a11219d2",  # 537375 Jul4
    "cc70d64b-1c30-4114-bbd8-910da80f491e",  # 537377 Jul5
    "abb43595-8eba-4317-8824-a9f0745d1f29",  # 537378 Jul6
    "a4a1f3d1-6797-46c2-8070-70085b989fca",  # 537379 Jul6
    "f017010c-bf91-45b1-84be-33ed360b5acc",  # 537380 Jul7
    "a84d517c-3329-4294-a0b2-668ed666449f",  # 537381 Jul7
    "3f064d22-765c-4dee-8e0d-72c670b14416",  # 537382 Jul7
]


def simulate_score(home: str, away: str, rng: random.Random) -> tuple[int, int]:
    """Generate a plausible score based on team strength."""
    h_str = TEAM_STRENGTH.get(home, 50)
    a_str = TEAM_STRENGTH.get(away, 50)
    # Home advantage slight boost
    h_str += 3
    diff = (h_str - a_str) / 25.0  # rough expected goal diff

    # Base goals ~ Poisson-ish
    h_goals = max(0, round(rng.gauss(1.3 + max(diff, 0) * 0.3, 0.8)))
    a_goals = max(0, round(rng.gauss(1.3 + max(-diff, 0) * 0.3, 0.8)))

    # Cap at reasonable values
    h_goals = min(h_goals, 6)
    a_goals = min(a_goals, 6)
    return h_goals, a_goals


def make_goal_events(
    home: str, away: str, h_goals: int, a_goals: int, rng: random.Random
) -> list[dict]:
    """Generate goal event list with scorers and minutes."""
    events = []
    h_scorers = TEAM_SCORERS.get(home, ["Player 1", "Player 2"])
    a_scorers = TEAM_SCORERS.get(away, ["Player 1", "Player 2"])

    used_minutes: set[int] = set()
    for _ in range(h_goals):
        minute = rng.randint(1, 90)
        while minute in used_minutes:
            minute = rng.randint(1, 90)
        used_minutes.add(minute)
        events.append({
            "team_code": home,
            "player_name": rng.choice(h_scorers),
            "minute": minute,
            "type": "REGULAR" if rng.random() > 0.1 else "PENALTY",
        })
    for _ in range(a_goals):
        minute = rng.randint(1, 90)
        while minute in used_minutes:
            minute = rng.randint(1, 90)
        used_minutes.add(minute)
        events.append({
            "team_code": away,
            "player_name": rng.choice(a_scorers),
            "minute": minute,
            "type": "REGULAR" if rng.random() > 0.1 else "PENALTY",
        })
    events.sort(key=lambda e: e["minute"])
    return events


# ── Fake users ──────────────────────────────────────────────────────

FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Sophia", "Jackson", "Ava", "Lucas",
    "Isabella", "Aiden", "Mia", "Ethan", "Charlotte", "James", "Amelia",
    "Benjamin", "Harper", "Elijah", "Evelyn", "Logan", "Abigail", "Mason",
    "Emily", "Jacob", "Elizabeth", "Michael", "Sofia", "Daniel", "Avery",
    "Henry", "Ella", "Sebastian", "Scarlett", "Jack", "Grace", "Owen",
    "Victoria", "Samuel", "Riley", "William", "Aria", "Alexander", "Lily",
    "Matthew", "Layla", "Joseph", "Zoey", "David", "Penelope", "Carter",
    "Nora", "Wyatt", "Camila", "John", "Hannah", "Luke", "Addison",
    "Gabriel", "Eleanor", "Anthony", "Stella", "Isaac", "Natalie", "Dylan",
    "Zoe", "Leo", "Leah", "Lincoln", "Hazel", "Jaxon", "Violet",
    "Asher", "Aurora", "Christopher", "Savannah", "Josiah", "Audrey",
    "Andrew", "Brooklyn", "Thomas", "Bella", "Joshua", "Claire",
    "Ezra", "Skylar", "Adrian", "Lucy", "Caleb", "Paisley", "Ryan",
    "Anna", "Nathan", "Caroline", "Aaron", "Genesis", "Eli", "Kennedy",
    "Landon", "Sadie", "Tyler", "Allison",
    # More variety
    "Pieter", "Sanne", "Lars", "Femke", "Bram", "Fleur", "Daan", "Eva",
    "Thijs", "Lotte", "Milan", "Sophie", "Ruben", "Julia", "Stijn", "Lisa",
    "Wouter", "Iris", "Bas", "Anne", "Joris", "Merel", "Niels", "Sara",
    "Marco", "Giulia", "Ahmed", "Fatima", "Yuki", "Sakura", "Carlos", "María",
    "Pierre", "Chloé", "Hans", "Greta", "Raj", "Priya", "Chen", "Wei",
    "Omar", "Layla", "Dmitri", "Natasha", "Tariq", "Aaliya", "Jean", "Céline",
    "Gustaf", "Astrid", "Paulo", "Beatriz",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "García", "Miller",
    "Davis", "Rodríguez", "Martínez", "Hernández", "López", "González",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Pérez", "Thompson", "White", "Harris", "Sánchez", "Clark",
    "Ramírez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    # Dutch/European surnames
    "de Vries", "van den Berg", "Jansen", "Bakker", "Visser", "Mulder",
    "de Groot", "Bos", "Vos", "Peters", "Hendriks", "van Dijk", "Smit",
    "Meijer", "de Boer", "Dekker", "Dijkstra", "Vermeer", "van der Linden",
    "Kuijpers", "Peeters", "Willems", "Claes", "Dubois", "Laurent",
    "Müller", "Schmidt", "Fischer", "Weber", "Becker", "Johansson",
    "Eriksson", "Lindberg", "Rossi", "Esposito", "Silva", "Santos",
    "Ferreira", "Almeida", "Tanaka", "Watanabe", "Yamamoto", "Suzuki",
    "Patel", "Kumar", "Singh", "Chen", "Wang", "Zhang", "Li", "Kim",
]

NATIONALITIES = [
    "NL", "NL", "NL", "NL", "NL",  # Dutch overrepresented (it's our pool!)
    "NL", "NL", "NL", "NL", "NL",
    "BE", "BE", "BE", "DE", "DE",
    "GB", "GB", "US", "US", "FR",
    "ES", "BR", "AR", "JP", "IT",
    "NO", "SE", "DK", "PT", "MX",
    "CO", "TR", "EG", "MA", "KR",
    "AU", "CA", "AT", "CH", "PL",
]

WINNER_PICKS = [
    # Weighted toward favorites
    "BRA", "BRA", "BRA", "BRA", "BRA", "BRA",
    "FRA", "FRA", "FRA", "FRA", "FRA",
    "ARG", "ARG", "ARG", "ARG", "ARG",
    "ENG", "ENG", "ENG", "ENG",
    "ESP", "ESP", "ESP", "ESP",
    "GER", "GER", "GER",
    "POR", "POR", "POR",
    "NED", "NED", "NED", "NED", "NED",  # Home bias
    "BEL", "BEL",
    "USA", "USA",
    "CRO", "URU", "COL", "MEX", "JPN", "MAR",
]

TOP_SCORERS = [
    "Mbappé", "Mbappé", "Mbappé", "Mbappé", "Mbappé",
    "Haaland", "Haaland", "Haaland", "Haaland",
    "Messi", "Messi", "Messi",
    "Kane", "Kane", "Kane",
    "Vinícius Jr.", "Vinícius Jr.", "Vinícius Jr.",
    "Ronaldo", "Ronaldo", "Ronaldo",
    "Bellingham", "Bellingham",
    "Yamal", "Yamal",
    "Musiala", "Musiala",
    "Isak", "Isak",
    "Pulisic", "Pulisic",
    "Son Heung-min", "Son Heung-min",
    "Salah", "Salah",
    "Depay", "Gakpo", "Álvarez", "Morata", "Lukaku",
    "Núñez", "Havertz", "Luis Díaz", "En-Nesyri",
]

# Map scorer names to their country code + name (required for scoring engine)
SCORER_COUNTRY: dict[str, tuple[str, str]] = {
    "Mbappé": ("FRA", "France"), "Haaland": ("NOR", "Norway"),
    "Messi": ("ARG", "Argentina"), "Kane": ("ENG", "England"),
    "Vinícius Jr.": ("BRA", "Brazil"), "Ronaldo": ("POR", "Portugal"),
    "Bellingham": ("ENG", "England"), "Yamal": ("ESP", "Spain"),
    "Musiala": ("GER", "Germany"), "Isak": ("SWE", "Sweden"),
    "Pulisic": ("USA", "United States"), "Son Heung-min": ("KOR", "South Korea"),
    "Salah": ("EGY", "Egypt"), "Depay": ("NED", "Netherlands"),
    "Gakpo": ("NED", "Netherlands"), "Álvarez": ("ARG", "Argentina"),
    "Morata": ("ESP", "Spain"), "Lukaku": ("BEL", "Belgium"),
    "Núñez": ("URU", "Uruguay"), "Havertz": ("GER", "Germany"),
    "Luis Díaz": ("COL", "Colombia"), "En-Nesyri": ("MAR", "Morocco"),
    "Endrick": ("BRA", "Brazil"), "Olmo": ("ESP", "Spain"),
}


def generate_users(n: int, rng: random.Random) -> list[dict]:
    """Generate n fake user records."""
    users = []
    used_emails: set[str] = set()
    for i in range(n):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        email = f"{first.lower()}.{last.lower().replace(' ', '')}@example.com"
        # Ensure unique
        if email in used_emails:
            email = f"{first.lower()}.{last.lower().replace(' ', '')}{i}@example.com"
        used_emails.add(email)

        nationality = rng.choice(NATIONALITIES)

        # Winner pick with slight bias toward own nation
        winner = rng.choice(WINNER_PICKS)
        if nationality in TEAM_STRENGTH and rng.random() < 0.3:
            winner = nationality  # 30% chance to pick own country

        users.append({
            "user_id": email,
            "email": email,
            "display_name": f"{first} {last}",
            "nationality": nationality,
            "expected_winner_team_code": winner,
            "tournament_winner_pick": winner,
            "top_scorer_pick": rng.choice(TOP_SCORERS),
        })
    return users


def generate_prediction(
    user_strength: float, actual_home: int, actual_away: int,
    home_team: str, away_team: str, is_knockout: bool, rng: random.Random,
) -> dict:
    """Generate a prediction that has some correlation with actual result."""
    # user_strength 0..1: how "good" the predictor is
    if rng.random() < user_strength * 0.15:
        # Exact score prediction (rare)
        h, a = actual_home, actual_away
    elif rng.random() < user_strength * 0.4:
        # Correct outcome, different score
        if actual_home > actual_away:
            h = rng.randint(1, 4)
            a = rng.randint(0, h - 1)
        elif actual_away > actual_home:
            a = rng.randint(1, 4)
            h = rng.randint(0, a - 1)
        else:
            h = a = rng.randint(0, 3)
    else:
        # Random-ish prediction with slight strength awareness
        h_str = TEAM_STRENGTH.get(home_team, 50)
        a_str = TEAM_STRENGTH.get(away_team, 50)
        diff = (h_str - a_str) / 30.0
        h = max(0, min(5, round(rng.gauss(1.2 + max(diff, 0) * 0.2, 0.9))))
        a = max(0, min(5, round(rng.gauss(1.2 + max(-diff, 0) * 0.2, 0.9))))

    result = {"home_goals": h, "away_goals": a}

    # For knockout matches with a draw, pick an advancer
    if is_knockout and h == a:
        # Pick based on team strength + randomness
        h_str = TEAM_STRENGTH.get(home_team, 50)
        a_str = TEAM_STRENGTH.get(away_team, 50)
        if rng.random() < h_str / (h_str + a_str):
            result["advance_team_code"] = home_team
        else:
            result["advance_team_code"] = away_team

    return result


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    rng = random.Random(42)  # Reproducible

    print("Connecting to simulation branch...")

    with engine.connect() as conn:
        # First, clear existing predictions (branch has some from production)
        conn.execute(text("DELETE FROM match_predictions"))
        conn.execute(text("DELETE FROM tournament_predictions"))
        conn.execute(text("DELETE FROM user_profiles"))
        conn.execute(text("DELETE FROM app_users_cache"))
        conn.commit()
        print("Cleared existing data")

        # ── 1. Update GROUP STAGE matches with results ──────────────
        print("\n=== Updating group stage matches ===")

        gs_rows = conn.execute(text("""
            SELECT id, home_team_code, away_team_code, stage, kickoff_utc
            FROM matches
            WHERE stage = 'GROUP_STAGE'
            ORDER BY kickoff_utc
        """)).fetchall()

        # Track goals per scorer for the whole tournament (for top scorer tracking)
        scorer_goals: dict[str, int] = {}

        for row in gs_rows:
            mid, home, away, stage, kickoff = row
            h_goals, a_goals = simulate_score(home, away, rng)

            # Make some results more interesting/realistic
            # Opening match is usually cagey
            if home == "MEX" and away == "RSA":
                h_goals, a_goals = 1, 0
            # Brazil always scores
            elif home == "BRA" and away == "MAR":
                h_goals, a_goals = 2, 1
            elif home == "BRA" and away == "HAI":
                h_goals, a_goals = 4, 0
            elif away == "BRA" and home == "SCO":
                h_goals, a_goals = 0, 3
            # Big matches
            elif home == "FRA" and away == "SEN":
                h_goals, a_goals = 2, 1
            elif home == "ARG" and away == "ALG":
                h_goals, a_goals = 3, 0
            elif home == "ENG" and away == "CRO":
                h_goals, a_goals = 1, 1
            elif home == "USA" and away == "PAR":
                h_goals, a_goals = 2, 0
            elif home == "GER" and away == "CUW":
                h_goals, a_goals = 5, 0
            elif home == "NED" and away == "JPN":
                h_goals, a_goals = 1, 2
            elif home == "ESP" and away == "CPV":
                h_goals, a_goals = 3, 0

            events = make_goal_events(home, away, h_goals, a_goals, rng)
            for e in events:
                scorer_goals[e["player_name"]] = scorer_goals.get(e["player_name"], 0) + 1

            conn.execute(text("""
                UPDATE matches SET
                    status = 'FINISHED',
                    home_score = :hs, away_score = :as_,
                    goal_events = CAST(:ge AS jsonb),
                    last_synced_at = NOW()
                WHERE id = :mid
            """), {"mid": mid, "hs": h_goals, "as_": a_goals, "ge": json.dumps(events)})

        conn.commit()
        print(f"  Updated {len(gs_rows)} group stage matches")

        # ── 2. Update R32 matches ───────────────────────────────────
        print("\n=== Updating R32 matches ===")

        r32_rows = conn.execute(text("""
            SELECT id, kickoff_utc FROM matches
            WHERE stage = 'LAST_32'
            ORDER BY kickoff_utc
        """)).fetchall()

        for i, (row, matchup, result) in enumerate(zip(r32_rows, R32_MATCHUPS, R32_RESULTS)):
            mid = row[0]
            home, away = matchup
            h_goals, a_goals, winner = result

            events = make_goal_events(home, away, h_goals, a_goals, rng)
            for e in events:
                scorer_goals[e["player_name"]] = scorer_goals.get(e["player_name"], 0) + 1

            conn.execute(text("""
                UPDATE matches SET
                    status = 'FINISHED',
                    home_team_code = :home, away_team_code = :away,
                    home_team_name = :hname, away_team_name = :aname,
                    home_score = :hs, away_score = :as_,
                    winner_team_code = :winner,
                    goal_events = CAST(:ge AS jsonb),
                    last_synced_at = NOW()
                WHERE id = :mid
            """), {
                "mid": mid, "home": home, "away": away,
                "hname": TEAM_NAMES[home], "aname": TEAM_NAMES[away],
                "hs": h_goals, "as_": a_goals, "winner": winner,
                "ge": json.dumps(events),
            })

        conn.commit()
        print(f"  Updated {len(r32_rows)} R32 matches")

        # ── 3. Update R16 matches ──────────────────────────────────
        print("\n=== Updating R16 matches ===")

        r16_rows = conn.execute(text("""
            SELECT id, kickoff_utc FROM matches
            WHERE stage = 'LAST_16'
            ORDER BY kickoff_utc
        """)).fetchall()

        for i, (row, matchup) in enumerate(zip(r16_rows, R16_MATCHUPS)):
            mid = row[0]
            home, away = matchup
            result = R16_RESULTS[i]

            if result is not None:
                h_goals, a_goals, winner = result
                events = make_goal_events(home, away, h_goals, a_goals, rng)
                for e in events:
                    scorer_goals[e["player_name"]] = scorer_goals.get(e["player_name"], 0) + 1

                conn.execute(text("""
                    UPDATE matches SET
                        status = 'FINISHED',
                        home_team_code = :home, away_team_code = :away,
                        home_team_name = :hname, away_team_name = :aname,
                        home_score = :hs, away_score = :as_,
                        winner_team_code = :winner,
                        goal_events = CAST(:ge AS jsonb),
                        last_synced_at = NOW()
                    WHERE id = :mid
                """), {
                    "mid": mid, "home": home, "away": away,
                    "hname": TEAM_NAMES[home], "aname": TEAM_NAMES[away],
                    "hs": h_goals, "as_": a_goals, "winner": winner,
                    "ge": json.dumps(events),
                })
            else:
                # Still scheduled — just update teams
                conn.execute(text("""
                    UPDATE matches SET
                        home_team_code = :home, away_team_code = :away,
                        home_team_name = :hname, away_team_name = :aname,
                        last_synced_at = NOW()
                    WHERE id = :mid
                """), {
                    "mid": mid, "home": home, "away": away,
                    "hname": TEAM_NAMES[home], "aname": TEAM_NAMES[away],
                })

        conn.commit()
        print("  Updated R16 matches (4 finished, 4 scheduled)")

        # Print top scorers so far
        top = sorted(scorer_goals.items(), key=lambda x: -x[1])[:10]
        print("\n  Tournament top scorers so far:")
        for name, goals in top:
            print(f"    {name}: {goals} goals")

        # ── 4. Create users ────────────────────────────────────────
        print("\n=== Creating 150 users ===")

        users = generate_users(150, rng)

        # Insert user profiles
        for u in users:
            conn.execute(text("""
                INSERT INTO user_profiles (user_id, display_name, nationality, expected_winner_team_code, created_at, updated_at)
                VALUES (:uid, :name, :nat, :winner, NOW(), NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    nationality = EXCLUDED.nationality,
                    expected_winner_team_code = EXCLUDED.expected_winner_team_code,
                    updated_at = NOW()
            """), {
                "uid": u["user_id"], "name": u["display_name"],
                "nat": u["nationality"], "winner": u["expected_winner_team_code"],
            })

        # Insert app_users_cache
        for u in users:
            conn.execute(text("""
                INSERT INTO app_users_cache (user_id, email, display_name, updated_at)
                VALUES (:uid, :email, :name, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    email = EXCLUDED.email,
                    display_name = EXCLUDED.display_name,
                    updated_at = NOW()
            """), {"uid": u["user_id"], "email": u["email"], "name": u["display_name"]})

        conn.commit()
        print(f"  Created {len(users)} user profiles + cache entries")

        # ── 5. Create tournament predictions ───────────────────────
        print("\n=== Creating tournament predictions ===")

        for u in users:
            scorer_name = u["top_scorer_pick"]
            cc, cn = SCORER_COUNTRY.get(scorer_name, ("?", "Unknown"))
            notes = {
                "top_scorers": [
                    {"player_name": scorer_name, "country_code": cc, "country_name": cn}
                ]
            }
            conn.execute(text("""
                INSERT INTO tournament_predictions (user_id, tournament_winner_team_code, top_scorer_player_name, notes_json, created_at, updated_at)
                VALUES (:uid, :winner, :scorer, CAST(:nj AS jsonb), NOW(), NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    tournament_winner_team_code = EXCLUDED.tournament_winner_team_code,
                    top_scorer_player_name = EXCLUDED.top_scorer_player_name,
                    notes_json = EXCLUDED.notes_json,
                    updated_at = NOW()
            """), {
                "uid": u["user_id"],
                "winner": u["tournament_winner_pick"],
                "scorer": scorer_name,
                "nj": json.dumps(notes),
            })

        conn.commit()
        print(f"  Created {len(users)} tournament predictions")

        # ── 6. Create match predictions ────────────────────────────
        print("\n=== Creating match predictions ===")

        # Get all match IDs
        all_matches = conn.execute(text("""
            SELECT id, home_team_code, away_team_code, home_score, away_score,
                   status, stage
            FROM matches
            ORDER BY kickoff_utc
        """)).fetchall()

        finished_matches = [m for m in all_matches if m[5] == "FINISHED"]
        scheduled_knockout = [m for m in all_matches if m[5] == "SCHEDULED" and m[6] != "GROUP_STAGE"]

        pred_count = 0
        batch_params = []

        for u_idx, u in enumerate(users):
            # Each user has a "skill level" that affects prediction quality
            user_skill = rng.random()

            # All users predicted all finished matches
            for m in finished_matches:
                mid, home, away, h_score, a_score, status, stage = m
                is_ko = stage != "GROUP_STAGE"
                pred = generate_prediction(user_skill, h_score, a_score, home, away, is_ko, rng)

                params = {
                    "uid": u["user_id"],
                    "mid": mid,
                    "hg": pred["home_goals"],
                    "ag": pred["away_goals"],
                    "adv": pred.get("advance_team_code"),
                }
                batch_params.append(params)
                pred_count += 1

            # ~70% of users also predicted upcoming R16 matches
            if rng.random() < 0.70:
                for m in scheduled_knockout:
                    mid, home, away, _, _, status, stage = m
                    if stage == "LAST_16" and home != "?":
                        is_ko = True
                        # For unplayed matches, generate based on strength
                        h_str = TEAM_STRENGTH.get(home, 50)
                        a_str = TEAM_STRENGTH.get(away, 50)
                        h = max(0, min(4, round(rng.gauss(1.2, 0.8))))
                        a = max(0, min(4, round(rng.gauss(1.2, 0.8))))
                        pred = {"home_goals": h, "away_goals": a}
                        if h == a:
                            if rng.random() < h_str / (h_str + a_str):
                                pred["advance_team_code"] = home
                            else:
                                pred["advance_team_code"] = away

                        params = {
                            "uid": u["user_id"],
                            "mid": mid,
                            "hg": pred["home_goals"],
                            "ag": pred["away_goals"],
                            "adv": pred.get("advance_team_code"),
                        }
                        batch_params.append(params)
                        pred_count += 1

            # Insert in batches of 500
            if len(batch_params) >= 500:
                conn.execute(text("""
                    INSERT INTO match_predictions (user_id, match_id, home_goals, away_goals, advance_team_code, created_at, updated_at)
                    VALUES (:uid, :mid, :hg, :ag, :adv, NOW(), NOW())
                    ON CONFLICT (user_id, match_id) DO UPDATE SET
                        home_goals = EXCLUDED.home_goals,
                        away_goals = EXCLUDED.away_goals,
                        advance_team_code = EXCLUDED.advance_team_code,
                        updated_at = NOW()
                """), batch_params)
                conn.commit()
                batch_params = []
                if (u_idx + 1) % 25 == 0:
                    print(f"  Progress: {u_idx + 1}/{len(users)} users, {pred_count} predictions")

        # Final batch
        if batch_params:
            conn.execute(text("""
                INSERT INTO match_predictions (user_id, match_id, home_goals, away_goals, advance_team_code, created_at, updated_at)
                VALUES (:uid, :mid, :hg, :ag, :adv, NOW(), NOW())
                ON CONFLICT (user_id, match_id) DO UPDATE SET
                    home_goals = EXCLUDED.home_goals,
                    away_goals = EXCLUDED.away_goals,
                    advance_team_code = EXCLUDED.advance_team_code,
                    updated_at = NOW()
            """), batch_params)
            conn.commit()

        print(f"  Created {pred_count} match predictions total")

        # ── 7. Summary ─────────────────────────────────────────────
        print("\n=== SIMULATION COMPLETE ===")
        for table in ["matches", "match_predictions", "tournament_predictions", "user_profiles", "app_users_cache"]:
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"  {table}: {cnt} rows")

        # Match status breakdown
        rows = conn.execute(text("""
            SELECT stage, status, COUNT(*) FROM matches
            GROUP BY stage, status ORDER BY stage, status
        """)).fetchall()
        print("\n  Match breakdown:")
        for r in rows:
            print(f"    {r[0]} / {r[1]}: {r[2]}")


if __name__ == "__main__":
    main()
