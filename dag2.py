from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

# Read environment variables
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")
POSTGRES_CONN_STR = os.getenv("POSTGRES_CONN_STR")

# Print masked debug info
print(
    "Debug - "
    f"CLIENT_ID: {'Set' if CLIENT_ID else 'Not Set'}, "
    f"CLIENT_SECRET: {'Set' if CLIENT_SECRET else 'Not Set'}, "
    f"SPOTIFY_REFRESH_TOKEN: {'Set' if SPOTIFY_REFRESH_TOKEN else 'Not Set'}, "
    f"POSTGRES_CONN_STR: {'Set' if POSTGRES_CONN_STR else 'Not Set'}"
)

# Spotify endpoints
TOKEN_URL = "https://accounts.spotify.com/api/token"
RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"
AUDIO_FEATURES_URL = "https://api.spotify.com/v1/audio-features"


def parse_spotify_release_date(release_date_str: str) -> str:
    """
    Convert partial Spotify release dates to a valid 'YYYY-MM-DD' string.
    - '1999' => '1999-01-01'
    - '1999-05' => '1999-05-01'
    - '1999-05-23' => '1999-05-23'
    """
    parts = release_date_str.split("-")
    if len(parts) == 1:
        # Only year
        return f"{release_date_str}-01-01"
    elif len(parts) == 2:
        # Year and month
        return f"{release_date_str}-01"
    else:
        # Already a full YYYY-MM-DD or more
        return release_date_str


def refresh_access_token(**kwargs):
    """Refresh the Spotify access token."""
    print("Debug - Starting refresh_access_token task")
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": SPOTIFY_REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    response = requests.post(TOKEN_URL, data=payload)
    print(f"Debug - Token URL response status: {response.status_code}")
    if response.status_code == 200:
        access_token = response.json()["access_token"]
        # Do not print the actual token, just show a masked snippet or that it's set
        print(f"Debug - Access token retrieved: {'Set' if access_token else 'Not Set'}")
        kwargs['ti'].xcom_push(key="access_token", value=access_token)
    else:
        print(f"Error - Failed to refresh token. Response: {response.text}")
        raise Exception(f"Failed to refresh token. Response: {response.text}")


def get_recently_played(**kwargs):
    """Fetch recently played tracks."""
    print("Debug - Starting get_recently_played task")
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids="refresh_access_token", key="access_token")
    # Do not print the token itself
    print(f"Debug - Using the refreshed access token (masked).")
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"limit": 50}
    response = requests.get(RECENTLY_PLAYED_URL, headers=headers, params=params)
    print(f"Debug - Recently Played URL response status: {response.status_code}")
    if response.status_code == 200:
        ti.xcom_push(key="recently_played", value=response.json())
        print("Debug - Recently played data fetched successfully")
    else:
        print(f"Error - Failed to fetch recently played tracks. Response: {response.text}")
        raise Exception(f"Failed to fetch recently played tracks. Response: {response.text}")


def get_audio_features(access_token, track_id):
    """Fetch audio features for a specific track."""
    print(f"Debug - Fetching audio features for track_id: {track_id}")
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(f"{AUDIO_FEATURES_URL}/{track_id}", headers=headers)
    print(f"Debug - Audio features response status for track_id {track_id}: {response.status_code}")
    return response.json() if response.status_code == 200 else None


def fetch_tracks_data(**kwargs):
    """Process recently played data."""
    print("Debug - Starting fetch_tracks_data task")
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids="refresh_access_token", key="access_token")
    recently_played_data = ti.xcom_pull(task_ids="get_recently_played", key="recently_played")
    items = recently_played_data.get("items", [])
    print(f"Debug - Recently played data retrieved: {len(items)} items")

    table2_data = []
    for item in items:
        track = item["track"]
        audio_features = get_audio_features(access_token, track["id"])

        # Parse the release_date to handle partial dates
        normalized_date = parse_spotify_release_date(track["album"]["release_date"])

        table2_data.append({
            "id": track["id"],
            "name": track["name"],
            "popularity": track.get("popularity"),
            "duration_ms": track["duration_ms"],
            "explicit": track["explicit"],
            "artists": ", ".join(artist["name"] for artist in track["artists"]),
            "release_date": normalized_date,  # Use normalized date
            "danceability": audio_features.get("danceability") if audio_features else None,
            # Add more fields as needed
        })

    print(f"Debug - Total tracks processed: {len(table2_data)}")
    table2_df = pd.DataFrame(table2_data)
    ti.xcom_push(key="tracks_data", value=table2_df.to_dict(orient="records"))


def save_to_postgres(**kwargs):
    """Save tracks data to PostgreSQL."""
    print("Debug - Starting save_to_postgres task")
    ti = kwargs['ti']
    table2_data = ti.xcom_pull(task_ids="fetch_tracks_data", key="tracks_data")
    table2_df = pd.DataFrame(table2_data)

    # Establish connection to PostgreSQL
    print("Debug - Connecting to PostgreSQL using the configured connection string (masked).")
    engine = create_engine(POSTGRES_CONN_STR)

    # Ensure table schema matches
    table_schema = """
    CREATE TABLE IF NOT EXISTS recently_played_spotify_tracks (
        id VARCHAR PRIMARY KEY,
        name VARCHAR,
        popularity INT,
        duration_ms INT,
        explicit BOOLEAN,
        artists TEXT,
        release_date DATE,
        danceability FLOAT
    )
    """
    with engine.connect() as conn:
        conn.execute(table_schema)
        print("Debug - Table schema ensured")

    # Insert data with handling for duplicates
    try:
        with engine.begin() as conn:
            for _, row in table2_df.iterrows():
                insert_query = """
                INSERT INTO recently_played_spotify_tracks (id, name, popularity, duration_ms, explicit, artists, release_date, danceability)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """
                conn.execute(
                    insert_query,
                    (
                        row["id"],
                        row["name"],
                        row["popularity"],
                        row["duration_ms"],
                        row["explicit"],
                        row["artists"],
                        row["release_date"],
                        row["danceability"]
                    )
                )
        print("Debug - Data successfully upserted to PostgreSQL")
    except Exception as e:
        print(f"Error - Failed to save data: {e}")
        raise e


# Define the DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    dag_id="dag2_recently_played_songs_spotify_data_pipeline",
    default_args=default_args,
    description="Fetch Spotify data and store it in PostgreSQL",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    refresh_token_task = PythonOperator(
        task_id="refresh_access_token",
        python_callable=refresh_access_token,
    )

    get_recently_played_task = PythonOperator(
        task_id="get_recently_played",
        python_callable=get_recently_played,
    )

    fetch_tracks_data_task = PythonOperator(
        task_id="fetch_tracks_data",
        python_callable=fetch_tracks_data,
    )

    save_to_postgres_task = PythonOperator(
        task_id="save_to_postgres",
        python_callable=save_to_postgres,
    )

    refresh_token_task >> get_recently_played_task >> fetch_tracks_data_task >> save_to_postgres_task
