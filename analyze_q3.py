import os
import ast
import time
import logging
import pandas as pd
import statsmodels.api as sm
from sqlalchemy import create_engine
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------------------------
load_dotenv()
POSTGRES_CONN_STR = os.getenv("POSTGRES_CONN_STR")
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
DATA_DIR = os.getenv("DATA_DIR", "./data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(DATA_DIR, "output"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------------------------------------------------------------------------
# Configure logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Initialize Spotipy client
# ------------------------------------------------------------------------------
spotify = Spotify(
    client_credentials_manager=SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET
    )
)

# ------------------------------------------------------------------------------
# Simplified safe_literal_eval function (no extra cleaning for unquoted items)
# ------------------------------------------------------------------------------
def safe_literal_eval(value):
    """
    Convert a value to a list.
    
    - If the value is a list-like string, use ast.literal_eval.
    - If it's a plain string, return it as a one-item list.
    - Otherwise, return an empty list.
    On error, log a warning and return an empty list.
    """
    try:
        if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
            return ast.literal_eval(value)
        elif isinstance(value, str):
            return [value]
        else:
            return []
    except Exception as e:
        logger.warning(f"Failed to parse value: {value}. Error: {e}")
        return []

# ------------------------------------------------------------------------------
# Task: Delete specific old CSV files produced by this DAG from OUTPUT_DIR
# ------------------------------------------------------------------------------
def delete_old_outputs(**kwargs):
    """
    Delete only the specific CSV files created by this DAG:
      - artist_statistics_dataset.csv
      - top_20_artist_statistics_enriched.csv
      - clean_exploded_dataset.csv
      - popularity_comparison.csv
    """
    try:
        logger.info("Starting task: delete_old_outputs")
        files_to_delete = [
            "artist_statistics_dataset.csv",
            "top_20_artist_statistics_enriched.csv",
            "clean_exploded_dataset.csv",
            "popularity_comparison.csv"
        ]
        deleted_any = False

        for filename in files_to_delete:
            file_path = os.path.join(OUTPUT_DIR, filename)
            if os.path.exists(file_path):
                os.remove(file_path)
                deleted_any = True
                logger.info(f"Deleted old CSV file: {file_path}")

        if not deleted_any:
            logger.info("No CSV files found to delete.")
        logger.info("Old CSV file deletion process complete.")
    except Exception as e:
        logger.error(f"Error deleting old CSV files: {e}")
        raise e

# ------------------------------------------------------------------------------
# Task 1: Analyze dataset in chunks
# ------------------------------------------------------------------------------
def analyze_dataset():
    """
    Process the raw_tracks data in chunks:
      - Clean and explode the artists columns.
      - Compute aggregated artist statistics.
      - Export two CSV files:
          1. Aggregated artist statistics (artist_statistics_dataset.csv)
          2. Full cleaned exploded dataset (clean_exploded_dataset.csv) for overall analysis.
    """
    try:
        logger.info("Starting task: analyze_dataset")
        if not POSTGRES_CONN_STR:
            raise ValueError("POSTGRES_CONN_STR environment variable is not set.")

        required_columns = ['artists', 'popularity', 'id_artists']
        query = "SELECT * FROM raw_tracks"
        chunk_size = 50000  # Adjust based on your environment
        group_list = []      # For aggregated artist stats (per chunk)
        exploded_list = []   # For full exploded dataset

        with create_engine(POSTGRES_CONN_STR).connect() as connection:
            for i, chunk in enumerate(pd.read_sql_query(query, connection, chunksize=chunk_size), start=1):
                logger.info(f"Processing chunk {i}")
                missing_columns = [col for col in required_columns if col not in chunk.columns]
                if missing_columns:
                    raise ValueError(f"Missing required columns in chunk {i}: {missing_columns}")

                # Clean the list columns using the simplified function.
                chunk['artists'] = chunk['artists'].apply(safe_literal_eval)
                chunk['id_artists'] = chunk['id_artists'].apply(safe_literal_eval)

                # Explode to have one row per artist.
                exploded = chunk.explode(['artists', 'id_artists']).dropna(subset=['artists', 'id_artists'])
                exploded['artists'] = exploded['artists'].str.strip()

                # Save exploded data for overall analysis.
                exploded_list.append(exploded)

                # Compute chunk-level aggregated stats.
                grouped = exploded.groupby(['id_artists', 'artists']).agg(
                    total_popularity=('popularity', 'sum'),
                    song_count=('popularity', 'count')
                ).reset_index()
                grouped['avg_popularity'] = grouped['total_popularity'] / grouped['song_count']
                group_list.append(grouped)

        if not group_list:
            raise ValueError("No data processed in chunks.")

        # Combine partial results.
        combined = pd.concat(group_list, ignore_index=True)
        artist_stats = combined.groupby(['id_artists', 'artists']).agg(
            total_popularity=('total_popularity', 'sum'),
            song_count=('song_count', 'sum')
        ).reset_index()
        artist_stats['avg_popularity'] = artist_stats['total_popularity'] / artist_stats['song_count']

        # Export aggregated artist stats.
        artist_stats_path = os.path.join(OUTPUT_DIR, "artist_statistics_dataset.csv")
        artist_stats.to_csv(artist_stats_path, index=False)
        logger.info(f"Artist statistics saved to {artist_stats_path}")

        # Optional: Uncomment to save to the database for Power BI.
        # with create_engine(POSTGRES_CONN_STR).connect() as connection:
        #     artist_stats.to_sql('artist_statistics_dataset', connection, if_exists='replace', index=False)
        #     logger.info("Artist statistics saved to database table: artist_statistics_dataset")

        # Export full cleaned exploded dataset.
        exploded_full = pd.concat(exploded_list, ignore_index=True)
        exploded_path = os.path.join(OUTPUT_DIR, "clean_exploded_dataset.csv")
        exploded_full.to_csv(exploded_path, index=False)
        logger.info(f"Clean exploded dataset saved to {exploded_path}")

    except Exception as e:
        logger.error(f"Error in analyze_dataset: {e}")
        raise e

# ------------------------------------------------------------------------------
# Task 2: Fetch artist details from Spotify API for top 20 artists
# ------------------------------------------------------------------------------
def fetch_artist_details_from_api():
    """
    Select top 20 artists based on total popularity from the aggregated data,
    fetch additional details from Spotify API, and export an enriched CSV.
    """
    try:
        logger.info("Starting task: fetch_artist_details_from_api")
        artist_stats_path = os.path.join(OUTPUT_DIR, "artist_statistics_dataset.csv")
        if not os.path.exists(artist_stats_path):
            raise FileNotFoundError(f"Artist statistics file not found: {artist_stats_path}")

        artist_stats = pd.read_csv(artist_stats_path)

        # Select top 20 artists based on total_popularity.
        logger.info("Selecting top 20 artists based on total_popularity...")
        top_artists = artist_stats.sort_values(by='total_popularity', ascending=False).head(20)
        logger.info(f"Found {len(top_artists)} top artists. Fetching additional details from Spotify API...")

        spotify_data = []
        for artist_id in top_artists['id_artists']:
            try:
                artist_info = spotify.artist(artist_id)
                spotify_data.append({
                    'id_artists': artist_id,
                    'genres': ", ".join(artist_info.get('genres', [])),
                    'followers': artist_info.get('followers', {}).get('total', 0),
                    'spotify_popularity': artist_info.get('popularity', 0)
                })
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error fetching data for artist ID {artist_id}: {e}")
                spotify_data.append({
                    'id_artists': artist_id,
                    'genres': '',
                    'followers': 0,
                    'spotify_popularity': 0
                })

        df_spotify = pd.DataFrame(spotify_data)
        enriched_artist_stats = pd.merge(top_artists, df_spotify, on='id_artists', how='left')

        enriched_artist_stats_path = os.path.join(OUTPUT_DIR, "top_20_artist_statistics_enriched.csv")
        enriched_artist_stats.to_csv(enriched_artist_stats_path, index=False)
        logger.info(f"Enriched artist statistics for top 20 artists saved to {enriched_artist_stats_path}")

        # Optional: Uncomment to save to the database for Power BI.
        # with create_engine(POSTGRES_CONN_STR).connect() as connection:
        #     enriched_artist_stats.to_sql('top_20_artist_statistics_enriched', connection, if_exists='replace', index=False)
        #     logger.info("Top 20 artist enriched statistics saved to database table: top_20_artist_statistics_enriched")
        
    except Exception as e:
        logger.error(f"Error in fetch_artist_details_from_api: {e}")
        raise e

# ------------------------------------------------------------------------------
# Task 3: Export comparison CSV for visualization in Power BI
# ------------------------------------------------------------------------------
def export_comparison_csv():
    """
    Create a summary CSV comparing the overall average popularity (from the full exploded dataset)
    with the weighted average popularity of songs by the top 20 artists.
    """
    try:
        logger.info("Starting task: export_comparison_csv")
        exploded_path = os.path.join(OUTPUT_DIR, "clean_exploded_dataset.csv")
        enriched_artist_stats_path = os.path.join(OUTPUT_DIR, "top_20_artist_statistics_enriched.csv")
        
        if not os.path.exists(exploded_path):
            raise FileNotFoundError(f"Clean exploded dataset not found: {exploded_path}")
        if not os.path.exists(enriched_artist_stats_path):
            raise FileNotFoundError(f"Top artist enriched dataset not found: {enriched_artist_stats_path}")

        # Overall dataset metrics.
        df_exploded = pd.read_csv(exploded_path)
        overall_avg = df_exploded['popularity'].mean()
        overall_count = len(df_exploded)
        
        # Top artists metrics from aggregated enriched dataset.
        df_top = pd.read_csv(enriched_artist_stats_path)
        total_pop = df_top['total_popularity'].sum()
        total_songs = df_top['song_count'].sum()
        top_weighted_avg = total_pop / total_songs if total_songs > 0 else 0

        summary_df = pd.DataFrame({
            'group': ['Overall Songs', 'Top 20 Artists'],
            'avg_popularity': [overall_avg, top_weighted_avg],
            'song_count': [overall_count, total_songs]
        })
        comparison_csv_path = os.path.join(OUTPUT_DIR, "popularity_comparison.csv")
        summary_df.to_csv(comparison_csv_path, index=False)
        logger.info(f"Comparison CSV saved to {comparison_csv_path}")

        # Optional: Uncomment to save to the database for Power BI.
        # with create_engine(POSTGRES_CONN_STR).connect() as connection:
        #     summary_df.to_sql('popularity_comparison', connection, if_exists='replace', index=False)
        #     logger.info("Comparison data saved to database table: popularity_comparison")

    except Exception as e:
        logger.error(f"Error in export_comparison_csv: {e}")
        raise e

# ------------------------------------------------------------------------------
# Airflow DAG Setup
# ------------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spotify_analysis_q3',
    default_args=default_args,
    description='Spotify Data Analysis DAG with Chunked Processing and CSV Exports for Visualization in Power BI',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_delete_old_outputs = PythonOperator(
        task_id='delete_old_outputs',
        python_callable=delete_old_outputs,
    )

    task_analyze_dataset = PythonOperator(
        task_id='analyze_dataset',
        python_callable=analyze_dataset,
    )

    task_fetch_artist_details_from_api = PythonOperator(
        task_id='fetch_artist_details_from_api',
        python_callable=fetch_artist_details_from_api,
    )

    task_export_comparison_csv = PythonOperator(
        task_id='export_comparison_csv',
        python_callable=export_comparison_csv,
    )

    # Define task dependencies.
    task_delete_old_outputs >> task_analyze_dataset >> task_fetch_artist_details_from_api >> task_export_comparison_csv
