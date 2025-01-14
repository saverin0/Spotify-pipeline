from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
POSTGRES_CONN_STR = os.getenv("POSTGRES_CONN_STR")
DATA_DIR = os.getenv("DATA_DIR", "./data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(DATA_DIR, "output"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Utility function to standardize release dates
def standardize_release_dates(df, column):
    """Standardize release dates in the DataFrame."""
    mask_year_only = df[column].str.match(r'^\d{4}$', na=False)
    df.loc[mask_year_only, column] += '-01-01'
    mask_year_month = df[column].str.match(r'^\d{4}-\d{2}$', na=False)
    df.loc[mask_year_month, column] += '-01'
    df[column] = pd.to_datetime(df[column], errors='coerce')
    return df

# Task 1: Load and clean artists
def load_and_clean_artists():
    """
    1. Read artists.csv
    2. Clean the 'genres' column
    3. Insert into raw_artists table in chunks
    4. Remove old clean_artists.csv if it exists, then write the new cleaned file
    """
    file_path = os.path.join(DATA_DIR, "artists.csv")
    logger.info(f"Reading artists data from {file_path} ...")
    df_artists = pd.read_csv(file_path)

    # Clean up the 'genres' column by removing all square brackets and extraneous quotes
    logger.info("Cleaning up 'genres' column in artists data...")
    df_artists['genres'] = (
        df_artists['genres']
        .str.replace(r'[\[\]]', '', regex=True)  # Remove all square brackets
        .str.replace("'", "")                     # Remove single quotes
        .str.replace('"', '')                     # Remove double quotes
    )

    # Connect to database and insert in chunks
    logger.info("Saving cleaned artists to raw_artists table in the database...")
    engine = create_engine(POSTGRES_CONN_STR)
    chunksize = 10000  # Define chunk size for database insertion
    with engine.connect() as conn:
        for i, start_idx in enumerate(range(0, len(df_artists), chunksize)):
            chunk_df = df_artists.iloc[start_idx : start_idx + chunksize]
            chunk_df.to_sql(
                "raw_artists",
                conn,
                if_exists="replace" if i == 0 else "append",
                index=False
            )
    logger.info("Cleaned artists saved to raw_artists table successfully.")

    # Remove old clean_artists.csv if it exists, then write the new one
    clean_artists_path = os.path.join(OUTPUT_DIR, "clean_artists.csv")
    if os.path.exists(clean_artists_path):
        logger.info(f"Removing old file at {clean_artists_path} ...")
        os.remove(clean_artists_path)

    logger.info(f"Writing new clean_artists.csv to {clean_artists_path} ...")
    df_artists.to_csv(clean_artists_path, index=False)
    logger.info("clean_artists.csv created successfully.")

# Task 2a: Standardize and clean track data
def standardize_and_clean_tracks():
    """
    1. Read tracks.csv
    2. Standardize 'release_date'
    3. Clean 'id_artists' and 'artists' columns
    4. Return cleaned DataFrame
    """
    file_path = os.path.join(DATA_DIR, "tracks.csv")
    logger.info(f"Reading tracks data from {file_path} ...")
    df_tracks = pd.read_csv(file_path)

    # Standardize release_date column
    logger.info("Standardizing release_date column...")
    df_tracks = standardize_release_dates(df_tracks, 'release_date')

    # Clean `id_artists` column: remove all square brackets and quotes, then split if needed
    logger.info("Cleaning 'id_artists' column...")
    df_tracks['id_artists'] = (
        df_tracks['id_artists']
        .str.replace(r'[\[\]]', '', regex=True)
        .str.replace("'", "")
        .str.replace('"', '')
        .str.split(", ")
        .str[0]
    )

    # Clean `artists` column: remove all square brackets and quotes, then split if needed
    logger.info("Cleaning 'artists' column...")
    df_tracks['artists'] = (
        df_tracks['artists']
        .str.replace(r'[\[\]]', '', regex=True)
        .str.replace("'", "")
        .str.replace('"', '')
        .str.split(", ")
        .str[0]
    )

    logger.info("Standardization and cleaning of tracks completed.")
    return df_tracks

# Task 2b: Merge tracks with artists
def merge_tracks_with_artists():
    """
    1. Standardize and clean tracks data
    2. Read the newly generated clean_artists.csv
    3. Merge tracks with artists on 'id_artists' = 'id'
    4. Write merged tracks to clean_tracks.csv in chunks
    """
    logger.info("Starting merge_tracks_with_artists task...")
    df_tracks = standardize_and_clean_tracks()

    # Load cleaned artists from CSV
    artists_path = os.path.join(OUTPUT_DIR, "clean_artists.csv")
    logger.info(f"Reading cleaned artists data from {artists_path} ...")
    df_artists = pd.read_csv(artists_path)

    # Merge
    logger.info("Merging tracks with artists...")
    df_merged = pd.merge(
        df_tracks,
        df_artists[['id', 'genres']],
        left_on='id_artists',
        right_on='id',
        how='left'
    ).drop(columns=['id_y']).rename(columns={'id_x': 'id'})

    # Save merged tracks to CSV in chunks
    output_path = os.path.join(OUTPUT_DIR, "clean_tracks.csv")
    logger.info(f"Saving merged tracks to CSV in chunks at {output_path}...")
    chunksize = 10000  # Define chunk size for saving CSV
    with open(output_path, mode='w', newline='') as f:
        for i, start_idx in enumerate(range(0, len(df_merged), chunksize)):
            chunk_df = df_merged.iloc[start_idx : start_idx + chunksize]
            if i == 0:
                chunk_df.to_csv(f, index=False)
            else:
                chunk_df.to_csv(f, index=False, header=False)

    logger.info("Merged tracks saved as CSV successfully.")

# Task 3: Load data into database
def load_data_to_database():
    """
    1. Read the final clean_tracks.csv in chunks
    2. Insert into raw_tracks table in Postgres
    """
    logger.info("Starting load_data_to_database task...")
    engine = create_engine(POSTGRES_CONN_STR)
    tracks_path = os.path.join(OUTPUT_DIR, "clean_tracks.csv")

    logger.info("Loading raw_tracks data from CSV to the database in chunks...")
    chunksize = 10000
    with engine.connect() as conn:
        for chunk in pd.read_csv(tracks_path, chunksize=chunksize):
            chunk.to_sql("raw_tracks", conn, if_exists="replace", index=False)
    logger.info("raw_tracks data loaded to the database successfully.")

# Define default_args and DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag1_spotify_etl_pipeline',
    default_args=default_args,
    description='Spotify ETL Pipeline with Chunked Saving',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    # Task 1: Load and clean artists
    task_load_artists = PythonOperator(
        task_id='load_and_clean_artists',
        python_callable=load_and_clean_artists,
    )

    # Task 2: Merge tracks with artists
    task_merge_tracks_with_artists = PythonOperator(
        task_id='merge_tracks_with_artists',
        python_callable=merge_tracks_with_artists,
    )

    # Task 3: Load raw_tracks data into database
    task_load_data_to_database = PythonOperator(
        task_id='load_data_to_database',
        python_callable=load_data_to_database,
    )

    # Define task dependencies
    task_load_artists >> task_merge_tracks_with_artists >> task_load_data_to_database
