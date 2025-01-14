import pandas as pd
import os
import logging
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
POSTGRES_CONN_STR = os.getenv("POSTGRES_CONN_STR")
DATA_DIR = os.getenv("DATA_DIR", "./data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(DATA_DIR, "output"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def clear_analysis_csvs(directory_path):
    """
    Remove only the specific CSV files created by this DAG.
    """
    csv_files_to_delete = [
        "validated_tracks.csv",
        "songs_released_per_year.csv",
        "genre_decade_popularity.csv"
    ]
    for filename in csv_files_to_delete:
        file_path = os.path.join(directory_path, filename)
        if os.path.exists(file_path):
            logger.info(f"Deleting existing file: {file_path}")
            os.remove(file_path)

# Task 1: Fetch and validate data
def fetch_and_validate_data():
    """Fetch data from the database and validate required columns."""
    logger.info("Starting task: fetch_and_validate_data")
    try:
        # Clear only the CSVs we plan to recreate in this DAG
        clear_analysis_csvs(OUTPUT_DIR)

        with create_engine(POSTGRES_CONN_STR).connect() as connection:
            logger.info("Fetching data from 'raw_tracks' table...")
            df_tracks = pd.read_sql("SELECT * FROM raw_tracks", connection)

        # Validate required columns
        required_columns = ['release_date', 'genres', 'popularity']
        missing_columns = [col for col in required_columns if col not in df_tracks.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Convert release_date to datetime
        logger.info("Validating and processing release_date column...")
        df_tracks['release_date'] = pd.to_datetime(df_tracks['release_date'], errors='coerce')

        # Save validated data
        validated_path = os.path.join(OUTPUT_DIR, "validated_tracks.csv")
        df_tracks.to_csv(validated_path, index=False)
        logger.info(f"Data fetched and validated successfully. Saved to {validated_path}")

    except Exception as e:
        logger.error(f"Error in fetch_and_validate_data: {e}")
        raise e

# Task 2: Analyze songs per year
def analyze_songs_per_year():
    """Analyze the number of songs released per year."""
    logger.info("Starting task: analyze_songs_per_year")
    try:
        file_path = os.path.join(OUTPUT_DIR, "validated_tracks.csv")
        df_tracks = pd.read_csv(file_path)

        # Ensure release_date is in datetime format
        logger.info("Converting release_date column to datetime...")
        df_tracks['release_date'] = pd.to_datetime(df_tracks['release_date'], errors='coerce')

        # Extract year and calculate songs per year
        logger.info("Calculating number of songs released per year...")
        df_tracks['year'] = df_tracks['release_date'].dt.year
        songs_per_year = df_tracks['year'].value_counts().sort_index()
        songs_per_year_df = songs_per_year.reset_index()
        songs_per_year_df.columns = ['Year', 'Number_of_Songs']

        # Save results
        yearly_csv_path = os.path.join(OUTPUT_DIR, "songs_released_per_year.csv")
        songs_per_year_df.to_csv(yearly_csv_path, index=False)
        logger.info(f"Songs per year analysis saved to {yearly_csv_path}")

    except Exception as e:
        logger.error(f"Error in analyze_songs_per_year: {e}")
        raise e

# Task 3: Analyze genre and decade popularity
def analyze_genre_decade_popularity():
    """Analyze average popularity by genre and decade."""
    logger.info("Starting task: analyze_genre_decade_popularity")
    try:
        file_path = os.path.join(OUTPUT_DIR, "validated_tracks.csv")
        df_tracks = pd.read_csv(file_path)

        # Extract decade
        logger.info("Processing decade and genre columns...")
        df_tracks['year'] = pd.to_datetime(df_tracks['release_date'], errors='coerce').dt.year
        df_tracks['decade'] = (df_tracks['year'] // 10) * 10

        # Parse and explode genres
        logger.info("Parsing and exploding genres column...")

        def parse_genres(value):
            """Split comma-separated genres into a list."""
            if not isinstance(value, str):
                return []
            return [genre.strip() for genre in value.split(',') if genre.strip()]

        df_tracks['genres'] = df_tracks['genres'].apply(parse_genres)
        df_tracks_exploded = df_tracks.explode('genres').dropna(subset=['genres'])

        # Calculate average popularity by genre and decade
        logger.info("Calculating average popularity by genre and decade...")
        genre_decade_popularity = (
            df_tracks_exploded.groupby(['genres', 'decade'])['popularity']
            .mean()
            .reset_index()
        )

        # Save results
        genre_csv_path = os.path.join(OUTPUT_DIR, "genre_decade_popularity.csv")
        genre_decade_popularity.to_csv(genre_csv_path, index=False)
        logger.info(f"Genre and decade popularity analysis saved to {genre_csv_path}")

    except Exception as e:
        logger.error(f"Error in analyze_genre_decade_popularity: {e}")
        raise e


# Airflow default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'spotify_analysis_q1',
    default_args=default_args,
    description='Spotify Data Analysis DAG',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Fetch and validate data
    task_fetch_and_validate_data = PythonOperator(
        task_id='fetch_and_validate_data',
        python_callable=fetch_and_validate_data,
    )

    # Task 2: Analyze songs per year
    task_analyze_songs_per_year = PythonOperator(
        task_id='analyze_songs_per_year',
        python_callable=analyze_songs_per_year,
    )

    # Task 3: Analyze genre and decade popularity
    task_analyze_genre_decade_popularity = PythonOperator(
        task_id='analyze_genre_decade_popularity',
        python_callable=analyze_genre_decade_popularity,
    )

    # Task dependencies
    task_fetch_and_validate_data >> [task_analyze_songs_per_year, task_analyze_genre_decade_popularity]
