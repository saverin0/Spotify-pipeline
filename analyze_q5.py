import pandas as pd
import os
import logging
import numpy as np
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------------------------
load_dotenv()
POSTGRES_CONN_STR = os.getenv("POSTGRES_CONN_STR")
DATA_DIR = os.getenv("DATA_DIR", "./data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(DATA_DIR, "output"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------------------------------------------------------------------------
# Configure logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Optimized Task: Generate song recommendations based on text similarity
# ------------------------------------------------------------------------------
# You can manually change the number of recently played songs to process
# by setting NUM_RECENT_SONGS. For instance, to process all songs, set it to None.
# To limit to a fixed number (e.g., 5), change it to 5.
NUM_RECENT_SONGS = None  # Set to None to process all recently played songs.

def analyze_q5():
    """
    Generate song recommendations based on textual similarity, optimized for large datasets.
    
    For each song in the 'recently_played_spotify_tracks' table (or a subset if NUM_RECENT_SONGS
    is set to a numeric value), this function:
      - Deletes any old recommendations CSV.
      - Streams candidate songs from the 'raw_tracks' table in chunks.
      - For each candidate song, creates a combined text using the song's name and artists.
      - Uses CountVectorizer to vectorize both the recent song's text and the candidate songs' texts.
      - Computes cosine similarity scores between the recent song and the candidates.
      - Aggregates candidates from all chunks and selects the top 5 recommendations (ignoring those with a similarity score of 0).
      - Saves the final recommendations to a CSV file.
      
    This approach uses textual metadata provided by the API (name and artists) since
    the audio features have been deprecated.
    """
    try:
        logger.info("Starting task: analyze_q5")
        engine = create_engine(POSTGRES_CONN_STR)
        
        # Delete an existing recommendations CSV file if it exists
        rec_file = os.path.join(OUTPUT_DIR, "recommendations.csv")
        if os.path.exists(rec_file):
            os.remove(rec_file)
            logger.info(f"Deleted existing recommendations file: {rec_file}")
        else:
            logger.info(f"No existing recommendations file found at: {rec_file}")
        
        # Fetch recently played songs from the database
        logger.info("Fetching data from 'recently_played_spotify_tracks' table...")
        recently_played = pd.read_sql("SELECT * FROM recently_played_spotify_tracks", engine)
        if recently_played.empty:
            logger.warning("No recently played songs available.")
            return
        
        # Option: Limit processing if NUM_RECENT_SONGS is not None.
        if NUM_RECENT_SONGS is not None:
            if 'played_at' in recently_played.columns:
                recently_played = recently_played.sort_values(by='played_at', ascending=False).head(NUM_RECENT_SONGS)
            else:
                recently_played = recently_played.tail(NUM_RECENT_SONGS)
        # If NUM_RECENT_SONGS is None, all songs are processed.
        
        recommendations = []
        chunksize = 10000  # Adjust chunk size as needed
        
        # Process each recently played song
        for idx, recent_song in recently_played.iterrows():
            recent_song_id = recent_song['id']
            # Build combined text for the recently played song using its 'name' and 'artists'
            if 'name' not in recent_song or 'artists' not in recent_song:
                logger.warning(f"Skipping song {recent_song_id} due to missing text data.")
                continue
            recent_text = str(recent_song['name']) + " " + str(recent_song['artists'])
            
            candidate_recs = []  # To store candidate DataFrames from each chunk
            query = "SELECT * FROM raw_tracks"
            
            # Stream the raw_tracks table in chunks.
            for chunk in pd.read_sql_query(query, engine, chunksize=chunksize):
                # Exclude the recent song from candidate set (if it appears)
                chunk = chunk[chunk['id'] != recent_song_id].copy()
                if chunk.empty:
                    continue
                # Check that the candidate songs have 'name' and 'artists' columns
                if 'name' not in chunk.columns or 'artists' not in chunk.columns:
                    continue
                # Build a combined text column for candidate songs.
                chunk['combined_text'] = chunk['name'].fillna('') + " " + chunk['artists'].fillna('')
                # Fit the vectorizer on the candidate texts
                vectorizer = CountVectorizer(stop_words='english')
                candidate_vectors = vectorizer.fit_transform(chunk['combined_text'])
                # Transform the recent song text using the same vectorizer
                recent_vector = vectorizer.transform([recent_text])
                # Compute cosine similarity between the recent song and candidate songs
                sim_scores = cosine_similarity(recent_vector, candidate_vectors)[0]
                chunk['similarity'] = sim_scores
                candidate_recs.append(chunk)
            
            if candidate_recs:
                # Aggregate candidate songs from all chunks
                candidates_all = pd.concat(candidate_recs, ignore_index=True)
                # Filter out candidates with similarity of 0
                candidates_all = candidates_all[candidates_all['similarity'] > 0]
                if candidates_all.empty:
                    logger.info(f"Skipping song {recent_song_id} as no candidate has a non-zero similarity.")
                    continue
                # Sort candidates by similarity (descending)
                sorted_candidates = candidates_all.sort_values(by='similarity', ascending=False)
                num_recs = min(5, len(sorted_candidates))
                top_overall = sorted_candidates.head(num_recs)
                
                for _, song in top_overall.iterrows():
                    recommendations.append({
                        'based_on_song_id': recent_song_id,
                        'based_on_song_name': recent_song.get('name', None),
                        'recommended_song_id': song['id'],
                        'recommended_song_name': song.get('name', None),
                        'recommended_artist': song.get('artists', None),
                        'similarity_score': song['similarity']
                    })
            else:
                logger.warning(f"No candidate recommendations generated for song ID {recent_song_id}.")
        
        if not recommendations:
            logger.warning("No recommendations generated for any recently played song.")
            return
        
        # Save recommendations to CSV
        logger.info("Saving recommendations to CSV...")
        recs_df = pd.DataFrame(recommendations)
        recommendations_path = os.path.join(OUTPUT_DIR, "recommendations.csv")
        recs_df.to_csv(recommendations_path, index=False)
        logger.info(f"Recommendations saved to {recommendations_path}")
        
        # Optional: Save recommendations to the database for Power BI.
        # with create_engine(POSTGRES_CONN_STR).connect() as connection:
        #     recs_df.to_sql("recommendations", connection, if_exists="replace", index=False)
        #     logger.info("Recommendations saved to the database successfully.")
        
    except Exception as e:
        logger.error(f"Error in analyze_q5: {e}")
        raise e

# ------------------------------------------------------------------------------
# Airflow default arguments
# ------------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ------------------------------------------------------------------------------
# Define the DAG
# ------------------------------------------------------------------------------
with DAG(
    'spotify_analysis_q5',
    default_args=default_args,
    description='Spotify Data Analysis DAG - Optimized Song Recommendations for Large Datasets (Text-Based)',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    task_analyze_q5 = PythonOperator(
        task_id='analyze_q5',
        python_callable=analyze_q5,
    )
    
    task_analyze_q5
