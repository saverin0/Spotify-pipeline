import pandas as pd
import os
import ast
import logging
from sqlalchemy import create_engine
from sklearn.feature_extraction.text import CountVectorizer
from prophet import Prophet
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

def analyze_q4():
    """Analyze seasonal keywords and forecast popularity trends."""
    try:
        logger.info("Starting task: analyze_q4")
        
        # Delete old CSV files if they exist.
        files_to_delete = [
            os.path.join(OUTPUT_DIR, "seasonal_keywords.csv"),
            os.path.join(OUTPUT_DIR, "popularity_forecast.csv")
        ]
        for file_path in files_to_delete:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted old CSV file: {file_path}")
            else:
                logger.info(f"No old CSV file found at: {file_path}")
        
        logger.info("Connecting to the database...")
        engine = create_engine(POSTGRES_CONN_STR)
        df_tracks = pd.read_sql("SELECT * FROM raw_tracks", engine)

        # Part 1: Seasonal Keywords in Song Titles
        logger.info("Analyzing seasonal keywords in song titles...")
        if 'name' not in df_tracks.columns or df_tracks['name'].isnull().all():
            logger.warning("No valid song names found for vectorization.")
        else:
            df_tracks['name'] = df_tracks['name'].fillna("")
            vectorizer = CountVectorizer(stop_words='english', ngram_range=(1, 2))
            title_matrix = vectorizer.fit_transform(df_tracks['name'])
            keyword_counts = pd.Series(
                title_matrix.sum(axis=0).A1, 
                index=vectorizer.get_feature_names_out()
            ).sort_values(ascending=False)

            seasonal_keywords = keyword_counts[keyword_counts.index.str.contains("holiday|christmas|summer|winter", case=False)]
            seasonal_keywords_df = seasonal_keywords.reset_index()
            seasonal_keywords_df.columns = ['Keyword', 'Count']

            # Save seasonal keywords to CSV
            seasonal_csv_path = os.path.join(OUTPUT_DIR, "seasonal_keywords.csv")
            seasonal_keywords_df.to_csv(seasonal_csv_path, index=False)
            logger.info(f"Seasonal keywords saved to {seasonal_csv_path}")

        # Part 2: Popularity Trend Forecasting
        logger.info("Forecasting popularity trends...")
        df_tracks['year_month'] = pd.to_datetime(df_tracks['release_date'], errors='coerce').dt.to_period('M')
        df_prophet = df_tracks.groupby('year_month')['popularity'].mean().reset_index()
        df_prophet.columns = ['ds', 'y']  # Prophet requires 'ds' for date and 'y' for values
        df_prophet['ds'] = df_prophet['ds'].dt.to_timestamp()

        # Remove outliers
        q1 = df_prophet['y'].quantile(0.25)
        q3 = df_prophet['y'].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df_prophet = df_prophet[(df_prophet['y'] >= lower_bound) & (df_prophet['y'] <= upper_bound)]

        logger.info("Training Prophet model...")
        model = Prophet(yearly_seasonality=True)
        model.fit(df_prophet)

        logger.info("Making future predictions...")
        future = model.make_future_dataframe(periods=12, freq='M')
        forecast = model.predict(future)

        # Export forecast data to CSV
        forecast_csv_path = os.path.join(OUTPUT_DIR, "popularity_forecast.csv")
        forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv(forecast_csv_path, index=False)
        logger.info(f"Popularity forecast saved to {forecast_csv_path}")

    except Exception as e:
        logger.error(f"Error in analyze_q4: {e}")
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
    'spotify_analysis_q4',
    default_args=default_args,
    description='Spotify Data Analysis DAG - Seasonal Keywords and Popularity Forecasting',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    # Task: Analyze Q4
    task_analyze_q4 = PythonOperator(
        task_id='analyze_q4',
        python_callable=analyze_q4,
    )

    task_analyze_q4
