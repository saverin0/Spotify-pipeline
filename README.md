# Spotify Genre Trends Pipeline

This repository contains an automated data pipeline for analyzing Spotify music data. The project leverages **Apache Airflow** for workflow orchestration, **PostgreSQL** for data storage, and **Python** for advanced analytics. The pipeline integrates data from CSV files and the Spotify Web API to provide insights into genre trends, artist popularity, and personalized music recommendations.

## Project Overview

The project aims to:
- Analyze genre popularity across decades.
- Correlate audio features (e.g., tempo, energy) with song popularity.
- Study artist influence on track popularity.
- Identify seasonal trends in music.
- Provide personalized music recommendations using textual metadata.

## Features

- **Automated ETL Pipelines**: Built using Apache Airflow to clean, transform, and analyze data.
- **Advanced Analytics**: Python and Power BI are used for data visualization and trend analysis.
- **Scalable Architecture**: Designed to handle large datasets and real-time data processing.
- **Predictive Recommendations**: Suggests songs based on user preferences and metadata similarity.

## Dataset and Resources

- **Dataset**: [Spotify Dataset on Kaggle](https://www.kaggle.com/datasets/lehaknarnauli/spotify-datasets)
- **Spotify Web API**: [Documentation](https://developer.spotify.com/documentation/web-api)
- **Project Video**: [Watch the Demo](https://drive.google.com/file/d/1T7sub6wVUnwbJgWAB37TS0gnRIZzPy70/view?usp=sharing)

## Repository Structure

- **DAGs**: Contains the Directed Acyclic Graphs (DAGs) for the ETL pipeline.
- **SQL Queries**: Includes SQL scripts for calculating task success rates and other metrics.
- **Notebooks**: Jupyter notebooks for exploratory data analysis and visualization.
- **Scripts**: Python scripts for data processing and API integration.

## How to Run

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/your-repository-name.git
   ```
2. Set up the environment:
   - Install dependencies using `requirements.txt`.
   - Configure Apache Airflow and PostgreSQL.
3. Run the DAGs in Apache Airflow to execute the pipeline.

## Results

- **Task Success Rate**: 100% for all DAGs.
  - SQL Query for Task Success Rate (specific DAG):
    ```sql
    SELECT 
        dag_id, 
        COUNT(CASE WHEN state = 'success' THEN 1 END) AS successful_tasks,
        COUNT(*) AS total_tasks,
        (COUNT(CASE WHEN state = 'success' THEN 1 END) * 100.0 / COUNT(*)) AS task_success_rate
    FROM 
        task_instance
    WHERE 
        dag_id = 'your_dag_id'
    GROUP BY 
        dag_id;
    ```
  - SQL Query for Task Success Rate (all DAGs):
    ```sql
    SELECT 
        dag_id, 
        COUNT(CASE WHEN state = 'success' THEN 1 END) AS successful_tasks,
        COUNT(*) AS total_tasks,
        (COUNT(CASE WHEN state = 'success' THEN 1 END) * 100.0 / COUNT(*)) AS task_success_rate
    FROM 
        task_instance
    GROUP BY 
        dag_id;
    ```

- **Insights**:
  - Strong correlation between loudness and energy (0.80).
  - Seasonal trends in music popularity (e.g., Christmas songs).
  - Top artists like Taylor Swift and Ed Sheeran consistently score higher in popularity.

## DAGs Overview

The project is divided into multiple DAGs for modularity and scalability:
1. **DAG 1**: ETL pipeline for cleaning and transforming data from CSV files.
2. **DAG 2**: Fetches recently played tracks from the Spotify Web API and enriches them with audio features.
3. **DAG 3**: Analyzes genre popularity across decades.
4. **DAG 4**: Correlation analysis between audio features and song popularity.
5. **DAG 5**: Artist-level analysis to compare top artists' popularity.
6. **DAG 6**: Seasonal trend analysis using keywords and forecasting with Prophet.
7. **DAG 7**: Personalized music recommendations based on textual metadata.

## Future Work

- Incorporate sentiment analysis for emotionally aware recommendations.
- Expand the dataset to include diverse user behaviors and preferences.
- Enhance multilingual support for song metadata.

## Contributors

- **Abhishek Singh**  
  Masters of Data Science, Univ. of Europe for Applied Sciences  
  14469 Potsdam, Germany  

- **Daniela Oliveira Rocha**  
  Masters of Data Science, Univ. of Europe for Applied Sciences  
  14469 Potsdam, Germany  

- **Sarawut Boonyarat**  
  Masters of Data Science, Univ. of Europe for Applied Sciences  
  14469 Potsdam, Germany  

- **Mati Nakphon**  
  Masters of Data Science, Univ. of Europe for Applied Sciences  
  14469 Potsdam, Germany  

- **Muthamizhan Alagu**  
  Masters of Data Science, Univ. of Europe for Applied Sciences  
  14469 Potsdam, Germany  
