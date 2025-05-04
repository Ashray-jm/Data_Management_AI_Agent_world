#!/usr/bin/env python
import sys
import warnings

from datetime import datetime
from textwrap import dedent
from crew import DataManagementCrew
# main.py
from dotenv import load_dotenv
load_dotenv()               # pulls .env into os.environ

warnings.filterwarnings("ignore", category=SyntaxWarning, module="pysbd")

# This main file is intended to be a way for you to run your
# crew locally, so refrain from adding unnecessary logic into this file.
# Replace with inputs you want to test with, it will automatically
# interpolate any tasks and agents information

def run():
    """
    Run the crew.
    """
    inputs = {
        "requirement": dedent("""
            A single CSV file named spotify_history.csv is written every day
            around 06:30 ET to s3://datasets-agentic-ai/.

            Columns in the file:
              * spotify_track_uri (STRING)
              * ts (TIMESTAMP in ISO‑8601, UTC)
              * platform (STRING)
              * ms_played (INTEGER, milliseconds)
              * track_name, artist_name, album_name (STRING)
              * reason_start, reason_end (STRING)
              * shuffle, skipped (BOOLEAN encoded as 'true'/'false')

            Build a data pipeline that:
              1. Waits for the new file each day.
              2. Copies it into Amazon Redshift cluster 'redshift-cluster-1-airflow',
                 database 'dev', schema 'public', staging table
                 'stg_spotify_history_raw'.
              3. Creates / replaces an analytics table 'spotify_history_clean'
                 applying these rules:
                 • Trim whitespace on all string columns.
                 • CAST ms_played -> INT, derive play_seconds = ms_played/1000.
                 • CAST ts -> TIMESTAMP WITH TIME ZONE.
                 • Filter out rows where spotify_track_uri OR ts is NULL.
                 • Remove duplicates on (spotify_track_uri, ts) keeping the latest.
              4. Makes the clean table available by 07:30 ET daily.

        """).strip(),
        "current_year": str(datetime.now().year),
    }
    DataManagementCrew().crew().kickoff(inputs=inputs)

if __name__ == "__main__":
    run()
































# def train():
#     """
#     Train the crew for a given number of iterations.
#     """
#     inputs = {
#         "topic": "AI LLMs",
#         'current_year': str(datetime.now().year)
#     }
#     try:
#         DataManagementCrew().crew().train(n_iterations=int(sys.argv[1]), filename=sys.argv[2], inputs=inputs)

#     except Exception as e:
#         raise Exception(f"An error occurred while training the crew: {e}")

# def replay():
#     """
#     Replay the crew execution from a specific task.
#     """
#     try:
#         DataManagementCrew().crew().replay(task_id=sys.argv[1])

#     except Exception as e:
#         raise Exception(f"An error occurred while replaying the crew: {e}")

# def test():
#     """
#     Test the crew execution and returns the results.
#     """
#     inputs = {
#         "topic": "AI LLMs",
#         "current_year": str(datetime.now().year)
#     }
    
#     try:
#         DataManagementCrew().crew().test(n_iterations=int(sys.argv[1]), eval_llm=sys.argv[2], inputs=inputs)

#     except Exception as e:
#         raise Exception(f"An error occurred while testing the crew: {e}")
