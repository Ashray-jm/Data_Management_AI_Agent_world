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
            [spotify_track_uri character varying(256) ENCODE lzo,
            ts timestamp without time zone ENCODE az64,
            platform character varying(256) ENCODE lzo,
            ms_played integer ENCODE az64,
            track_name character varying(2000) ENCODE lzo,
            artist_name character varying(256) ENCODE lzo,
            album_name character varying(256) ENCODE lzo,
            reason_start character varying(256) ENCODE lzo,
            reason_end character varying(256) ENCODE lzo,
            shuffle boolean ENCODE raw,
            skipped boolean ENCODE raw]     
            
            Build a data pipeline that:
              1. Waits for the new file each day in s3.
              2. Copies it into Amazon Redshift cluster 'redshift-cluster-1-airflow',
                 database 'dev', schema 'spotify_database', staging table
                 'spotify_upload_new'.
              3. Creates / replaces an analytics table 'spotify_history_clean'
                 applying these rules:
                 - Create a new table if it does not exist
                 - Use the same columns as the staging table, but with the following changes:
                 - Concat ms_played and platform as 'test_column'
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
