import requests
from bs4 import BeautifulSoup
#from selenium import webdriver
import pandas as pd
import constants as const
from google.cloud import storage
import os
import json
from datetime import datetime
from google.cloud import bigquery
import re
from flask import escape

#service_account_path = os.path.join("/Users/Anthony.Piccolo/dev/nbamodel/nba-data-keys.json")
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

client = storage.Client()
bucket = client.get_bucket(os.getenv("DESTINATION_GCS_BUCKET"))
# bucket = client.get_bucket(const.destination_gcs_bucket)

def scrape_team_data(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.
    """

    headers = [
        'Rank',
        'Team',
        'Age',
        'Wins',
        'Losses',
        'PW',
        'PL',
        'MOV',
        'SOS',
        'SRS',
        'ORtg',
        'DRtg',
        'NRtg',
        'Pace',
        'FTr',
        '_3PAr',
        'TS_pct',
        'blank1',
        'offense_eFG_pct',
        'offense_TOV_pct',
        'offense_ORB_pct',
        'offense_FT_FGA',
        'blank2',
        'defense_eFG_pct',
        'defense_TOV_pct',
        'defense_DRB_pct',
        'defense_FT_FGA',
        'blank3',
        'Arena',
        'Attendance',
        'Attendance_Game'
        ]

    r = requests.get('https://www.basketball-reference.com/leagues/NBA_2022.html')
    matches = re.findall(r'id=\"all_advanced_team\".+?(?=table>)table>', r.text, re.DOTALL)
    find_table = pd.read_html('<table ' + matches[0])
    df = find_table[0]
    df.columns = headers
    df = df.drop(['blank1', 'blank2', 'blank3'], axis=1)
    filename = 'teams_data_adv_stats' #+ datetime.now().strftime("%Y%m%d"))
    #df['date'] = datetime.now().strftime("%Y-%m-%d")
    data_json = df.to_json(orient='records', lines=True)

        #URL = 'https://www.basketball-reference.com/leagues/NBA_2020.html'

        # The following options are required to make headless Chrome
        # work in a Docker container
        #chrome_options = webdriver.ChromeOptions()
        #chrome_options.add_argument("--headless")
        #chrome_options.add_argument("--disable-gpu")
        #chrome_options.add_argument("window-size=1024,768")
        #chrome_options.add_argument("--no-sandbox")

        #driver = webdriver.Chrome(chrome_options=chrome_options)
        #driver.get(URL)
        #soup = BeautifulSoup(driver.page_source,'html')
        #driver.quit()
        #tables = soup.find_all('table',{"id":["misc_stats"]})

        #table = tables[0]
        #tab_data = [[cell.text for cell in row.find_all(["th","td"])]
                                #for row in table.find_all("tr")]
        #headers = [
            #'Rank',
            #'Team',
            #'Age',
            #'Wins',
            #'Losses',
            #'PW',
            #'PL',
            #'MOV',
            #'SOS',
            #'SRS',
            #'ORtg',
            #'DRtg',
            #'NRtg',
            #'Pace',
            #'FTr',
            #'_3PAr',
            #'TS_pct',
            #'offense_eFG_pct',
            #'offense_TOV_pct',
            #'offense_ORB_pct',
            #'offense_FT_FGA',
            #'defense_eFG_pct',
            #'defense_TOV_pct',
            #'defense_DRB_pct',
            #'defense_FT_FGA',
            #'Arena',
            #'Attendance',
            #'Attendance_Game'
        #]

    print(filename)

    # Push data to GCS
    blob = bucket.blob(filename)

    blob.upload_from_string(
        data_json,
        content_type='application/json'
    )

    # Create BQ table from data in bucket
    client = bigquery.Client()
    dataset_id = 'nba_model'

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'
        #job_config.schema = [
            #bigquery.SchemaField("Attend__G", "INTEGER"),
            #bigquery.SchemaField("Arena", "STRING"),
            #bigquery.SchemaField("FT_FGA", "FLOAT"),
            #bigquery.SchemaField("ORBpct", "FLOAT"),
            #bigquery.SchemaField("ORtg", "FLOAT"),
            #bigquery.SchemaField("TOVpct", "FLOAT"),
            #bigquery.SchemaField("FTr", "FLOAT"),
            #bigquery.SchemaField("SOS", "FLOAT"),
            #bigquery.SchemaField("Pace", "FLOAT"),
            #bigquery.SchemaField("eFGpct", "FLOAT"),
            #bigquery.SchemaField("MOV", "FLOAT"),
            #bigquery.SchemaField("NRtg", "FLOAT"),
            #bigquery.SchemaField("TSpct", "FLOAT"),
            #bigquery.SchemaField("Rk", "INTEGER"),
            #bigquery.SchemaField("_3PAr", "FLOAT"),
            #bigquery.SchemaField("SRS", "FLOAT"),
            #bigquery.SchemaField("DRtg", "FLOAT"),
            #bigquery.SchemaField("PW", "INTEGER"),
            #bigquery.SchemaField("Attend_", "INTEGER"),
            #bigquery.SchemaField("PL", "INTEGER"),
            #bigquery.SchemaField("L", "INTEGER"),
            #bigquery.SchemaField("DRBpct", "FLOAT"),
            #bigquery.SchemaField("Age", "FLOAT"),
            #bigquery.SchemaField("W", "INTEGER"),
            #bigquery.SchemaField("Team", "STRING"),
            #bigquery.SchemaField("date", "DATE"),
        #]
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    uri = "gs://nba_teams_data/{}".format(filename)

    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table("teams_data"),
        location="US",  # Location must match that of the destination dataset.
        job_config=job_config,
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("teams_data"))
    print("Loaded {} rows.".format(destination_table.num_rows))

    return