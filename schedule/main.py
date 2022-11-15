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

# service_account_path = os.path.join("/Users/Anthony.Piccolo/dev/nbamodel/nba-data-keys.json")
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

client = storage.Client()
bucket = client.get_bucket(os.getenv("DESTINATION_GCS_BUCKET"))
# bucket = client.get_bucket(const.destination_gcs_bucket)

def scrape_schedule_data(request):
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
        'Date',
        'Start_ET',
        'Visitor_Neutral',
        'PTS_Visitor',
        'Home_Neutral',
        'PTS_Home',
        'Box_Score',
        'OT',
        'Attend',
        'Arena',
        'Notes'
    ]

    # running a for loop, from 1 to the number of the current month, and then getting the month name from that
    months = [datetime.strptime(str(i), "%m").strftime("%B") for i in range(1, datetime.now().month+1)]

    data = []

    for month in months:
        url = f"https://www.basketball-reference.com/leagues/NBA_2023_games-{month.lower()}.html"
        res = requests.get(url)
        try:
            res.status_code == 200
            soup = BeautifulSoup(res.text, "html.parser")
            table = soup.find("div", {"id":"all_schedule"})
        #print(table)
        #table = table_id.find("table", {"id":"schedule"})
        #print(table)
            columns = [i.get_text() for i in table.find("thead").findAll('th')]
            print(columns)


            for tr in table.find('tbody').find_all('tr', class_=False):
                temp = [tr.find('th').get_text(strip=True)]
                temp.extend([i.get_text(strip=True) for i in tr.find_all("td")])
                data.append(temp)
        except:
            res.status_code != 200

    filename = 'schedule' #+ datetime.now().strftime("%Y%m%d")
    df = pd.DataFrame(data, columns = headers)
    # Select a subset of the data frame
    df = df[['Date', 'Start_ET', 'Visitor_Neutral', 'PTS_Visitor', 'Home_Neutral', 'PTS_Home', 'OT']]
    df['Date'] = pd.to_datetime(df['Date'])
    data_json = df.to_json(orient='records', lines=True, date_format='iso')

    print(df)

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
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    uri = "gs://nba_schedule/{}".format(filename)

    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table("schedule"),
        location="US",  # Location must match that of the destination dataset.
        job_config=job_config,
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("schedule"))
    print("Loaded {} rows.".format(destination_table.num_rows))

    return