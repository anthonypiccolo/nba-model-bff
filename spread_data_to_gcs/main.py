"""
Deploy commands:
gcloud config set project nbamodel-223111
gcloud functions deploy spread_data_to_gcs --runtime python37 --trigger-topic mj_goat
"""

import base64
import json
import logging
import constants as const
from google.cloud import bigquery

BUCKET_NAME = const.BUCKET_NAME
PROJECT = const.PROJECT
FILE_FORMAT = const.FILE_FORMAT
ZIPPED = const.ZIPPED
COMPRESSION_FORMAT = const.COMPRESSION_FORMAT

def spread_data_to_gcs(event, context):
    """
    Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    #print(event)
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    obj = json.loads(pubsub_message)
    logging.info('pubsub_message: {}'.format(pubsub_message))

    #print(obj)
    bq_client = bigquery.Client()
    table_id = 'data'
    dataset_id = obj['destinationDatasetId']
    file_name = 'data'

    if FILE_FORMAT == "AVRO" and COMPRESSION_FORMAT == "GZIP":
        logging.info("Cannot GZIP an AVRO file")
        return False
    elif FILE_FORMAT in ["NEWLINE_DELIMITED_JSON","CSV"] and COMPRESSION_FORMAT in ["SNAPPY","DEFLATE"]:
        logging.info("Cannot use Snappy or Deflate on a CSV or JSON file")
        return False

    if FILE_FORMAT == "NEWLINE_DELIMITED_JSON":
        file_name = file_name + '.json'
    elif FILE_FORMAT == "CSV":
        file_name = file_name + '.csv'
    elif FILE_FORMAT == "AVRO":
        file_name = file_name + '.avro'
    else:
        return False

    #if ZIPPED and FILE_FORMAT in ["NEWLINE_DELIMITED_JSON","CSV"]:
        #file_name = file_name + '.gz'

    logging.info('file name: {}'.format(file_name))

    destination_uri = 'gs://{}/{}'.format(BUCKET_NAME, file_name)
    dataset_ref = bq_client.dataset(dataset_id, project=PROJECT)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.ExtractJobConfig()
    job_config.compression = COMPRESSION_FORMAT
    job_config.destination_format = FILE_FORMAT

    extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config)
    extract_job.result()

    print('Exported {}:{} to {}'.format(
        PROJECT, table_id, destination_uri))

    return
