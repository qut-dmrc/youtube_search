import datetime
import logging
import numpy as np
import os
import pandas as pd
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
import google.auth

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def bq_get_client(project_id, json_key_file):
    logger = logging.getLogger()

    try:
        # noinspection PyPackageRequirements
        from bigquery import \
            get_client  # (get the latest version with: pip install git+https://github.com/tylertreat/BigQuery-Python.git#egg=bigquery-python)
        client = get_client(project_id=project_id, json_key_file=json_key_file,
                            readonly=False, num_retries=2)
    except ImportError as e:
        logger.warning("error importing bigquery: {}".format(e))
        return None

    return client


def bq_get_clients(project_id, json_key_file):
    credentials, your_project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    # Make clients.
    bqclient = bigquery.Client(
        credentials=credentials,
        project=your_project_id,
    )
    bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient(
        credentials=credentials
    )
    return bqclient, bqstorageclient


def yt_get_client(developer_key):
    client = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=developer_key)
    return client
        

def check_create_table(bq_client, schema, bq_dataset, bq_table, partition_by_day=True, create_if_not_exists=True, expiry_days=14):
    _exists = bq_client.check_table(bq_dataset, bq_table)

    expiry_time_ms = int((datetime.datetime.utcnow() + datetime.timedelta(days=expiry_days)).timestamp()) * 1000
    if not _exists and create_if_not_exists:
        # Create table
        created = bq_client.create_table(bq_dataset, bq_table, schema,
                                         time_partitioning=partition_by_day,
                                         expiration_time=expiry_time_ms)
        if not created:
            return False
    return True

def upload_rows(schema, rows, bq_client, bq_dataset, bq_table,
                backup_file_name=None, len_chunks=500):
    """ Upload results to Google Bigquery """

    logger = logging.getLogger()

    inserted = False
    bq_rows = rows

    # Make sure objects are serializable. So far, special handling for Numpy types and dates:
    bq_rows = scrub_serializable(bq_rows)

    table = None
    try:
        table = bq_client.get_table(f"{bq_dataset}.{bq_table}")
    except Exception as e:
        logger.error(
            msg=f"Unable to save rows. Table {bq_dataset}.{bq_table} does not exist or there was some other "
                         f"problem getting the table.", subject="Error inserting rows to Google Bigquery!")

    # google recommends chunks of ~500 rows
    for index, chunk in enumerate(chunks(bq_rows, len_chunks)):
        str_error = ""
        inserted = False
        if table:
            try:
                logger.debug(
                    "Inserting {} rows to BigQuery table {}.{}, attempt {}.".format(len(chunk), bq_dataset,
                                                                                                bq_table, index))

                errors = bq_client.insert_rows(table, chunk)
                if errors == []:
                    inserted = True

                    logger.info("Successfully inserted {} rows to BigQuery table {}.{}, attempt {}.".format(len(chunk), bq_dataset, bq_table, index))
                else:
                    str_error += f"Google BigQuery returned an error result: {str(errors)}\n\n"

            except Exception as e:
                str_error += "Exception pushing to BigQuery table {}.{}, attempt {}, reason: {}\n\n".format(bq_dataset, bq_table, index, str(e)[:2000])
        else:
            str_error += "Could not get table, so could not push rows.\n\n"

        if not inserted:
            if backup_file_name:
                save_file_full = '{}.{}'.format(backup_file_name, index)
                logger.error("Failed to upload rows! Saving {} rows to newline delimited JSON file ({}) for later upload.".format(len(rows), save_file_full))

                try:
                    os.makedirs(os.path.dirname(save_file_full), exist_ok=True)
                except FileNotFoundError:
                    pass  # We get here if we are saving to a file within the cwd without a full path

                try:
                    df = pd.DataFrame.from_dict(chunk)
                    df = nan_ints(df, convert_strings=True)
                    df.to_json(save_file_full, orient="records", lines=True, force_ascii=False)
                    str_error += "Saved {} rows to newline delimited JSON file ({}) for later upload.\n\n".format(len(rows), save_file_full)
                except Exception as e:
                    str_error += "Unable to save backup file {}: {}\n\n".format(save_file_full,  str(e)[:200])

            else:
                str_error += "No backup save file configured.\n\n"

            message_body = "Exception pushing to BigQuery table {}.{}, chunk {}.\n\n".format(
                bq_dataset, bq_table, index)
            message_body += str_error

            logger.error(
                msg=message_body, subject=f"Error inserting rows to Google Bigquery! Table: {bq_dataset}.{bq_table}")
            logger.debug("First three rows:")
            logger.debug(bq_rows[:3])

    return inserted


def nan_ints(df,convert_strings=False,subset = None):
    # Convert int, float, and object columns to int64 if possible (requires pandas >0.24 for nullable int format)
    types = ['int64','float64']
    if subset is None:
        subset = list(df)
    if convert_strings:
        types.append('object')
    for col in subset:
        try:
            if df[col].dtype in types:
                df[col] = df[col].astype(float).astype('Int64')
        except:
            pass
    return df

def scrub_serializable(d):
    try:
        if isinstance(d, list):
            d = [scrub_serializable(x) for x in d]
            return d

        if isinstance(d, dict):
            for key in list(d.keys()):
                if d[key] is None:
                    del d[key]
                elif hasattr(d[key], 'dtype'):
                    d[key] = np.asscalar(d[key])
                elif isinstance(d[key], dict):
                    d[key] = scrub_serializable(d[key])
                elif isinstance(d[key], list):
                    d[key] = [scrub_serializable(x) for x in d[key]]
                elif isinstance(d[key], datetime.datetime):
                    # ensure dates are stored as strings in ISO format for uploading
                    d[key] = d[key].isoformat()

        return d
    except Exception as e:
        print(e)
        raise

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]
