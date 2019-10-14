import datetime
import logging
import numpy as np
import os
import pandas as pd

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

def upload_rows(schema, rows, bq_client, bq_dataset, bq_table, partition_by_day=True, insert_id='insert_id',
                backup_file_name=None, ensure_schema_compliance=False, len_chunks=500):
    """ Upload results to Google Bigquery """

    str_most_recent_error = ''

    logger = logging.getLogger()

    # Try to save to BigQuery. Sometimes this fails for reasons out of our control; catch the errors and return False
    try:
        _table_exists = check_create_table(bq_client, schema, bq_dataset, bq_table, create_if_not_exists=True)
        if not _table_exists:
            str_most_recent_error = 'Table does not exist.'
            logger.exception('Table does not exist and cannot create')

    except Exception as e:
        str_most_recent_error = str(e)
        logger.exception("Exception checking BigQuery table: ".format(e))
        _table_exists = False

    # Insert data into table.
    inserted = False

    bq_rows = rows

    # Make sure objects are serializable. So far, special handling for Numpy types and dates:
    bq_rows = scrub_serializable(bq_rows)

    # google recommends chunks of ~500 rows
    for index, chunk in enumerate(chunks(bq_rows, len_chunks)):
        if _table_exists:
            try:
                inserted = bq_client.push_rows(bq_dataset, bq_table, chunk, insert_id_key=insert_id)
                if inserted:
                    logger.info("Successfully inserted {} rows to BigQuery table {}.{}, chunk {}.".format(len(chunk), bq_dataset, bq_table, index))
                else:
                    str_most_recent_error = 'Unable to push rows.'
                    logger.info("Failed inserting {} rows to BigQuery table {}.{}, chunk {}.".format(len(chunk), bq_dataset, bq_table, index))
            except Exception as e:
                str_most_recent_error = str(e)[:200]
                logger.error("Exception pushing to BigQuery table {}.{}, attempt {}, reason: {}".format(bq_dataset, bq_table, index, str_most_recent_error))

        if not inserted and backup_file_name:
            save_file_full = '{}.{}'.format(backup_file_name, index)
            logger.error("Failed to upload rows! Saving {} rows to newline delimited JSON file ({}) for later upload.\nMost recent error: {}".format(len(rows), save_file_full, str_most_recent_error))

            try:
                os.makedirs(os.path.dirname(save_file_full), exist_ok=True)
            except FileNotFoundError:
                pass # We get here if we are saving to a file within the cwd without a full path

            try:
                df = pd.DataFrame.from_dict(chunk)
                df.to_json(save_file_full, orient="records", force_ascii=False)
            except Exception as e:
                str_most_recent_error = str(e)[:200]
                logger.error("Unable to save backup file {}: {}".format(save_file_full, str_most_recent_error))
    return inserted


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
