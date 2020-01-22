"""
This module is designed to support research on political videos during the Australian Federal elections.

Process:
    For each keyword, search for matching videos uploaded in the last hour or day (sorted by relevance).
    For each video found, log its metadata.

Input:
    Create a CSV file 'youtube_keywords.csv' with at least two columns: keyword, study_group.

Output:
    Save results to a BigQuery table (data expires in 14 days).

Setup:
    Copy config_default.py to config_local.py and fill with your values (API keys and save table)

"""

import datetime
from logging.handlers import RotatingFileHandler

from docopt import docopt
import pandas as pd
import csv
import logging
from config import cfg

from schemas import SCHEMA_YOUTUBE_SEARCH_RESULTS
from utils import bq_get_client, upload_rows, yt_get_client
from youtube_utils import search_youtube


def main():
    """ Search YouTube and log results

    Usage:
      youtube_search.py [-v] [-l log_file] [--search_results=s] [--search_type=type] <csv_input_file_name>

    Options:
      -h --help                 Show this screen.
      -v --verbose              Increase verbosity for debugging.
      -l <log_file> --log=<log_file>    Save log to file
      --search_results=s        Number of search results to save [default: 20]
      --search_type=type        Type of search (last-hour, top-rated, all-time, or today [default: today]

      --version  Show version.

    """

    args = docopt(main.__doc__, version='YouTube Search 0.1')
    max_search_results = int(args['--search_results'])
    search_type = ''.join(args['--search_type'])

    setup_logging(log_file_name=args['--log'], verbose=args['--verbose'])
    
    keywords = get_keywords(args['<csv_input_file_name>'])
    search_youtube_keywords(keywords, max_search_results, search_type)


def search_youtube_keywords(keywords, max_search_results, search_type):
    logging.info(f"Starting to collect search results from {len(keywords)} keywords.")
    start_time = datetime.datetime.utcnow()
    results = get_search_results_from_keywords(keywords, search_type=search_type, max_results=max_search_results)
    logging.info(f"Processed search results in {datetime.datetime.utcnow() - start_time}, "
                 f"found {len(results)} results from {len(keywords)} keywords")
    # Save search results
    save_table = cfg['SAVE_TABLE_SEARCH']
    backup_file_name = "data/{}_{}.json" .format(save_table, datetime.datetime.now().strftime('%Y%m%d'))
    bq_client = bq_get_client(project_id=cfg['PROJECT_ID'], json_key_file=cfg['BQ_KEY_FILE'])
    logging.info(f"Saving results to BQ {save_table} or backup file {backup_file_name}.")
    upload_rows(SCHEMA_YOUTUBE_SEARCH_RESULTS, results, bq_client, cfg['DATASET'], save_table,
                backup_file_name=backup_file_name)


def get_search_results_from_keywords(keywords_dicts, search_type, max_results):
    assert search_type in ['last-hour', 'top-rated', 'all-time', 'today'], "Type must be specified."
    youtube_client = yt_get_client(developer_key=cfg['DEVELOPER_KEY'])

    search_results = []
    seconds_between_calls = 2

    for entry in keywords_dicts:
        keyword = entry['keyword']
        study_group = entry['study_group']

        ts_now = datetime.datetime.utcnow()  # <-- get time in UTC

        arguments = {"part": "id,snippet",
                     "maxResults": max_results,
                     "order": "relevance",
                     "safeSearch": "none",
                     "type": "video", }

        arguments['q'] = keyword
        # Escaping search terms for youtube
        #escaped_search_terms = quote(keyword.encode('utf-8'))
        #arguments['q'] = escaped_search_terms

        if search_type == 'all-time':
            pass
        elif search_type == 'top-rated':
            arguments['order'] = "rating"
        elif search_type == 'last-hour':
            search_date = ts_now + datetime.timedelta(hours=-1)
            search_date = search_date.isoformat("T") + "Z" # Convert to RFC 3339
            arguments['publishedAfter'] = search_date
        elif search_type == 'today':
            search_date = ts_now + datetime.timedelta(days=-1)
            search_date = search_date.isoformat("T") + "Z"  # Convert to RFC 3339
            arguments['publishedAfter'] = search_date

        logging.info(f'Searching for {entry}')

        results = search_youtube(youtube_client=youtube_client, seconds_between_calls=seconds_between_calls, **arguments)

        for vid in results:
            video = vid
            video['search_term'] = keyword
            video['search_type'] = search_type
            video['search_time'] = ts_now
            video['study_group'] = study_group
            video['observatory_data_source'] = 'YouTube search from keywords'
            search_results.append(video)

        #time.sleep(seconds_between_calls)

    return search_results


def get_keywords(csv_file):
    columns = ['keyword', 'study_group']

    df = pd.read_csv(csv_file, encoding='utf-8', quoting=csv.QUOTE_ALL, usecols=columns)
    assert 'keyword' in df.columns, "Invalid CSV file. Must contain columns: keyword, study_group"
    assert 'study_group' in df.columns, "Invalid CSV file. Must contain columns: keyword, study_group"

    return df[columns].to_dict(orient='records')


def setup_logging(log_file_name=None, verbose=False):
    if not verbose:
        # Quieten other loggers down a bit (particularly requests and google api client)
        for logger_str in logging.Logger.manager.loggerDict:
            try:
                logging.getLogger(logger_str).setLevel(logging.WARNING)

            except:
                pass

    logFormatter = logging.Formatter(
        "%(asctime)s [%(filename)-20.20s:%(lineno)-4.4s - %(funcName)-20.20s() [%(levelname)-8.8s]  %(message).5000s")
    logger = logging.getLogger()

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)

    if verbose:
        consoleHandler.setLevel(logging.DEBUG)
    else:
        consoleHandler.setLevel(logging.INFO)

    if log_file_name:
        fileHandler = RotatingFileHandler(log_file_name, maxBytes=20000000, backupCount=20, encoding="UTF-8")
        fileHandler.setFormatter(logFormatter)

        if verbose:
            fileHandler.setLevel(logging.DEBUG)
        else:
            fileHandler.setLevel(logging.INFO)

        logger.addHandler(fileHandler)

    # Add

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    return logger


if __name__ == '__main__':
    main()
