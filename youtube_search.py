"""
This module is designed to support research on political videos during the Australian Federal elections.

Process:
    For each keyword, search for matching videos uploaded in the last hour or day (sorted by relevance).
    For each video found, log its metadata.

Input:
    Create a CSV file 'youtube_keywords.csv' with at least two columns: keyword, study_group.

Output:
    Save results to local CSV file 'youtube_politics.csv'

"""

import datetime
from docopt import docopt
import pandas as pd
import csv
import requests
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from urllib.parse import quote
import logging
from config_local import DEVELOPER_KEY
import time
from dateutil import parser


YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def main():
    """ Search YouTube and log results

    Usage:
      youtube_search.py [-v] [--search_results=s] [--search_type=type] <csv_input_file_name>

    Options:
      -h --help                 Show this screen.
      -v --verbose              Increase verbosity for debugging.
      --search_results=s        Number of search results to save [default: 20]
      --search_type=type        Type of search (last-hour, top-rated, all-time, or today [default: today]

      --version  Show version.

    """

    args = docopt(main.__doc__, version='YouTube Search 0.1')
    max_search_results = int(args['--search_results'])
    search_type = ''.join(args['--search_type'])

    logging.basicConfig(format="%(asctime)s [%(filename)-20.20s:%(lineno)-4.4s - %(funcName)-20.20s() [%(levelname)-8.8s]  %(message).5000s")
    logger = logging.getLogger()
    if args['--verbose']:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    keywords = get_keywords(args['<csv_input_file_name>'])
    logging.info(f"Starting to collect search results from {len(keywords)} keywords.")

    start_time = datetime.datetime.utcnow()

    results = get_search_results_from_keywords(keywords, search_type=search_type, max_results=max_search_results)

    logging.info(f"Processed search results in {datetime.datetime.utcnow() - start_time}, "
                f"found {len(search_results_to_test)} results from {len(keywords)} keywords")

    # Save search results
    suffix = ".{}".format(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
    save_file_name = f"youtube_politics_search_{suffix}.csv"

    df = pd.DataFrame(results)
    df.to_csv(save_file_name, encoding="utf-8", quoting=csv.QUOTE_ALL)
    logging.info(f"Saved results to {save_file_name}.")


def get_search_results_from_keywords(keywords_dicts, search_type, max_results):
    assert search_type in ['last-hour', 'top-rated', 'all-time', 'today'], "Type must be specified."
    youtube_client = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

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

        # Escaping search terms for youtube
        escaped_search_terms = quote(keyword.encode('utf-8'))
        arguments['q'] = escaped_search_terms

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


def search_youtube(youtube_client, seconds_between_calls, **kwargs):
    videos = []

    try:
        search_response = youtube_client.search().list(
            **kwargs
        ).execute()

        for search_result in search_response.get("items", []):
            if search_result["id"]["kind"] == "youtube#video":
                videos.append(search_result)
    except HttpError as e:
        # If the error is a rate limit or connection error, back off a bit -  usually a server problem
        if e.resp.status in [403, 500, 503]:
            time.sleep(2 * seconds_between_calls)
            logging.error("Received a HTTP error searching API. Sleeping for five seconds. Error: {}".format(e))
        else:
            logging.error("Problem getting youtube videos: {}".format(e))
    except Exception as e:
        logging.error("Problem getting youtube videos: {}".format(e))

    results = []
    for video in videos:
        rowdict = {'publishedAt': video["snippet"]["publishedAt"]}

        if isinstance(rowdict['publishedAt'], str):
            rowdict['publishedAt'] = parser.parse(rowdict['publishedAt'])

        rowdict['videoId'] = video["id"]["videoId"]
        rowdict['title'] = video["snippet"]["title"]
        rowdict['channelTitle'] = video["snippet"]["channelTitle"]
        rowdict['description'] = video["snippet"]["description"]

        results.append(rowdict)

    return results


def get_keywords(csv_file):
    columns = ['keyword', 'study_group']

    df = pd.read_csv(csv_file, encoding='utf-8', quoting=csv.QUOTE_ALL, usecols=columns)
    assert 'keyword' in df.columns, "Invalid CSV file. Must contain columns: keyword, study_group"
    assert 'study_group' in df.columns, "Invalid CSV file. Must contain columns: keyword, study_group"

    return df[columns].to_dict(orient='records')


if __name__ == '__main__':
    main()
