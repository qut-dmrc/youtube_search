""" This module uses the YouTube API to create a sample of new videos.
    This is as close to a random sample as we can currently generate.
    
    This random sample is useful for academic research and analysis.  
    
    The script runs continuously, searching for videos posted in the last minute.
    Video metadata for collected videos are logged to a table in Google BigQuery.
    Data in these tables expires in 14 days.

    Input:
        None

    Output:
        Video metadata for collected videos are logged to a table in Google BigQuery. 

    Setup:
        Copy config_default.yml to config.yml and fill with your values

        Create the table as per the schema in schemas.py, and set default expiration:
         `bq update  --time_partitioning_expiration 1209600 project:dataset.table`
"""

import datetime
import time

import pytz

from utils import bq_get_client, yt_get_client, upload_rows
from log import setup_logging, print_run_summary, send_exception
from schemas import SCHEMA_YOUTUBE_SEARCH_RESULTS
from config import cfg
from youtube_utils import search_youtube

logger = setup_logging(log_file_name=None, verbose=True)

TIME_TO_RUN = -1  # Run indefinitely

MODULE_FRIENDLY_IDENTIFIER = "YouTube Random Sampler"
SECONDS_BETWEEN_EMAIL_UPDATES = 3600 * 24

# Default YouTube quota is 1,000,000 units / day.
# Cost of a search.list call is 100 units.
# The YouTube Search API is broken; we're only receiving a handful of videos every time we check.
# See https://digitalsocialcontract.net/youtube-nukes-its-api-and-search-functionality-in-response-to-christchurch-massacre-6051b4f2bb77
# For now, we're checking only once every two minutes
SECONDS_BETWEEN_CALLS = 120

def main():
    videos = []
    youtube = None

    start_time = datetime.datetime.utcnow()  # grabs the system time
    logger.info("Starting scrape from Youtube, running every {} seconds.".format(SECONDS_BETWEEN_CALLS))
    next_summary_time = datetime.datetime.utcnow() + datetime.timedelta(
        seconds=SECONDS_BETWEEN_EMAIL_UPDATES)
    last_save_time = start_time
    while True:
        try:
            run_time = datetime.datetime.utcnow()
            if not youtube or start_time < (
                    run_time + datetime.timedelta(hours=1)):  # refresh youtube token every hour
                youtube = yt_get_client(cfg['DEVELOPER_KEY'])
                start_time = run_time

            if datetime.datetime.utcnow() > next_summary_time:
                print_run_summary("{} regular update".format(MODULE_FRIENDLY_IDENTIFIER))
                next_summary_time = run_time + datetime.timedelta(
                    seconds=SECONDS_BETWEEN_EMAIL_UPDATES)

            new_vids = get_recent_youtube_vids(youtube, seconds_between_calls=SECONDS_BETWEEN_CALLS,
                                               minutes_ago=2)
            for vid in new_vids:
                video = vid
                video['search_term'] = None
                video['search_type'] = None
                video['search_time'] = run_time
                video['study_group'] = "random sample"
                video['observatory_data_source'] = 'YouTube random sample'
                videos.append(video)

            if len(videos) >= 250:
                bq_client = bq_get_client(project_id=cfg['PROJECT_ID'], json_key_file=cfg['BQ_KEY_FILE'])
                logger.info("Saving {} videos to BigQuery.".format(len(videos)))
                data_prefix = format(
                    datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S'))
                upload_rows(SCHEMA_YOUTUBE_SEARCH_RESULTS, videos, bq_client, cfg['DATASET'], cfg['SAVE_TABLE_SEARCH'],
                                   backup_file_name=None)

                time_taken = datetime.datetime.utcnow() - last_save_time
                logger.debug(
                    "Received and saved {} videos in {} seconds. {} videos per second throughput.".format(len(videos),
                                  time_taken.total_seconds(), (time_taken.total_seconds() / len(videos))))
                last_save_time = datetime.datetime.utcnow()

                videos = []

            time.sleep(SECONDS_BETWEEN_CALLS)

        except Exception as e:
            send_exception(module_name=MODULE_FRIENDLY_IDENTIFIER, message="Problem getting videos",
                           message_body="Problem getting videos: {}".format(e))
            time.sleep(30)



def get_recent_youtube_vids(youtube_client, seconds_between_calls, minutes_ago=5):
    # we are limiting to videos that have been published in the minute before this minute,
    # and reverse sorting by date. This should help us avoid disproportionately
    # retrieving live streams.

    ts_now = datetime.datetime.utcnow()  # <-- get time in UTC
    ts_to = ts_now - datetime.timedelta(minutes=minutes_ago)
    ts_from = ts_to - datetime.timedelta(seconds=seconds_between_calls)

    ts_from_str = ts_from.isoformat("T") + "Z"
    ts_to_str = ts_to.isoformat("T") + "Z"

    arguments = {"part" : "id,snippet",
            "maxResults": "50",
            "order":"date",
            "safeSearch": "none",
            "type": "video",
            "publishedBefore": ts_to_str,
            "publishedAfter": ts_from_str }

    videos = search_youtube(youtube_client, seconds_between_calls, **arguments)

    results = []

    num_inaccurate_results = 0

    for video in videos:
        # discard videos not published in the interval - sometimes YouTube doesn't return accurate results.
        if video['publishedAt'] >= pytz.timezone('UTC').localize(ts_from) and video['publishedAt'] <= pytz.timezone('UTC').localize(ts_to):
            results.append(video)
        else:
            num_inaccurate_results += 1

    logger.debug(f"Search results found {len(results)} out of {len(videos)} within timeframe. "
                 f"We discarded {num_inaccurate_results} outside of the timeframe.")

    return results


if __name__ == "__main__":
    main()
