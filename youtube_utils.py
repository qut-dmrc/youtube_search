import logging
import time

from dateutil import parser
from googleapiclient.errors import HttpError


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