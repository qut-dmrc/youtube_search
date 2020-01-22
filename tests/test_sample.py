import tempfile

from nose.tools import assert_equal, assert_true

from config import cfg
from log import getLogger
from utils import yt_get_client
from youtube_sample import get_recent_youtube_vids

logger = getLogger()


class TestYoutube(object):
    def __init__(self):
        pass

    @classmethod
    def setup_class(cls):
        """This method is run once for each class before any tests are run"""
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_yt_api(self):
        youtube = youtube = yt_get_client(cfg['DEVELOPER_KEY'])
        results = get_recent_youtube_vids(youtube, 120, 60 * 28)
        num_description = 0
        for video in results:
            if video["description"]:
                num_description += 1
        assert_true(num_description>0, "At least one video has a description.")
        assert_true(len(results) >= 10, "At least ten results found.")



