from nose.tools import assert_equal, assert_not_equal, assert_raises, assert_true, assert_is_not_none

from youtube_search import get_search_results_from_keywords, search_youtube_keywords


class TestYoutubeSearch(object):
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

    # def test_yt_search_keyword(self):
    #     keywords = [{'keyword': 'election', 'study_group': 'unit tests'},
    #                 {'keyword': 'scott morrison', 'study_group': 'unit tests'},]
    #     search_results = get_search_results_from_keywords(keywords, search_type='top-rated', max_results=10)
    #
    #     assert_equal(len(search_results)==20, "Not enough results found for search.")


    def test_yt_search_keyword_integration(self):
        keywords = [{'keyword': 'canberra', 'study_group': 'unit tests'},
                    {'keyword': 'scott morrison', 'study_group': 'unit tests'}, ]
        success = search_youtube_keywords(keywords=keywords, max_search_results=20, search_type='today')
        assert_true(success)