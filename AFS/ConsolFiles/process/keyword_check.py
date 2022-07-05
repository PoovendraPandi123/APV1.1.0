import logging
from .models import Sources

logger = logging.getLogger("consolidation_files")


class KeywordsUniqueCheck():
    keyword = ''
    source_keywords = []
    check_output = []
    check_keyword_output = False

    def __init__(self, keyword):
        self.keyword = keyword
        self.source_keywords_from_db()
        self.check_keyword_unique()

    def source_keywords_from_db(self):
        sources = Sources.objects.filter(is_active=1)
        source_key_words_list_dict = []
        for source in sources:
            source_key_words_list_dict.append(source.key_words)

        for key_word in source_key_words_list_dict:
            self.source_keywords.append(key_word["keywords"])

    def check_keyword_unique(self):
        try:
            source_list = self.source_keywords
            # print("source_list", source_list)
            # print("Keyword", self.keyword)
            for key in self.keyword.split(","):
                # print("Before Check Source_list", source_list)
                # print("key", key)
                self.check(key, source_list)
                source_list = self.check_output
                # print("After Check Source_list", source_list)
            if len(source_list) == 0:
                self.check_keyword_output = True

        except Exception:
            logger.error("Error in Check Keyword Unique Function in KeywordsUniqueCheck Class!!!", exc_info=True)

    def check(self, key, source_key_list):
        try:
            # print("keyword" + self.keyword)
            self.check_output = []
            for list_sour in source_key_list:
                # list_sour = list(v)
                # print("list_sour", list_sour)
                if key in list_sour:
                    self.check_output.append(list_sour)

        except Exception:
            logger.error("Error in Check Function in KeywordsUniqueCheck Class!!!", exc_info=True)

    def get_keyword_unique_check_output(self):
        return self.check_keyword_output
