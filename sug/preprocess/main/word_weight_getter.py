# coding: utf8
import argparse
import codecs
import json
import logging
import re

import jieba

from config.conf import LOGGING_FORMAT, MIN_WORD_LEN, MAX_WEIGHT, MAX_WORD_LEN
from utils.common import get_row_num, is_start_with_ordinal_num, strQ2B
from utils.filter_stopwords import FilterWords
from utils.paper_word_trie import Trie


FILE_STOPWORD = 'resources/stopwords.txt'
FILE_PAPER_PHRASE = 'resources/paper_phrase/paper.keywords.uniq.k4'

PUNCTUATIONS=u'[(|)|\[|\]|\{|\}|–|─|—|、|\-|_|:|;|!|@|#|$|&|\*|•|?|,|.|。|\"|\']'


class WordWeightGetter():
    @property
    def category_weight(self):
        _category_weight = {
            'course_name': 0,
            'about': 5,
            'chapter': 10,
            'sequential': 15
        }
        return _category_weight

    @property
    def stopwords_filter(self):
        if not hasattr(self, '_stopwords_filter'):
            self._stopwords_filter = FilterWords()
            self._stopwords_filter.init(FILE_STOPWORD)
        return self._stopwords_filter

    @property
    def paper_trie(self):
        if not hasattr(self, '_paper_trie'):
            self._paper_trie = Trie()
            self._paper_trie.build_dict_by_file(FILE_PAPER_PHRASE)
        return self._paper_trie

    def get_clean_text(self, text):
        text = strQ2B(text)
        text = text.lower()
        text = re.sub(r'\s+', ' ', text)
        return text

    def get_words(self, text):
        words = list(jieba.cut(text, cut_all=True))
        words = self.stopwords_filter.filter_stopwords_lst(words)
        words = [word.strip() for word in words]
        return words

    def add_word_weight(self, word, weight):
        if len(word) < MIN_WORD_LEN or len(word) > MAX_WORD_LEN:
            return
        if is_start_with_ordinal_num(word):
            return
        if weight < self.word_weight.get(word, 0):
            return
        self.word_weight[word] = weight

    def get_word_weight(self, file_in):
        self.word_weight = {}

        row_num = get_row_num(file_in)
        with codecs.open(file_in, encoding='utf8') as f:
            for line_no, line in enumerate(f):
                if line_no % 10000 == 0:
                    logging.info('finished: %s, %s', line_no, row_num)
                try:
                    row = json.loads(line)
                except Exception, e:
                    logging.error(line_no)
                    logging.error(line)
                    raise Exception(e)
                category_weight = self.category_weight.get(row['category'])
                if category_weight is None:
                    continue
                weight = MAX_WEIGHT - category_weight

                text = self.get_clean_text(row['value'])

                # common
                words = self.get_words(text)
                for word in words:
                    self.add_word_weight(word, weight)

                # course_name
                if row['category'] == 'course_name':
                    words = [word.strip() for word in re.split(PUNCTUATIONS, text)]
                    for word in words:
                        self.add_word_weight(word, MAX_WEIGHT)
                
                # paper_words
                paper_words = self.paper_trie.get_all_match(text)
                for word, _ in paper_words.items():
                    self.add_word_weight(word, weight)

        return self.word_weight

    def dump_file(self, word_weight, file_ot):
        wf = codecs.open(file_ot, 'w', encoding='utf8')
        for word, weight in word_weight.items():
            row = {
                'input': word,
                'output': word,
                'weight': weight
            }
            wf.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + '\n')
        wf.close()


    def proc_file(self, file_in, file_ot):
        word_weight = self.get_word_weight(file_in)
        self.dump_file(word_weight, file_ot)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_in', required=True)
    parser.add_argument('--file_ot', required=True)
    args = parser.parse_args()

    obj = WordWeightGetter()
    obj.proc_file(args.file_in, args.file_ot)



