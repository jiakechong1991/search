# coding: utf8
import argparse
import codecs
import logging
import os
import sys

import pypinyin

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))
from config.conf import INPUT_MIN_LEN, INPUT_MAX_LEN
from utils.common import get_row_num
from utils.pinyin_generator import PinyinGenerator
from utils.similar_form_generator import SimilarFormGenerator

class WordWeightExtender(object):
    def __init__(self, category):
        if category == 'pinyin':
            self.generator = self.__get_pinyin_generator()
            self.min_len = INPUT_MIN_LEN
            self.max_len = INPUT_MAX_LEN
        elif category == 'similar_form':
            self.generator = self.__get_similar_form_generator()
        self.category = category

    def __get_pinyin_generator(self):
        kwargs = { 
            'styles': {
                pypinyin.NORMAL: {}, 
                'fuzzy': {
                    'char_fuzzy_flag': 'single',
                    'chars_fuzzy_flag': 'single'}
            }   
        }   
        return PinyinGenerator(**kwargs)

    def __get_similar_form_generator(self):
        return SimilarFormGenerator()

    def extend_word_weight(self, word, weight):
        if self.category == 'similar_form':
            lst_new_word_weight = self.generator.get_similar_word(word, weight)
        elif self.category == 'pinyin':
            lst_pinyin_weight = self.generator.get_local_word_pinyins(word, weight).items()
            lst_new_word_weight = []
            for pinyin, new_weight in lst_pinyin_weight:
                if (self.min_len is None or len(pinyin) >= self.min_len)\
                    and (self.max_len is None or len(pinyin) <= self.max_len):
                    lst_new_word_weight.append((pinyin, new_weight))
        return lst_new_word_weight


    def proc_file(self, file_in, file_ot):
        if not os.path.exists(file_in):
            raise Exception('file[%s] not exist.', file_in)

        row_num = get_row_num(file_in)
        with codecs.open(file_ot, 'w', encoding='utf8') as wf:
            with codecs.open(file_in, encoding='utf8') as f:
                for row_no, line in enumerate(f):
                    if row_no % 100 == 0:
                        logging.info('%d/%d', row_no, row_num)
                    items = line.strip('\r\n').split('\0')
                    if len(items) != 2:
                        logging.warning('格式不对：%s', line.strip('\r\n'))
                    word = items[0]
                    weight = int(items[1])
                    lst_new_word_weight = self.extend_word_weight(word, weight)
                    for new_word, new_weight in lst_new_word_weight:
                        new_line = '\0'.join([new_word, word, str(new_weight)])
                        wf.write(new_line + '\n')

        logging.info('proc finished!')
                    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--category', required=True, help='pinyin or similar_form')
    parser.add_argument('--file_in', required=True, help='infile')
    parser.add_argument('--file_ot', required=True, help='outfile')
    args = parser.parse_args()

    obj = WordWeightExtender(args.category)
    obj.proc_file(args.file_in, args.file_ot)



