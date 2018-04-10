# coding: utf8

from collections import defaultdict
import itertools
import logging

import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Q
import pypinyin
from pyxdameraulevenshtein import damerau_levenshtein_distance
from tornado import gen

from config.conf import ES_HOSTS, ES_INDEX
from force_correct_map import FORCE_CORRECT_MAP
from utils.common import all_in_list,\
                            all_in_range,\
                            has_in_range
from utils.common import discard_all_digits,\
                            get_lcs,\
                            get_chinese_sequence,\
                            get_remain_sequence
from utils.lang_conf import arabic_digits, chinese_digits, chinese_range, digit_range
from utils.pinyin_generator import PinyinGenerator
from utils.word_cleaner import WordCleaner

class Corrector(object):
    def __init__(self):
        self.ES_CONNECTION_TIMEOUT = 1

    @property
    def es_client(self):
        if not hasattr(self, '_es_client'):
            self._es_client = Elasticsearch(ES_HOSTS, port=9200)
        return self._es_client

    @property
    def cleaner(self):
        if not hasattr(self, '_cleaner'):
            self._cleaner = WordCleaner()
        return self._cleaner

    @property
    def pinyin_generator(self):
        if not hasattr(self, '_pinyin_generator'):
            self._pinyin_generator = PinyinGenerator()
        return self._pinyin_generator

    def get_clean_word(self, word):
        clean_word, strange_set = self.cleaner.clean(word) 
        if strange_set:
            logging.warning('用户的查询包含特殊字符：', strange_set)
        return clean_word

    def get_forced_word(self, clean_word):
        return FORCE_CORRECT_MAP.get(clean_word)

    def has_exact_match(self, clean_word):
        s = Search(using=self.es_client, index=ES_INDEX, doc_type='chinese_ngram')\
                .query(Q('bool', filter=[Q('term', chinese_word=clean_word)]))\
                .params(size=1, request_timeout=self.ES_CONNECTION_TIMEOUT)
        response = s.execute()
        if response:
            return True
        s = Search(using=self.es_client, index=ES_INDEX, doc_type='non_chinese_ngram')\
                .query(Q('bool', filter=[Q('term', non_chinese_word=clean_word)]))\
                .params(size=1, request_timeout=self.ES_CONNECTION_TIMEOUT)
        response = s.execute()
        if response:
            return True
        return False
       
    def get_homophones(self, clean_word):
        """获取与这个词最接近的同音拼音或者近音拼音"""
        # 利用2-gram来操作
        homophones = defaultdict(int)
        word_pinyins = self.pinyin_generator.get_word_simple_pinyins(clean_word, pypinyin.NORMAL)
        for word_pinyin in word_pinyins[:10]:
            discard_digits_pinyin = discard_all_digits(word_pinyin)
            s = Search(using=self.es_client, index=ES_INDEX, doc_type='pinyin_ngram')\
                    .query(Q('bool', filter=[Q('term', non_chinese_word=discard_digits_pinyin)]))\
                    .params(request_timeout=self.ES_CONNECTION_TIMEOUT)
            response = s.execute()

            for hit in response:
                output = hit.output
                weight = hit.weight
                if weight > homophones[output]:
                    homophones[output] = weight
        print "word_pinyins:", word_pinyins
        return homophones
    
    def get_candidate_from_homophones(self, clean_word, homophones):
        if not has_in_range(clean_word, chinese_range):
            response = {
                'status': 'need_correct', 
                'msg': u'同音词：原词为纯英文+数字',
                'candidate': [homophone_weight[0] for homophone_weight in
                    sorted(homophones.items(), key=lambda homophone: -homophone[1])]
            }
            return response

        candidate_homophone_weight = []
        for homophone, weight in homophones.items():
            long_common_sequence = get_lcs(clean_word, homophone)
            if long_common_sequence:
                remain_sequence = get_remain_sequence(long_common_sequence, clean_word, included=True)
                if all_in_range(remain_sequence, digit_range):
                    response = {
                        'status': 'right',
                        'msg': u'query与候选词只差几个数字',
                        'candidate': []
                    }
                    return response
                else:
                    candidate_homophone_weight.append((homophone, weight))

        if candidate_homophone_weight:
            response = {
                'status': 'need_correct',
                'msg': u'同音词：query与候选词不完全相同',
                'candidate': [homophone_weight[0] for homophone_weight in
                                sorted(candidate_homophone_weight, key=lambda x: x[1])]
            }
        else:
            response = {
                'status': '',
                'msg': u'同音词：所有的候选词都被抛弃了，大概是query与候选词不存在公共字符串',
                'candidate': []
            }
        return response

    def get_similar_forms(self, clean_word):
        similar_forms = []
        s = Search(using=self.es_client, index=ES_INDEX, doc_type='similar_form_no_ngram')\
                .query(Q('bool', filter=[Q('term', similar_word=clean_word)]))\
                .params(request_timeout=self.ES_CONNECTION_TIMEOUT)
        response = s.execute()
        for hit in response:
            similar_forms.append((hit.output, hit.weight))
        sorted_similar_forms = [x[0] for x in sorted(similar_forms, key=lambda x: x[1])]
        return sorted_similar_forms

    def get_word_first_letters(self, word):
        lst_first_letters = []
        lst_pinyin = list(itertools.product(*pypinyin.pinyin(word, pypinyin.NORMAL)))
        for word_pinyin in lst_pinyin:
            first_letters = [char_pinyin[0] for char_pinyin in word_pinyin if len(char_pinyin)>0]
            lst_first_letters.append(''.join(first_letters))
        return lst_first_letters

    def get_ngram_words(self, clean_word):
        def update(ngram_words, output, score, weight, params):
            if output not in ngram_words:
                ngram_words[output] = {
                    'score': score,
                    'weight': weight,
                    'params': params
                }
            else:
                if score > ngram_words[output]['score']:
                    ngram_words[output]['score'] = score
                    ngram_words[output]['params'] = params
                if weight > ngram_words[output]['weight']:  # weight只和output有关
                    ngram_words[output]['weight'] = weight
        
        def compare(x, y):
            x_digit_num, y_digit_num = len(str(x['weight'])), len(str(y['weight']))
            if x_digit_num != y_digit_num:
                return -1 if x_digit_num < y_digit_num else 1
            if x['score'] != y['score']:
                return -1 if x['score'] < y['score'] else 1
            if x['weight'] != y['weight']:
                return -1 if x['weight'] < y['weight'] else 1
            return 0
 
        ngram_words = {}
        # 获取query的拼音 ["南京"]
        word_pinyins = self.pinyin_generator.get_word_simple_pinyins(clean_word, pypinyin.NORMAL)
        for word_pinyin in word_pinyins:
            # 拼音
            s = Search(using=self.es_client, index=ES_INDEX, doc_type='pinyin_ngram')\
                    .query(Q({'match': {'non_chinese_word.analyzed': word_pinyin}}))\
                    .params(size=10, request_timeout=self.ES_CONNECTION_TIMEOUT)
            response = s.execute()
            for hit in response:
                kwargs = {
                    'params': {
                        'word_pinyin': word_pinyin,
                        'non_chinese_word': hit.non_chinese_word
                    },
                    'output': hit.output,
                    'score': hit.meta.score,
                    'weight': hit.weight
                }
                update(ngram_words, **kwargs)

            # 非中文
            s = Search(using=self.es_client, index=ES_INDEX, doc_type='non_chinese_word')\
                    .query(Q({'match': {'non_chinese_word.analyzed': word_pinyin}}))\
                    .params(size=10, request_timeout=self.ES_CONNECTION_TIMEOUT)
            response = s.execute()
            for hit in response:
                kwargs = {
                    'params': {
                        'word_pinyin': word_pinyin,
                        'non_chinese_word': hit.non_chinese_word
                    },
                    'output': hit.non_chinese_word,
                    'score': hit.meta.score,
                    'weight': hit.weight
                } 
                update(ngram_words, **kwargs)

        sorted_ngram_words = sorted(ngram_words.items(),
                cmp=lambda x, y: compare(x[1], y[1]), reverse=True)

        sorted_ngram_words = [(x[0], x[1]['params']) for x in sorted_ngram_words]
        return sorted_ngram_words


    def get_candidate_from_ngram(self, clean_word, ngram_words):
        candidate = []
        lst_word_first_letters = self.get_word_first_letters(clean_word)
        for ngram_output, param in ngram_words:
            logging.debug(ngram_output)
            remain_sequence = get_remain_sequence(ngram_output, clean_word)
            remain_are_all_digits = all_in_list(remain_sequence, arabic_digits + chinese_digits)
            if remain_are_all_digits:
                logging.debug('ngram: 纯数字')
                response = {
                    'status': 'right',
                    'msg': u'ngram: 去掉公共字符串后是纯数字',
                    'candidate': []
                }
                return response
            else:
                query_chinese_sequence = get_chinese_sequence(clean_word)
                output_chinese_sequence = get_chinese_sequence(ngram_output)
                if len(query_chinese_sequence) == len(output_chinese_sequence):
                    logging.debug('ngram: 中文字符串长度相同')
                    edit_distance = damerau_levenshtein_distance(
                            param['word_pinyin'], param['non_chinese_word'])
                    if edit_distance <= 2:
                        logging.debug('ngram: 编辑距离符合要求')
                        if clean_word[0] == ngram_output[0]:
                            logging.debug('ngram: 首字母相同')
                            candidate.append(ngram_output)
                        elif query_chinese_sequence:
                            diff_ratio = len(remain_sequence) * 1.0 / len(query_chinese_sequence) / 2
                            if diff_ratio < 0.3:
                                logging.debug('ngram: 字形不同所占的比例符合要求')
                                candidate.append(ngram_output)
                elif clean_word[0] != ngram_output[0] and clean_word[0] != param['non_chinese_word'][0]:
                    logging.debug('ngram: 首字符不同, 拼音首字符不同')
                    continue
                else:
                    long_common_sequence = get_lcs(param['word_pinyin'], param['non_chinese_word'])
                    if len(long_common_sequence) == \
                            min(len(param['word_pinyin']), len(param['non_chinese_word'])):
                        logging.debug('ngram: 全拼音有包含关系')
                        candidate.append(ngram_output)
                    else:
                        first_letters_has_inclusion_relation = False
                        lst_output_first_letters = self.get_word_first_letters(ngram_output)
                        if len(lst_word_first_letters) <= 1 or len(lst_output_first_letters) <= 1:
                            logging.debug('ngram: query或output的首字母长度为1')
                            continue
                        for output_first_letters in lst_output_first_letters:
                            for word_first_letters in lst_word_first_letters:
                                long_common_sequence = get_lcs(output_first_letters, word_first_letters)
                                if (len(long_common_sequence) == len(output_first_letters)
                                    or len(long_common_sequence) == len(word_first_letters)):
                                    first_letters_has_inclusion_relation = True
                                    break
                            if first_letters_has_inclusion_relation:
                                break
                        if first_letters_has_inclusion_relation:
                            logging.debug('ngram: 首字母有包含关系')
                            candidate.append(ngram_output)
    
        if candidate:
            response = {
                'status': 'need_correct',
                'msg': 'ngram',
                'candidate': candidate
            }
        else:
            response = {
                'status': 'no_solution',
                'msg': 'no solution',
                'candidate': []
            }
        return response
        
    def get_correct_words(self, word):
        response = {
            'status': 'no_solution',
            'msg': 'no_solution',
            'candidate': []
        }
        if not word:
            response['status'] = 'right'
            response['msg'] = u'word is empty: %s' % word
            return response

        clean_word = self.get_clean_word(word)
        if not clean_word:
            response['status'] = 'right'
            response['msg'] = u'clean_word is empty: %s, %s' % (word, clean_word)
            return response

        # 1. 强制纠错
        forced_word = self.get_forced_word(clean_word)
        if forced_word is not None:
            if forced_word == clean_word:
                response['status'] = 'right'
                response['msg'] = u'强制不纠错'
            else:
                response['status'] = 'need_correct'
                response['msg'] = u'强制纠错'
                response['candidate'] = [forced_word]
            return response

        # 2. 精确匹配：看看这个词是不是在 non_chinese_ngram 或者chinese_ngram中精确匹配到呢？
        # 这个策略是有问题的：1-2gram 对这种term精确匹配没有意义
        # 无效代码
        if self.has_exact_match(clean_word):
            response['status'] = 'right'
            response['msg'] = u'精确匹配'
            return response

        # 3. 同音异形字策略
        homophones = self.get_homophones(clean_word) 
        if homophones:
            homophones_response = self.get_candidate_from_homophones(clean_word, homophones)
            return homophones_response

        # 4. 形近字
        # 对那些常见的近似字做笛卡尔积，进行替换，这样会造成doc大量的上升
        similar_forms = self.get_similar_forms(clean_word)
        if similar_forms:
            response['status'] = 'need_correct'
            response['msg'] = u'形近字'
            response['candidate'] = similar_forms
            return response

        # 5. ngram
        ngram_words = self.get_ngram_words(clean_word)
        if ngram_words:
            ngram_response = self.get_candidate_from_ngram(clean_word, ngram_words)
            return ngram_response

        return response


if __name__ == '__main__':
    import sys
    word = sys.argv[1].decode('utf8')

    corrector = Corrector()
    response = corrector.get_correct_words(word)
    if response:
        print response['status']
        print response['msg']
        for word in response['candidate']:
            print word
    else:
        print response 




 
