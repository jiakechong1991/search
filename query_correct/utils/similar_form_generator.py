# coding: utf8
import argparse
import codecs
import logging
import os
import sys
from common import in_range
from lang_conf import chinese_range

class SimilarFormGenerator():
    def load_similar_form(self, filename=('resource/similar_form.txt')):
        self.similar_words = []
        self.word_dict = {}
        with codecs.open(filename, encoding='utf8') as f:
            for no, line in enumerate(f):
                line = line.strip('\r\n')
                words = line.split(',')
                self.similar_words.append(words)
                for word in words:
                    self.word_dict.setdefault(word, [])
                    self.word_dict[word].append(no)

    def get_similar_word(self, word, weight=-1):
        """
            注意：
                1、生成的形近词集合不包含自己。
                2、集合中的每个形近词与word的编辑距离都为1。
        """
        if not hasattr(self, 'word_dict'):
            self.load_similar_form()
        result = []
        for i, c in enumerate(word):
            if not in_range(c, chinese_range):
                continue 
            if c in self.word_dict:
                pre_word, last_word = word[:i], word[i+1:]
                similar_set = set()
                for no in self.word_dict[c]:
                    similar_set = similar_set.union(set(self.similar_words[no]))
                similar_set.remove(c)
                for similar_c in similar_set:
                    similar_word = pre_word + similar_c + last_word
                    if weight != -1:      # 有潜在隐患...
                        new_weight = weight
                        result.append((similar_word, new_weight))
                    else:
                        result.append(similar_word)
        return result

