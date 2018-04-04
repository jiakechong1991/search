#coding:utf8
"""
generate pinyin by words

inputfile:[word_weight.txt]
word  weight
数据结构  100

http://pypinyin.readthedocs.org/en/master/
"""
import sys, os, json
import logging, itertools, copy, argparse, codecs
import pypinyin
from lang_conf import FUZZY_INITIALS, FUZZY_FINALS 

class PinyinGenerator():
    def __init__(self, **args):
        self.styles = args.get('styles', {})
        self.heteronym = args.get('heteronym', True)
        self.errors = args.get('errors', 'default')
        self.args = args.get('args', {})

    def load_fuzzy(self, lst_fuzzy, mutual=True):
        self.dic_fuzzy = {}
        for lst_one in lst_fuzzy:
            if len(lst_one) != 2:
                continue
            self.dic_fuzzy[lst_one[0]] = lst_one[1]
            if mutual:
                self.dic_fuzzy[lst_one[1]] = lst_one[0]


    def cut_word_pinyin_lst(self, word_pinyin_lst, max_len):
        """
            args:
                lst_pinyin  ..
                max_len     若pinyin的总长大于max_len，需要截断
            ret:
                截取lst_pinyin的前i个pinyin，使得join后的总长刚好不大于max_len                
            e.g.
                cut_pinyin(['welcome to', 'zhong', 'guo'], 16)  # join总长是18
                --> ['welcome to', 'zhong']  # join总长是15 
        """
        if len(''.join(word_pinyin_lst)) <= max_len:
            return word_pinyin_lst
        length = 0
        for i, char_pinyin in enumerate(word_pinyin_lst):
            length += len(char_pinyin)
            if length >= max_len:
                return word_pinyin_lst[:i]

    def cut_word_pinyins_lst(self, word_pinyins_lst, max_len):
        cut_result = []
        for word_pinyin_lst in word_pinyins_lst:
            cut_result.append(self.cut_word_pinyin_lst(word_pinyin_lst, max_len))
        return cut_result


    def seg(self, chars, errors):
        """ 按照pypinyin的分词（字）策略，将字符串转成list """
        hans = pypinyin.seg(chars)
        char_lst = []
        for words in hans:
            if pypinyin.RE_HANS.match(words):
                char_lst += list(words)
            else:
                for word in pypinyin.simple_seg(words):
                    if not (pypinyin.RE_HANS.match(word)):
                        py = pypinyin.handle_nopinyin(word, errors=errors)
                        char_lst.append(py[0]) if py else None
                    else:
                        char_lst += self.seg(word, errors)
        return char_lst


    def get_word_simple_pinyins(self, word, style, heteronym=True, errors='default', **args):
        pinyins_of_chars = pypinyin.pinyin(word, style, heteronym, errors)
        word_pinyins_lst = itertools.product(*pinyins_of_chars)
        if args.get('cut_max_len', None):
            cut_max_len = args['cut_max_len']
            word_pinyins_lst = self.cut_word_pinyins_lst(word_pinyins_lst, cut_max_len)
        word_pinyins = [''.join(x) for x in word_pinyins_lst] 
        return word_pinyins

    def get_initial_final(self, char_pinyin):
        """ 得到拼音的声母和韵母 """
        initials = 'bpmfdtnlgkhjqxzcsryw'
        if char_pinyin[0] in initials:
            dividing_pos = 1 if char_pinyin[1] != 'h' else 2  # zcs, zhchsh
        else:
            dividing_pos = 0    # 只有韵母，没有声母
        return char_pinyin[:dividing_pos], char_pinyin[dividing_pos:]


    # 不包含本身！
    # 请确保char_pinyin是拼音
    def get_char_fuzzy_pinyins(self, char_pinyin, fuzzy_flag='all'):
        """
            args:   
                char_pinyin
                fuzzy_flag: ['all', 'single']
                    all: 新生成的pinyin，可以既模糊声母，也模糊韵母，包含不模糊的情况（即原始拼音）。
                    single: 新生成的pinyin，要么模糊声母，要么模糊韵母，不包含不模糊的情况。
            ret:    fuzzy of the pinyin
            e.g.    'chuang' --> ['ch'] ['uang'] 
                    --> ['ch', 'c'] ['uang', 'uan']
                    fuzzy_flag='all'的结果：['chuang', 'cuang', 'chuan', 'cuan']
                    fuzzy_flag='single'的结果：['cuang', 'chuan']
        """
        if not hasattr(self, 'dic_fuzzy'):
            self.load_fuzzy(FUZZY_INITIALS)
            self.load_fuzzy(FUZZY_FINALS)
        initials_lst = 'bpmfdtnlgkhjqxzcsryw' # zcs, zhchsh
        initial, final = self.get_initial_final(char_pinyin)
        if fuzzy_flag == 'all':
            initials, finals = [initial], [final]
            initials.append(self.dic_fuzzy[initial]) if self.dic_fuzzy.has_key(initial) else None
            finals.append(self.dic_fuzzy[final]) if self.dic_fuzzy.has_key(final) else None
            char_fuzzy_pinyins_lst = list(itertools.product(initials, finals))
            char_fuzzy_pinyins = [''.join(x) for x in char_fuzzy_pinyins_lst]
            char_fuzzy_pinyins.remove(char_pinyin)
        elif fuzzy_flag == 'single':
            char_fuzzy_pinyins = []
            char_fuzzy_pinyins.append(self.dic_fuzzy[initial] + final) if self.dic_fuzzy.has_key(initial) else None
            char_fuzzy_pinyins.append(initial + self.dic_fuzzy[final]) if self.dic_fuzzy.has_key(final) else None
        else:
            logging.warning('get_fuzzy_char_pinyin() no flag named %s', fuzzy_flag)
        return char_fuzzy_pinyins


    def get_word_fuzzy_pinyins(self, chars, heteronym=True, errors='default', char_fuzzy_flag='all', chars_fuzzy_flag='all', **args):
        """
            args: word
            process: word -> word_pinyin -> char_pinyin -> fuzzy_char_pinyin
            return: like result of pypinyin.pinyin()
            e.g.: u'光彩' -> [['guang'], ['cai']]
                            -> ['guang'] --> ['guang', 'guan']
                            -> ['cai'] --> ['cai', 'chai']
                          -> ret: [['guang', 'guan'], ['cai', 'chai']]
        """
        char_lst = self.seg(chars, errors)
        pinyins_of_chars = pypinyin.pinyin(chars, pypinyin.NORMAL, heteronym, errors)
        if chars_fuzzy_flag == 'all':
            fuzzy_pinyins_of_chars = [set() for x in char_lst]
            for char_no, pinyins_of_char in enumerate(pinyins_of_chars):
                for char_pinyin in pinyins_of_char:
                    fuzzy_pinyins_of_chars[char_no].add(char_pinyin)
                    if char_pinyin != char_lst[char_no]:   # 如果经过拼音转换后，发生变化。说明原词是有拼音的，也就是转换之后的结果是拼音，才能进行模糊
                        fuzzy_pinyins_of_char = self.get_char_fuzzy_pinyins(char_pinyin, char_fuzzy_flag)
                        fuzzy_pinyins_of_chars[char_no] |= set(fuzzy_pinyins_of_char)
            fuzzy_pinyins_lst_of_word = list(itertools.product(*fuzzy_pinyins_of_chars))
            if args.get('cut_max_len', None):
                cut_max_len = args['cut_max_len']
                fuzzy_pinyins_lst_of_word = self.cut_word_pinyins_lst(fuzzy_pinyin_lst_of_word)
            fuzzy_pinyins_of_word = [''.join(x) for x in fuzzy_pinyins_lst_of_word]
            # 把原始正确的pinyin删掉
            src_word_pinyins = list(itertools.product(*pinyins_of_chars))
            src_word_pinyins = [''.join(src_word_pinyin) for src_word_pinyin in src_word_pinyins]
            for src_word_pinyin in src_word_pinyins:
                fuzzy_pinyins_of_word.remove(src_word_pinyin)
            return fuzzy_pinyins_of_word
        elif chars_fuzzy_flag == 'single':
            fuzzy_pinyins_of_chars = [[] for x in char_lst]
            for char_no, pinyins_of_char in enumerate(pinyins_of_chars):
                for char_pinyin in pinyins_of_char:
                    if char_pinyin != char_lst[char_no]:
                        fuzzy_pinyins_of_char = self.get_char_fuzzy_pinyins(char_pinyin, char_fuzzy_flag)
                        fuzzy_pinyins_of_chars[char_no] += fuzzy_pinyins_of_char
            # 对于word的原始pinyin，将任一char的pinyin换成fuzzy_pinyin
            src_pinyins_of_word = list(itertools.product(*pinyins_of_chars))
            fuzzy_pinyins_lst_of_word = []
            for src_pinyin_of_word in src_pinyins_of_word:
                for char_no, fuzzy_pinyins_of_char in enumerate(fuzzy_pinyins_of_chars):
                    for fuzzy_char_pinyin in fuzzy_pinyins_of_char:
                        fuzzy_pinyin_lst_of_word = list(copy.deepcopy(src_pinyin_of_word))
                        fuzzy_pinyin_lst_of_word[char_no] = fuzzy_char_pinyin
                        fuzzy_pinyins_lst_of_word.append(fuzzy_pinyin_lst_of_word)
            if args.get('cut_max_len', None):
                cut_max_len = args['cut_max_len']
                fuzzy_pinyins_lst_of_word = self.cut_word_pinyins_lst(fuzzy_pinyins_lst_of_word, cut_max_len)
            fuzzy_pinyins_of_word = [''.join(x) for x in fuzzy_pinyins_lst_of_word]
            return set(fuzzy_pinyins_of_word)


    def get_word_mix_pinyins(self, chars, heteronym=True, errors='default', mix_styles={}, **args):
        """ 获取一个词的混合拼音。
            如：全拼和声母混合。
                '重要'：['zyao', 'zhongy', 'zy', 'zhongyao'] """
        if not mix_styles:
            return []
        char_lst = self.seg(chars, errors)
        pinyins_of_chars = [set() for x in char_lst]
        for mix_style in mix_styles:
            if mix_style in [pypinyin.NORMAL, pypinyin.FIRST_LETTER, pypinyin.INITIALS]:
                cur_pinyins_of_chars = pypinyin.pinyin(chars, mix_style, heteronym, errors)
                for i, cur_pinyins_of_char in enumerate(cur_pinyins_of_chars):
                    pinyins_of_chars[i] |= set(cur_pinyins_of_char)
            elif mix_style == 'fuzzy':
                normal_pinyins_of_chars = pypinyin.pinyin(chars, pypinyin.NORMAL, heteronym, errors)
                char_fuzzy_flag = mix_styles['fuzzy'].get('char_fuzzy_flag', 'all')
                for i, normal_pinyins_of_char in enumerate(normal_pinyins_of_chars):
                    for normal_pinyin_of_char in normal_pinyins_of_char:
                        if normal_pinyin_of_char != char_lst[i]:
                            pinyins_of_chars[i] |= set(self.get_char_fuzzy_pinyins(normal_pinyin_of_char, char_fuzzy_flag))
            elif mix_style == 'chinese':
                for i, c in enumerate(char_lst):
                    pinyins_of_char[i].add(c)
            else:
                 logging.warning('cannot recognize this mix_style: %s', mix_style)
        mix_pinyins_lst_of_word = itertools.product(*pinyins_of_chars)
        if args.get('cut_max_len', None):
            cut_max_len = args['cut_max_len']
            mix_pinyins_lst_of_word = self.cut_word_pinyins_lst(mix_pinyins_lst_of_word, cut_max_len)
        mix_pinyins_of_word = [''.join(x) for x in mix_pinyins_lst_of_word]
        return set(mix_pinyins_of_word)


    def get_style_weight(self, weight, style):
        if style == pypinyin.NORMAL:
            return weight - 1
        elif style == pypinyin.FIRST_LETTER:
            return weight - 2
        elif style == pypinyin.INITIALS:
            return weight - 3
        elif style == 'fuzzy':
            return weight / 2
        elif style == 'mix':
            return weight / 2 - 100

    def get_all_word_pinyins(self, word, styles, weight=0, heteronym=True, errors='default', **args):
        """ 获取styles中指定的所有形式的拼音。 """
        word_pinyins = {} if weight else set()
        for style in styles:
            if style in [pypinyin.NORMAL, pypinyin.FIRST_LETTER, pypinyin.INITIALS]: 
                cur_word_pinyins = self.get_word_simple_pinyins(word, style, heteronym, errors, **args)
            elif style == 'fuzzy':
                char_fuzzy_flag = styles['fuzzy'].get('char_fuzzy_flag', 'all')
                chars_fuzzy_flag = styles['fuzzy'].get('chars_fuzzy_flag', 'all')
                cur_word_pinyins = self.get_word_fuzzy_pinyins(word, heteronym, errors, char_fuzzy_flag, chars_fuzzy_flag, **args)
            elif style == 'mix':
                mix_styles = styles['mix'].get('mix_styles', {}) 
                cur_word_pinyins = self.get_word_mix_pinyins(word, heteronym, errors, mix_styles, **args)
            if weight:
                # cur_weight = self.get_style_weight(weight, style)
                cur_weight = weight
                for cur_word_pinyin in cur_word_pinyins:
                    word_pinyins.setdefault(cur_word_pinyin, 0)
                    if cur_weight > word_pinyins[cur_word_pinyin]:
                        word_pinyins[cur_word_pinyin] = cur_weight
            else:
                word_pinyins |= set(cur_word_pinyins)
        return word_pinyins

    def get_local_word_pinyins(self, word, weight=0):
        """ 获取self.styles中指定的所有形式的拼音。 """
        return self.get_all_word_pinyins(word, self.styles, weight, self.heteronym, self.errors, **self.args)


if __name__ == '__main__':
    obj = GetPinyinWeight()
    # word = u'数据结构'
    # word = u'穿上新衣裳'
    word = u'穿上'
    args = {'cut_max_len': 15}
    # result = obj.get_word_simple_pinyins(word,  pypinyin.INITIALS, True, 'default', **args) 
    # result = obj.get_word_fuzzy_pinyins(word, True, 'default', 'single', 'single', **args)
    mix_styles = {
        # pypinyin.NORMAL: {},
        pypinyin.FIRST_LETTER: {},
        pypinyin.INITIALS: {},
        # 'fuzzy': {'char_fuzzy_flag': 'single'}
    }
    styles = {
        pypinyin.NORMAL: {},
        'fuzzy': {},
        'mix': {
            'mix_styles': mix_styles
        }
    }
            
    # result = obj.get_word_mix_pinyins(word, True, 'default', mix_style, **args)
    result = obj.get_all_word_pinyins(word, styles=styles)
    print result
    print len(result)





