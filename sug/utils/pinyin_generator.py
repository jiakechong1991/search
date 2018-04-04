# coding: utf8
from copy import deepcopy
from itertools import product
import pypinyin

class PinyinGenerator():
    def __init__(self, word):
        self.word = word

    @property
    def chars(self):
        if not hasattr(self, '_chars'):
            self._chars = self.get_chars()
        return self._chars

    @property
    def first_letters(self):
        if not hasattr(self, '_first_letters'):
            self._first_letters = self.get_first_letters()
        return self._first_letters

    @property
    def pinyins(self):
        if not hasattr(self, '_pinyins'):
            self._pinyins = self.get_pinyins()
        return self._pinyins

    @property
    def initials(self):
        if not hasattr(self, '_initials'):
            self._initials = self.get_initials()
        return self._initials

    @property
    def fuzzy_pinyins(self):
        if not hasattr(self, '_fuzzy_pinyins'):
            self._fuzzy_pinyins = self.get_fuzzy_pinyins()
        return self._fuzzy_pinyins

    @property
    def mix_pinyins(self):
        if not hasattr(self, '_mix_pinyins'):
            self._mix_pinyins = self.get_mix_pinyins()
        return self._mix_pinyins

    @property
    def mix_pinyins_with_chinese(self):
        if not hasattr(self, '_mix_pinyins_with_chinese'):
            self._mix_pinyins_with_chinese = self.get_mix_pinyins_with_chinese()
        return self._mix_pinyins_with_chinese

    def get_chars(self):
        chars = []
        non_chinese_word = u''
        is_first_char = True
        for uchar in self.word:
            if uchar >= u'\u4e00' and uchar <= u'\u9fa5':  # 中文
                if not is_first_char:
                    chars.append(non_chinese_word)
                    non_chinese_word = u''
                    is_first_char = True
                chars.append(uchar)
            else:
                is_first_char = False
                non_chinese_word += uchar
        if not is_first_char:
            chars.append(non_chinese_word)
        return chars

    def get_pinyins(self):
        pinyin = pypinyin.pinyin(self.word, pypinyin.NORMAL, heteronym=True, errors='default')
        pinyins = product(*pinyin)
        return pinyins

    def get_first_letters(self):
        first_letter = pypinyin.pinyin(self.word, pypinyin.FIRST_LETTER, heteronym=True, errors='default')
        first_letters = product(*first_letter)
        return first_letters

    def get_initials(self):
        initial = pypinyin.pinyin(self.word, pypinyin.INITIALS, heteronym=True, errors='default')
        initials = product(*initial)
        return initials

    def get_shengmu_yunmu_from_pinyin(self, char_pinyin):
        if not char_pinyin:
            shengmu = ''
            yunmu = ''
        elif char_pinyin[0] in 'bpmfdtnlgkhjqxryw':
            shengmu = char_pinyin[0]
            yunmu = char_pinyin[1:]
        elif char_pinyin[0] in 'zcs':
            if len(char_pinyin) > 1 and char_pinyin[1] == 'h':
                shengmu = char_pinyin[:2]
                yunmu = char_pinyin[2:]
            else:
                shengmu = char_pinyin[0]
                yunmu = char_pinyin[1:]
        else:
            shengmu = ''
            yunmu = char_pinyin
        return (shengmu, yunmu)

    def get_fuzzy_char_pinyin(self, char_pinyin):
        if not char_pinyin:
            return [char_pinyin]
        FUZZY_SHENGMU = [['c', 'ch'], ['s', 'sh'], ['z', 'zh']]
        FUZZY_YUNMU = [['an','ang'], ['en','eng'], ['in','ing'], ['ian','iang'], ['uan','uang']]

        shengmu, yunmu = self.get_shengmu_yunmu_from_pinyin(char_pinyin)

        fuzzy_shengmu, fuzzy_yunmu = set([shengmu]), set([yunmu])
        for group in FUZZY_SHENGMU:
            if shengmu in group:
                fuzzy_shengmu |= set(group)
        for group in FUZZY_YUNMU:
            if yunmu in group:
                fuzzy_yunmu |= set(group)

        fuzzy_pinyin = product(list(fuzzy_shengmu), list(fuzzy_yunmu))
        fuzzy_pinyin = [''.join(x) for x in fuzzy_pinyin]
        return fuzzy_pinyin

    def get_fuzzy_pinyin(self):
        word_pinyin = pypinyin.pinyin(self.word, pypinyin.NORMAL, heteronym=True, errors='default')
        fuzzy_word_pinyin = []
        for lst_char_pinyin in word_pinyin:
            set_fuzzy_char_pinyin = set()
            for char_pinyin in lst_char_pinyin:
                fuzzy_char_pinyin = self.get_fuzzy_char_pinyin(char_pinyin)
                set_fuzzy_char_pinyin |= set(fuzzy_char_pinyin)
            fuzzy_word_pinyin.append(list(set_fuzzy_char_pinyin))
        return fuzzy_word_pinyin

    def get_fuzzy_pinyins(self):
        fuzzy_pinyin = self.get_fuzzy_pinyin()
        fuzzy_pinyins = product(*fuzzy_pinyin)
        return fuzzy_pinyins

    def get_mix_pinyin(self):
        pinyin = pypinyin.pinyin(self.word, pypinyin.NORMAL, heteronym=True, errors='default')
        first_letter = pypinyin.pinyin(self.word, pypinyin.FIRST_LETTER, heteronym=True, errors='default')
        initial = pypinyin.pinyin(self.word, pypinyin.INITIALS, heteronym=True, errors='default')
        fuzzy_pinyin = self.get_fuzzy_pinyin()
        
        mix_pinyin = [[] for i in range(len(pinyin))]
        for i, _ in enumerate(pinyin):
            mix_pinyin[i] = list(set(pinyin[i])\
                | set(first_letter[i])\
                | set(initial[i])\
                | set(fuzzy_pinyin[i]))
        return mix_pinyin 
        
    def get_mix_pinyins(self):
        mix_pinyin = self.get_mix_pinyin()
        mix_pinyins = product(*mix_pinyin)
        return mix_pinyins

    def get_mix_pinyin_with_chinese(self):
        mix_pinyin = self.get_mix_pinyin()
        chars = self.get_chars()
        mix_pinyin_with_chinese = []
        for i, _ in enumerate(chars):
            mix_pinyin_with_chinese.append(list(set(mix_pinyin[i]) | set([chars[i]])))
        return mix_pinyin_with_chinese

    def get_mix_pinyins_with_chinese(self):
        mix_pinyin_with_chinese = self.get_mix_pinyin_with_chinese()
        mix_pinyins_with_chinese = product(*mix_pinyin_with_chinese)
        return mix_pinyins_with_chinese


if __name__ == '__main__':
    word = u'种子'
    word = u'苍蝇'
    word = u'船上'
    word = u'hello, China! hello 陆喆!'
    obj = PinyinGenerator(word)
    # xx = obj.pinyins
    # xx = obj.first_letters
    # xx = obj.initials
    # xx = obj.fuzzy_pinyins
    # xx = obj.mix_pinyins

    xx = obj.mix_pinyins_with_chinese
    for x in xx:
        print x

    # xx = obj.chars
    # print xx
