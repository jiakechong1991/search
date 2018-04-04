# coding: utf8
import logging, re
from lang_conf import *
from common import *

class WordCleaner():
    ignore_range = [('+', '+')]
    allow_range = chinese_range + foreign_range + digit_range + ignore_range

    def discard_punctuation(self, word):
        punctuations = set([u'、', u'；', u'：', u'。', u'，', u'—', u'！', u'？', u'-', 
            u'#', u'‘', u'’', u'“', u'”', u'＆', u'·', u'Ⅰ', u'Ⅱ', u'•', 
            u'－', u'®', u'—', u'─', u'–',
            u'/', u'\\', u';', u':', u'.', u',', u'_', u'!', u'?', u'-', u'#', 
            u'\'', u'"', u'&', u'|', u'——', u'*'
        ])
        for no, c in enumerate(word):
            if c in punctuations:
                word = replace_pos(word, no, ' ')
        return word  


    def discard_bracket(self, word):
        discard_bracket_first_letters  = chinese_digits + [u'上', u'下']    # 如果括号内以此开头
        bracket_types = [
            (('(', u'（'), (')', u'）')), 
            ((u'《', '<'), (u'》', '>')),
            ((u'【', '['), (u'】', ']'))
        ]
        src_word = word
        for left_brackets, right_brackets in bracket_types:
            # disgard double(match) brackets, include different format, such as (haha）
            # if like (2016春) (2) (上), delete all
            # else like (计算机), replace '(' ')' with ' '
            while True:
                no = 0
                # 找配对的左右括号
                end = word.find(right_brackets[no])
                if end == -1:
                    end = word.find(right_brackets[1])
                    if end != -1:
                        no = 1
                    else:
                        break
                beg = word.rfind(left_brackets[no], 0, end)
                # 左括号找不着
                if beg == -1:
                    beg = word.rfind(left_brackets[(no+1)%2], 0, end)  # # 找中英不同形式的左括号
                    if beg == -1:
                        logging.warning('mismatching: lack left bracket %s in %s', left_brackets[no], src_word)
                        word = replace_pos(word, end, ' ')
                        break
                    else:
                        logging.warning('mismatching: left and right brackets have different form %s -- %s in %s', left_brackets[(no+1)%2], right_brackets[no], src_word)
                if in_range(word[beg+1], digit_range) or word[beg+1] in discard_bracket_first_letters: 
                    word = word[:beg]+word[end+1:]
                else:
                    word = replace_pos(word, beg, ' ')
                    word = replace_pos(word, end, ' ')
            # 只有单边括号
            # disgard single bracket, replace with space
            while True:
                single_flag = False
                for bracket in (left_brackets+right_brackets):
                    pos = word.find(bracket)
                    if pos != -1:
                        logging.warning('mismatching: just have one side bracket %s in %s', bracket, src_word)
                        word = replace_pos(word, pos, ' ')
                        single_flag = True
                if not single_flag:
                    break
        return word


    def discard_strange_char(self, word):
        strange_chars = [chr(11), u'\u200b']  # 垂直制表符，zero width space
        for c in strange_chars:
            if c in word:
                pos = word.find(c)
                word = replace_pos(word, pos, ' ')
        return word

    # only reserve the space between digits and letters
    def process_space(self, word):
        word = ' '.join(word.split())   # continuous spaces --> one space, and strip header & trailor space
        thrown_space_poses = []         # pos of spaces which should be thrown
        pos = word.find(' ')
        cur_range = foreign_range + digit_range
        while pos != -1:
            if not(in_range(word[pos-1], cur_range) and in_range(word[pos+1], cur_range)):
                thrown_space_poses.append(pos)
            pos = word.find(' ', pos+1)
        return replace_poses(word, thrown_space_poses, '') 


    def extract_strange(self, word):
        strange_poses, strange_set = [], set()
        for no, c in enumerate(word):
            if not_in_range(c, self.allow_range):
                strange_poses.append(no)
        new_word = replace_poses(word, strange_poses, ' ')
        return new_word, strange_set


    def clean(self, word):
        word = word.lower()
        funcs = [self.discard_punctuation, self.discard_strange_char, self.discard_bracket]
        for func in funcs:
            word = func(word)
        word, strange_set = self.extract_strange(word)
        return self.process_space(word), strange_set


if __name__ == '__main__':
    obj = WordCleaner()
    import codecs
    filename = 'data/word.txt'    
    outfile = 'tmp.txt'
    strange_set = set()
    wf = codecs.open(outfile, mode='w', encoding='utf8')
    with codecs.open(filename, encoding='utf8') as f:
        for line in f:
            line = line.strip('\r\n')
            word, weight = line.split('\t')
            new_word, cur_set = obj.clean(word) 
            wf.write('%s\n' % word)
            wf.write('%s\n' % new_word)
            strange_set |= cur_set
    wf.close()
    for c in strange_set:
        print c



