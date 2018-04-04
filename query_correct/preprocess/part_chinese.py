# coding: utf8
"""
    infile format: word\tweight
    如果word包含中文，则将该行保存到chinese_output
    否则，保存到non_chinese_output
"""
import argparse
import codecs
import os
import sys

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))
from utils.common import has_in_range
from utils.lang_conf import chinese_range

def part_chinese(file_in, chinese_file_ot, non_chinese_file_ot):
    with codecs.open(file_in, encoding='utf8') as f:
        chinese_wf = codecs.open(chinese_file_ot, mode='w', encoding='utf8')
        non_chinese_wf = codecs.open(non_chinese_file_ot, mode='w', encoding='utf8')
        for line in f:
            word = line.split('\0')[0]
            if has_in_range(word, chinese_range):
                chinese_wf.write(line)
            else:
                non_chinese_wf.write(line) 
        chinese_wf.close()
        non_chinese_wf.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_in')
    parser.add_argument('--chinese_file_ot')
    parser.add_argument('--non_chinese_file_ot')
    args = parser.parse_args()

    part_chinese(args.file_in, args.chinese_file_ot, args.non_chinese_file_ot)
