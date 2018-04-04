# coding: utf8
import argparse
import codecs
from collections import defaultdict
import copy
import json
import logging

from config.conf import MIN_WORD_LEN, MAX_WORD_LEN, MAX_MIX_NUM, LOGGING_FORMAT
from utils.common import get_row_num
from utils.pinyin_generator import PinyinGenerator

def dump_pinyin_weight(file_in, file_ot, args):
    wf = codecs.open(file_ot, 'w', encoding='utf8')
    row_num = get_row_num(file_in) 
    with codecs.open(file_in, encoding='utf8') as f:
        for line_no, line in enumerate(f):
            if line_no % 10000 == 0:
                logging.info('finished: %s/%s', line_no, row_num)
            try:
                row = json.loads(line)
            except Exception, e:
                print e
                print line_no, line
                raise Exception(e)
            word = row['input']
            generator = PinyinGenerator(word)

            lst_pinyin_weight = []
            try:
                if args.FULL_PINYIN:
                    new_weight = row['weight'] - 1
                    lst_pinyin_weight.append((new_weight, [''.join(x) for x in generator.pinyins]))
                if args.FIRST_LETTER:
                    new_weight = row['weight'] - 2
                    lst_pinyin_weight.append((new_weight, [''.join(x) for x in generator.first_letters]))
                if args.INITIAL:
                    new_weight = row['weight'] - 3
                    lst_pinyin_weight.append((new_weight, [''.join(x) for x in generator.initials]))
                if args.FUZZY_PINYIN:
                    new_weight = row['weight'] / 2
                    lst_pinyin_weight.append((new_weight, [''.join(x) for x in generator.fuzzy_pinyins]))
                all_pinyins = set()
                for weight, pinyins in lst_pinyin_weight:
                    all_pinyins |= set(pinyins)
 
                if len(all_pinyins) < MAX_MIX_NUM and len(word) < 6:
                    if args.MIX_PINYIN_WITH_CHINESE:
                        new_weight = row['weight'] / 2 - 100
                        lst_pinyin_weight.append(
                            (new_weight, [''.join(x) for x in generator.mix_pinyins_with_chinese]))
                    elif args.MIX_PINYIN:
                        new_weight = row['weight'] / 2 - 100
                        lst_pinyin_weight.append((new_weight, [''.join(x) for x in generator.mix_pinyins]))
            except Exception, e:
                print line_no, line
                raise Exception(e) 
 
            new_input_weight = defaultdict(int)
            for weight, pinyins in lst_pinyin_weight:
                for pinyin in pinyins:
                    if weight > new_input_weight[pinyin]:
                        new_input_weight[pinyin] = weight

            for _input, weight in new_input_weight.items():
                if len(_input) <= MAX_WORD_LEN and len(_input) >= MIN_WORD_LEN:
                    new_row = copy.deepcopy(row)
                    new_row['input'] = _input
                    new_row['weight'] = weight
                    wf.write(json.dumps(new_row, sort_keys=True, ensure_ascii=False) + '\n')
    wf.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_in', required=True)
    parser.add_argument('--file_ot', required=True)
    parser.add_argument('--FULL_PINYIN', action='store_true')
    parser.add_argument('--FIRST_LETTER', action='store_true')
    parser.add_argument('--INITIAL', action='store_true')
    parser.add_argument('--FUZZY_PINYIN', action='store_true')
    parser.add_argument('--MIX_PINYIN', action='store_true')
    parser.add_argument('--MIX_PINYIN_WITH_CHINESE', action='store_true')
    args = parser.parse_args()

    dump_pinyin_weight(args.file_in, args.file_ot, args)




