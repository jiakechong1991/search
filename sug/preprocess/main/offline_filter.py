# coding: utf8
import argparse
import codecs
import json

def get_word_num(filename):
    word_num = {}
    with codecs.open(filename, encoding='utf8') as f:
        for line_no, line in enumerate(f):
            row = json.loads(line)
            word_num[row['word']] = row['num']
    return word_num


def add_new_words(new_words, file_word_num):
    with codecs.open(file_word_num, 'a', encoding='utf8') as wf:
        for word in new_words:
            row = {'word': word, 'num': 0}
            wf.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + '\n')


def offline_filter(file_in, file_ot, file_word_num):
    word_num = get_word_num(file_word_num)
    new_words = []
    wf = codecs.open(file_ot, 'w', encoding='utf8')
    with codecs.open(file_in, encoding='utf8') as f:
        for line_no, line in enumerate(f):
            row = json.loads(line)
            word = row['output']
            if word not in word_num:
                new_words.append(word)
            elif word_num[word] != 0:
                wf.write(line)
    wf.close()
    
    add_new_words(new_words, file_word_num)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_in', required=True)
    parser.add_argument('--file_ot', required=True)
    parser.add_argument('--file_word_num', required=True)
    args = parser.parse_args()

    offline_filter(args.file_in, args.file_ot, args.file_word_num)
