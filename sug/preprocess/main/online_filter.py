# coding: utf8
import argparse
import codecs
import json
import logging
import requests
import sys
import urllib

from config.conf import LOGGING_FORMAT
from utils.common import get_row_num

def get_word_num(word):
    url = 'http://10.0.2.152:9999/search'
    params = {'query': word, 'qt': 1, 'st': 1, 'num': 1}
    word_num = 0
    try:
        result = requests.get(url, data=params, timeout=3)
        status_code = int(result.status_code)
        if status_code != 200:
            logging.error('status_code: %s, url: %s', status_code, url)
        else:
            result = result.json()
            word_num = result['total']['all']
    except Exception, e:
        logging.error(e)
        logging.error(params)
    finally:
        return word_num


def proc_file(file_in, file_ot):
    row_num = get_row_num(file_in)
    wf = codecs.open(file_ot, 'w', encoding='utf8')
    with codecs.open(file_in, encoding='utf8') as f:
        for line_no, line in enumerate(f):
            if line_no % 100 == 0:
                logging.info('finished: %s/%s', line_no, row_num) 
            row = json.loads(line)
            word = row['word']
            word_num = get_word_num(word)
            new_row = {'word': word, 'num': word_num}
            wf.write(json.dumps(new_row, ensure_ascii=False, sort_keys=True) + '\n')
    wf.close()


if __name__ == '__main__':
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter(LOGGING_FORMAT))
    root.addHandler(ch)

    http_connection_logger = logging.getLogger('requests.packages.urllib3.connectionpool')
    http_connection_logger.setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_in', required=True)
    parser.add_argument('--file_ot', required=True)
    args = parser.parse_args()

    proc_file(args.file_in, args.file_ot)



