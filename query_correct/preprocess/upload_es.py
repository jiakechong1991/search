import argparse
import hashlib
from datetime import datetime
import logging
import os
import sys

from elasticsearch import Elasticsearch, helpers

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))
from config.conf import ES_HOSTS, ES_INDEX, LOGGING_FORMAT

def get_diff_data(filename):
    def get_new_data():
        new_data = {}
        with open(os.path.join('data', filename)) as f:
            for line in f:
                items = line.strip('\r\n').split('\0')
                key = '\0'.join(items[:-1])
                weight = int(items[-1])
                new_data[key] = weight
        return new_data

    diff_data = {
        'delete': [],
        'index': [],
        'same': [],
        'update': [],
    }
    new_data = get_new_data()
    old_file_in = os.path.join('old_data', filename)
    if os.path.exists(old_file_in):
        with open(old_file_in) as f:
            for line in f:
                items = line.strip('\r\n').split('\0')
                key = '\0'.join(items[:-1])
                old_weight = int(items[-1])
                if key not in new_data:
                    diff_data['delete'].append(items)
                else:
                    new_weight = new_data.pop(key)
                    if old_weight == new_weight: # same
                        diff_data['same'].append(items)
                    else:
                        diff_data['update'].append(items[:-1] + [new_weight])

    for key, new_weight in new_data.items():
        diff_data['index'].append(key.split('\0') + [new_weight])

    logging.info('delete: %s' % len(diff_data['delete']))
    logging.info('index: %s' % len(diff_data['index']))
    logging.info('same: %s' % len(diff_data['same']))
    logging.info('update: %s' % len(diff_data['update']))

    return diff_data
 

def upload(actions):
    logging.info('actions: %s' % len(actions)) 
    es_client = Elasticsearch(ES_HOSTS, port=9200, timeout=300)
    step = 1000
    for i in range(0, len(actions), step):
        res = helpers.bulk(es_client, actions[i:i+step], stats_only=True, raise_on_error=False)
        logging.info(res)
        if res[1] != 0:
            logging.error('bulk failed!')

def work(filename, es_index, doc_type):
    diff_data = get_diff_data(filename)
    actions = [] 
    for _op_type, rows in diff_data.items():
        if _op_type in ['same']:
            continue
        for row in rows:
            action = {
                '_op_type': _op_type,
                '_index': es_index,
                '_type': doc_type,
            }
            if doc_type == 'chinese_ngram':
                word, weight = row
                _id = hashlib.md5(word).hexdigest()
                doc = {
                    'chinese_word': word.decode('utf8'),
                    'weight': weight
                }
            elif doc_type == 'non_chinese_ngram':
                word, weight = row
                _id = hashlib.md5(word).hexdigest()
                doc = {
                    'non_chinese_word': word.decode('utf8'),
                    'weight': weight
                }
            elif doc_type == 'similar_form_no_ngram':
                similar_word, output, weight = row
                _id = hashlib.md5(similar_word + output).hexdigest()
                doc = {
                    'similar_word': similar_word.decode('utf8'),
                    'output': output.decode('utf8'),
                    'weight': weight
                }
            elif doc_type == 'pinyin_ngram':
                pinyin, output, weight = row
                _id = hashlib.md5(pinyin + output).hexdigest()
                doc = {
                    'non_chinese_word': pinyin.decode('utf8'),
                    'output': output.decode('utf8'),
                    'weight': weight
                }
            else:
                continue
            doc['_ut'] = datetime.now() 
            if _op_type == 'index':
                action['_source'] = doc
            elif _op_type == 'update':
                action['doc'] = doc
            action['_id'] = _id
            actions.append(action)
    upload(actions)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('--doc_type', required=True)
    args = parser.parse_args()

    es_index = ES_INDEX 
    work(args.filename, es_index, args.doc_type)
