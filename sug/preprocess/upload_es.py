# coding: utf8
import argparse
import codecs
from datetime import datetime
import hashlib
import json
import logging
import sys

from elasticsearch import Elasticsearch, helpers

from config.conf import ES_HOSTS, ES_INDEX, LOGGING_FORMAT
from utils.common import get_row_num

def get_actions(file_in, es_index, doc_type):
    actions = []
    row_num = get_row_num(file_in)
    with codecs.open(file_in, encoding='utf8') as f:
        for line_no, line in enumerate(f):
            if line_no % 10000 == 0 and actions:
                upload(actions)
                actions = []
            row = json.loads(line)

            _op_type = row.pop('_op_type')
            if _op_type == 'same':
                continue

            _id = hashlib.md5((row['input']+row['output']).encode('utf8')).hexdigest()
                
            action = {
                '_op_type': _op_type,
                '_index': es_index,
                '_type': doc_type,
                '_id': _id
            }

            suggest_field = doc_type

            doc = {
                suggest_field: { 
                    'input': row.pop('input'),
                    'weight': row.pop('weight'),
                    'output': row.pop('output'),
                    'payload': {
                        'record_id': _id,
                        '_ut': datetime.now()
                    }
                }
            }
            doc[suggest_field]['payload'].update(row)

            if _op_type == 'index':
                action['_source'] = doc
            elif _op_type == 'update':
                action['doc'] = doc

            actions.append(action)

    return actions


def get_considerable_errors(errors):
    """ 不可忽视的错误. """
    considerable_errors = []
    if not errors:
        return considerable_errors
    for error in errors:
        _op_type, action = error.popitem()
        if not (_op_type == 'delete' and not action.get('found')):
            considerable_errors.append({_op_type: action})
    return considerable_errors


def upload(actions):
    es_client = Elasticsearch(ES_HOSTS, port=9200, timeout=300)
    step = 3000
    success_num, error_num = 0, 0
    for begin in range(0, len(actions), step):
        success, errors = helpers.bulk(es_client, actions[begin:begin+step], stats_only=False, raise_on_error=False)
        considerable_errors = get_considerable_errors(errors)
        if considerable_errors:
            logging.error(considerable_errors)
            error_num += len(considerable_errors)
        success_num += success
    logging.info('total: %s, success: %s, error: %s', len(actions), success_num, error_num) 
 

def work(file_in, doc_type):
    actions = get_actions(file_in, ES_INDEX, doc_type)
    upload(actions)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_in', required=True)
    parser.add_argument('--doc_type', required=True)
    args = parser.parse_args()

    work(args.file_in, args.doc_type)
