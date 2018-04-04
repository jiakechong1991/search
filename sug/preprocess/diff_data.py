# coding: utf8
import argparse
import codecs
import json
import logging
import os

from config.conf import LOGGING_FORMAT
from utils.common import get_row_num

def get_data(filename, key_fields):
    data = {}
    if not os.path.exists(filename):
        return data
    row_num = get_row_num(filename)
    with codecs.open(filename, encoding='utf8') as f:
        for line_no, line in enumerate(f):
            if line_no % 100000 == 0:
                logging.info('finished: %s/%s', line_no, row_num)
            row = json.loads(line)
            key = '\0'.join([unicode(row[field]) for field in key_fields])
            data[key] = row
    return data


def get_diff_data(old_data, new_data, key_fields):
    diff_data = {
        'index': [],
        'update': [],
        'delete': [],
        'same': []
    }

    row_num = len(new_data)
    for no, (key, new_row) in enumerate(new_data.items()):
        if no % 100000 == 0:
            logging.info('finished: %s/%s', no, row_num)
        if key not in old_data:
            diff_data['index'].append(new_row)
        else:
            old_row = old_data.pop(key)
            if new_row == old_row:
                diff_data['same'].append(new_row)
            else:
                diff_data['update'].append(new_row)

    row_num = len(old_data)
    for key, old_row in old_data.items():
        if no % 100000 == 0:
            logging.info('finished: %s/%s', no, row_num)
        diff_data['delete'].append(old_row)
    
    return diff_data


def save_diff_data(diff_data, file_ot):
    with codecs.open(file_ot, 'w', encoding='utf8') as wf:
        for _op_type, rows in diff_data.items():
            row_num = len(rows)
            logging.info('%s: %s', _op_type, row_num)
            for row_no, row in enumerate(rows):
                if row_no % 100000 == 0:
                    logging.info('finished: %s/%s', row_no, row_num)
                row['_op_type'] = _op_type
                wf.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + '\n')


def work(old_file, new_file, file_ot, key_fields):
    logging.info('get_old_data')
    old_data = get_data(old_file, key_fields)

    logging.info('get_new_data')
    new_data = get_data(new_file, key_fields)

    logging.info('get_diff_data')
    diff_data = get_diff_data(old_data, new_data, key_fields)

    logging.info('save_diff_data')
    save_diff_data(diff_data, file_ot) 

    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

    parser = argparse.ArgumentParser()
    parser.add_argument('--old_file', required=True)
    parser.add_argument('--new_file', required=True)
    parser.add_argument('--file_ot', required=True)
    args = parser.parse_args()

    key_fields = ['input', 'output']
    work(args.old_file, args.new_file, args.file_ot, key_fields)


