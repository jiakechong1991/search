import argparse
import codecs
import json
import logging
import re

from config.conf import LOGGING_FORMAT, MAX_WORD_LEN, MAX_USER_WEIGHT
from utils.common import cut_long_word, strQ2B
from utils.connection import mysql_connection, with_mysql_cursor

class WordWeightGetter():
    @with_mysql_cursor('edxapp')
    def get_user_num(self, cursor):
        sql = """ select count(*) as user_num from auth_userprofile """
        cursor.execute(sql)
        user_num = cursor.fetchone()['user_num']
        return user_num

    def is_nickname_clean(self, nickname):
        special_chars = ['\t', '\n', u'\u2028', u'\u0085']
        for c in special_chars:
            if c in nickname:
                return False
        return True

    def get_input(self, nickname):
        _input = strQ2B(nickname)
        _input = _input.lower()
        _input = re.sub(r'\s+', ' ', _input)
        return _input

    @with_mysql_cursor('edxapp')
    def dump_file(self, cursor, file_ot):
        logging.info('start dump user name')
        user_num = self.get_user_num()
        begin, size = 0, 100000
        cut_num, reject_num, success_num = 0, 0, 0
        user_nickname = {}
        wf = codecs.open(file_ot, 'w', encoding='utf8')
        while begin < user_num:
            logging.info('finished: %s/%s', begin, user_num)
            sql = """ select user_id, nickname from auth_userprofile
                order by id limit {begin},{size} """.format(begin=begin, size=size)
            cursor.execute(sql)
            users = cursor.fetchall()
            for user in users:
                user_id = user['user_id']
                nickname = user['nickname']

                is_clean = self.is_nickname_clean(nickname)
                if not is_clean:
                    logging.warning('reject!!! user_id: %s, nickname: %s', user_id, nickname)
                    reject_num += 1
                    continue

                if nickname in user_nickname:
                    logging.warning('nickname duplicate! nickname: %s, user_id: %s, %s',
                        nickname, user_id, user_nickname[nickname])
                    continue
                else:
                    user_nickname[nickname] = user_id
                
                _input = self.get_input(nickname)
                if not _input:
                    logging.warning('reject!!! input is empty. user_id: %s, nickname: %s', user_id, nickname)
                    reject_num += 1
                    continue

                weight = MAX_USER_WEIGHT - len(_input) * 2
                if len(_input) > MAX_WORD_LEN:
                    logging.warning('input is too long! input: %s, user_id: %s', _input, user_id) 
                    _, _input = cut_long_word(_input, MAX_WORD_LEN)
                    cut_num += 1
              
                row = {
                    'input': _input,
                    'output': nickname,
                    'user_id': user_id,
                    'weight': weight
                }

                wf.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + '\n')
                success_num += 1
            begin += size 
        wf.close()
        logging.info('total_num: %s, cut_num: %s, reject_num: %s, success_num: %s', user_num, cut_num, reject_num, success_num)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_ot', required=True)
    args = parser.parse_args()

    obj = WordWeightGetter()
    obj.dump_file(args.file_ot)
  
