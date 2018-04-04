# coding: utf8
import argparse
import codecs
import json
import logging
import re

from utils.connection import with_mysql_cursor
from config.conf import LOGGING_FORMAT, MAX_WORD_LEN, MIN_WORD_LEN

class WordWeightGetter():
    def __init__(self):
        pass

    @property
    def course_sql(self):
        return """ select course_id from course_meta_course where
            status>=0 and lower(owner) in ('xuetangx', 'edx') """

    @with_mysql_cursor('edxapp')
    def get_course_num(self, cursor):    
        sql = """ select count(*) as course_num from course_meta_course
            where course_id in ({courses})
        """.format(courses=self.course_sql)
        cursor.execute(sql)
        course_num = cursor.fetchone()['course_num']
        return course_num

    def get_clean_name(self, name):
        replace_maps = {
            u'\n': '',
            u'\t': '',
            u'《': '',
            u'》': '',
            u'（': '(',
            u'）': ')',
            u'®': '',
            u'*': ' ',
            u'•': ' ',
            u'—': '-',
            u'–': '-',
            u'：': ':',
            u'、': ',',
            u'，': ',',
            u'"': ' '}
        for old_char, new_char in replace_maps.items():
            name = name.replace(old_char, new_char)
        name = re.sub(r'\s+', ' ', name)

        # 除去周围的空格
        punctuations = '()-:'
        for c in punctuations:
            name = re.sub(r' *\{c} *'.format(c=c), c, name)
        return name

       
    def cut_long_name(self, name):
        len_name = len(name)
        if len_name <= MAX_WORD_LEN:
            return (0, name)

        lst_word = name.split(' ')
        if len(lst_word) < 5: # 中文
            name = name[:MAX_WORD_LEN]
        else:
            cur_len = len_name
            for i, word in enumerate(lst_word[::-1]):
                cur_len -= len(word)
                if cur_len <= MAX_WORD_LEN:
                    name = ' '.join(lst_word[:-(i+1)])
                    break
        return (1, name) 
 

    @with_mysql_cursor('edxapp')
    def dump_file(self, cursor, file_ot):
        logging.info('start dump course name')
        course_num = self.get_course_num()
        begin, size = 0, 100000
        cut_num, success_num = 0, 0
        course_names = set()
        wf = codecs.open(file_ot, 'w', encoding='utf8')
        while begin < course_num:
            sql = """ select course_info.course_id, name, enroll_num from
                ( select id, course_id, name from course_meta_course
                    where course_id in ({courses}) ) course_info
                left outer join
                ( select course_id, count(*) as enroll_num from student_courseenrollment
                    where course_id in ({courses}) and is_active=1
                    group by course_id ) course_enroll
                on course_info.course_id = course_enroll.course_id
                order by id limit {begin},{size}
            """.format(courses=self.course_sql, begin=begin, size=size)
            cursor.execute(sql)
            courses = cursor.fetchall()

            batch_num = len(courses)
            for no, course in enumerate(courses):
                if no % 100 == 0:
                    logging.info('finished: %s/%s', begin+no, begin+batch_num)
                name = self.get_clean_name(course['name'])
                is_cut, name = self.cut_long_name(name)
                enroll_num = course['enroll_num'] if course['enroll_num'] else 0
                cut_num += is_cut
                _input = name.lower()
                if len(_input) < MIN_WORD_LEN or len(_input) > MAX_WORD_LEN:
                    continue
                elif _input in course_names:
                    logging.warning('重复的课程名：%s, %s, %s', name.lower().encode('utf8'), name.encode('utf8'), course['course_id'].encode('utf8'))
                else:
                    course_names.add(name)
                    row = {
                        'input': _input,
                        'output': name,
                        'course_id': course['course_id'],
                        'weight': enroll_num + 100
                    }
                    wf.write(json.dumps(row, sort_keys=True).decode('unicode-escape') + '\n')
                    success_num += 1
            begin += size 
        wf.close()
        logging.info('total_num: %s, cut_num: %s, success_num: %s', course_num, cut_num, success_num)
        


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_ot', required=True)
    args = parser.parse_args()

    obj = WordWeightGetter()
    obj.dump_file(args.file_ot)






