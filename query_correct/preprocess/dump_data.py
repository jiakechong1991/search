# coding: utf8
import argparse
import codecs
from collections import defaultdict
import contextlib
import logging
import math
import os
import re
import sys
import MySQLdb
import MySQLdb.cursors

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))
from config.conf import INPUT_MIN_LEN, INPUT_MAX_LEN, LOGGING_FORMAT
from config.db_conf import database
from utils.common import all_in_range
from utils.lang_conf import chinese_digits, digit_range
from utils.word_cleaner import WordCleaner

def with_cursor(func):
    def wrapper(self, *args, **kwargs):
        with contextlib.closing(self.mysql_connect()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                return func(self, cursor, *args, **kwargs)
    return wrapper


class WordWeightGetter():
    def __init__(self):
        pass

    @property
    def cleaner(self):
        return WordCleaner()

    def mysql_connect(self):
        conn = MySQLdb.connect(
            host=database['mysql']['host'],
            user=database['mysql']['user'],
            passwd=database['mysql']['passwd'],
            db='edxapp',
            charset='utf8',
            use_unicode=True,
            cursorclass=MySQLdb.cursors.DictCursor)
        return conn

    # 除去第xx讲，第xx部分等
    def discard_series_word(self, word):
        chinese_digits_str = ''.join(chinese_digits)
        xx = u'(第[0-9{0}]*[讲场课期册])|(第[0-9i{0}]*部分)|(第[0-9 ]*小时)|[0-9]+[春夏秋]|([第]$)|(之[{0}]+)'.format(chinese_digits_str)
        pattern = re.compile(xx)
        series_poses = pattern.search(word)
        if series_poses:
            series_poses = series_poses.span()
            result = [word[:series_poses[0]], word[series_poses[1]:]]
            return result
        else:
            return [word]

    def get_clean_words(self, word):
        res = []
        # 破折号
        words = word.split(u'——')
        # 去掉特殊字符
        clean_words = []
        for word in words:
            word, _ = self.cleaner.clean(word)       
            clean_words.append(word)
        # 去掉序列词
        no_series_words = []
        for word in clean_words:
            no_series_words += self.discard_series_word(word)
        
        # 去掉关键词
        no_key_words = []
        for word in no_series_words:
            for keyword in (u'微慕课', u'自主模式'):
                if word.endswith(keyword):
                    word = word[:-len(keyword)]
                    break
            no_key_words.append(word)

        res = no_key_words
        return res

    @property
    def course_sql(self):
        return """ select course_id from course_meta_course
            where status>=0 and owner='xuetangX' """

    @with_cursor
    def get_course_enroll_num(self, cursor):
        query = """ select course_id, count(*) as enroll_num
            from student_courseenrollment
            where course_id in ({}) and is_active = 1
            group by course_id """.format(self.course_sql)
        cursor.execute(query)
        response = cursor.fetchall()
        enroll_num = defaultdict(int)
        for row in response: 
            enroll_num[row['course_id']] = row['enroll_num']
        return enroll_num

    @with_cursor
    def get_category_enroll(self, cursor):
        def get_category_group_id():
            query = """ select id as group_id from course_meta_categorygroup
                where owner='xuetangx' and slug='xuetangx' """
            cursor.execute(query)
            response = cursor.fetchone()
            group_id = response['group_id']
            return group_id

        query = """ select course.course_id, course_category.name as category_name
            from (select id, course_id from course_meta_course where course_id in ({courses}))course
            inner join (
                select relation.course_id, category.name from
                (select course_id, coursecategory_id as category_id from course_meta_course_category) relation
                inner join (select id, name from course_meta_coursecategory where group_id={group_id}) category
                on relation.category_id = category.id) course_category
            on course.id = course_category.course_id
            where course_category.name is not null
        """.format(courses=self.course_sql, group_id=get_category_group_id())
        cursor.execute(query)
        response = cursor.fetchall()
        category_enroll = defaultdict(int)
        for row in response:
            words = self.get_clean_words(row['category_name'])
            for word in words:
                category_enroll[word] += self.course_enroll_num[row['course_id']]
        return category_enroll

    @with_cursor
    def get_course_name_enroll(self, cursor):
        query = """ select course_id, name as course_name from course_meta_course
            where course_id in ({}) """.format(self.course_sql)
        cursor.execute(query)
        response = cursor.fetchall()
        course_name_enroll = defaultdict(int)
        for row in response:
            words = self.get_clean_words(row['course_name'])
            for word in words:
                course_name_enroll[word] += self.course_enroll_num[row['course_id']]
        return course_name_enroll

    @with_cursor
    def get_staff_enroll(self, cursor):
        query = """ select course.course_id, staff.name as staff_name
            from (select id, course_id from course_meta_course where course_id in ({})) course
            inner join (select course_id, staff_id from course_meta_coursestaffrelationship)relation
            on course.id=relation.course_id
            inner join (select id, name from course_meta_staff) staff
            on relation.staff_id=staff.id """.format(self.course_sql)
        cursor.execute(query)
        response = cursor.fetchall()
        staff_enroll = defaultdict(int)
        for row in response:
            words = self.get_clean_words(row['staff_name'])
            for word in words:
                staff_enroll[word] += self.course_enroll_num[row['course_id']]
        return staff_enroll

    @with_cursor
    def get_org_enroll(self, cursor):
        query = """ select course.course_id, org.name as org_name
            from (select course_id, org from course_meta_course where course_id in ({})) course
            left join (select org, name from course_meta_organization) org
            on course.org=org.org """.format(self.course_sql)
        cursor.execute(query)
        response = cursor.fetchall()
        org_enroll = defaultdict(int)
        for row in response:
            words = self.get_clean_words(row['org_name'])
            for word in words:
                org_enroll[word] += self.course_enroll_num[row['course_id']]
        return org_enroll
        
    def _get_word_weight(self, domain, ratio):
        weight_range = {
            'org_name': (20, 100),
            'staff_name': (200, 1000),
            'course_name': (2000, 10000),
            'course_category': (20000, 100000)}
        min_weight, max_weight = weight_range[domain]
        weight = int(math.ceil(min_weight + (max_weight - min_weight) * 1.0 * ratio))
        return weight

    def show(self, data, row_num=10):
        print len(data)
        for no, (key, value) in enumerate(data.items()):
            print key, value
            if no >= 10:
                break

    def save(self, word_weight, file_ot, min_len=None, max_len=None):
        with codecs.open(file_ot, 'w', encoding='utf8') as wf:
            for word, weight in word_weight.items():
                if (word and (min_len is None or len(word) >= min_len)
                    and (max_len is None or len(word) <= max_len)
                    and not all_in_range(word, digit_range)):
                    line = '%s\0%s' % (word, weight)
                    wf.write(line + '\n')
                    
    def get_word_weight(self):
        logging.info('get_course_enroll_num ...')
        self.course_enroll_num = self.get_course_enroll_num()
        logging.info('get_category_enroll ...')
        category_enroll = self.get_category_enroll()
        logging.info('get_course_name_enroll ...')
        course_name_enroll = self.get_course_name_enroll()
        logging.info('get_org_enroll ...')
        org_enroll = self.get_org_enroll()
        logging.info('get_staff_enroll ...')
        staff_enroll = self.get_staff_enroll()

        data = {
            'course_category': category_enroll,
            'course_name': course_name_enroll,
            'org_name': org_enroll,
            'staff_name': staff_enroll
        }

        """
        self.show(category_enroll)
        self.show(course_name_enroll)
        self.show(org_enroll)
        self.show(staff_enroll)
        """

        word_weight = {}
        for domain, domain_data in data.items():
            max_enroll_num = max(domain_data.values())
            for word, enroll_num in domain_data.items():
                ratio = enroll_num * 1.0 / max_enroll_num
                weight = self._get_word_weight(domain, ratio)
                word_weight[word] = max(weight, word_weight.get(word, 0))

        return word_weight


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_ot', required=True)
    args = parser.parse_args()

    obj = WordWeightGetter()
    word_weight = obj.get_word_weight()
    obj.save(word_weight, args.file_ot, min_len=INPUT_MIN_LEN, max_len=INPUT_MAX_LEN)
