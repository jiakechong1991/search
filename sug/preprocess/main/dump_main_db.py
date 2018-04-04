# coding: utf8
import argparse
import codecs
import copy
import json
import logging
import re

from search_service.indexing import Searchable

from config.conf import LOGGING_FORMAT, MAX_WORD_LEN
from utils.connection import with_mysql_cursor

class WordGetter():
    def __init__(self):
        pass

    @with_mysql_cursor('edxapp')
    def get_course_ids(self, cursor):
        sql = 'select course_id from course_meta_course where status >= 0'
        cursor.execute(sql)
        course_ids = [x['course_id'] for x in cursor.fetchall()]
        return course_ids

    def get_clean_text(self, text):
        special_chars = ['\t', '\n', u'\u2028', u'\u0085']
        for c in special_chars:
            text = text.replace(c, '')
        return text

    def dump_file(self, file_ot):
        course_ids = self.get_course_ids()
        course_num = len(course_ids)

        wf = codecs.open(file_ot, 'w', encoding='utf8')
        search = Searchable()
        for no, course_id in enumerate(course_ids):
            try:
                course_info = search.get_course(course_id.encode('utf8'))
            except Exception, e:
                logging.error('%s: %s', course_id, e)
                continue
            if not course_info:
                continue
            if no % 100 == 0:
                logging.info('finished: %s/%s', no, course_num)
           
            base_info = {
                'course_id': course_id,
            } 

            # course_name
            row = copy.deepcopy(base_info)
            row.update({
                'category': 'course_name',
                'value': course_info.course_name
            })
            wf.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + '\n')

            # course_about
            row = copy.deepcopy(base_info)
            row.update({
                'category': 'course_about',
                'value': self.get_clean_text(course_info.get_about_searchable_text())
            })
            wf.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + '\n')

            # children
            for child in course_info.children:
                if child.searchable_text:
                    row = copy.deepcopy(base_info)
                    row.update({
                        'category': child.category,
                        'value': self.get_clean_text(child.searchable_text)
                    })
                    wf.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + '\n')

        wf.close()    


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_ot', required=True)
    args = parser.parse_args()

    obj = WordGetter()
    obj.dump_file(args.file_ot)




