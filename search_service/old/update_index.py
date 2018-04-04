#-*- coding=utf-8 -*-
# By Yufei
from models import es_instance
from indexing import Searchable
import sys
import logging.config
from tool.settings import IndexName

update_logger = logging.getLogger('update.'+__name__)
doc_types = ['about', 'title', 'course', 'structure', 'staff']


def delete_index(course_id):
    # Delete the index of a course
    # 这是一段废弃的代码
    es = es_instance()
    del_json = {
        "query":{
            "filtered": {
                "filter": {
                    "term": {
                        "course_id": course_id
                        }
                    }
                }
            }
        }
    for doc_type in doc_types:
        es.delete_by_query(IndexName, doc_type, del_json)


def rebuild_index(course_id):
    # Build index of a course
    s = Searchable()
    try:
        course = s.get_course(course_id)
    except Exception, e:
        update_logger.warn('rebuild_index error')
        print e
        course = None
    if course != None:
        course.build_chunks()
        course.flush_chunks()
        course.close_mysql()
        return True
    else:
        return False


def update_index(course_id):
    return rebuild_index(course_id)


if __name__ == "__main__":
    update_index('course-v1:Yifang+ASEC121+2017_T1')

