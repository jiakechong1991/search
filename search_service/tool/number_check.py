# -*- coding: utf-8 -*-

import requests
from elasticsearch import Elasticsearch

_ES_INSTANCE = None
es_conf = {
    "es_host": ["10.0.2.151", "10.0.2.152", "10.0.2.153", "10.0.2.154", "10.0.2.155"],
    "tap_url": "http://tapapi.xuetangx.info/data/student_courseenrollment"
}


def es_instance():
    global _ES_INSTANCE
    if _ES_INSTANCE is None:
        _ES_INSTANCE = Elasticsearch(es_conf["es_host"], sniffer_timeout=60, timeout=60)
    return _ES_INSTANCE


def check_es_tapapi(course_id):
    """这是 TAP_API 与 ES的 选课人数的对比检查点"""
    es = es_instance()
    search_json = {
        "query": {
            "term": {
                "course_id": {
                    "value": course_id
                }
            }
        }
    }
    res = es.search(index="course", doc_type='course', body=search_json)
    accumulate_es = res['hits']['hits'][0]['_source']['accumulate_num']

    pyload = {'course_id': course_id}
    accumulate_result = requests.get(es_conf["tap_url"], timeout=3, params=pyload)
    accumulate_api = int(accumulate_result.json().get('data')[0]['acc_enrollment_num'])
    if abs(accumulate_api-accumulate_es) < 0.1*accumulate_api:
        flag = True
    else:
        flag = False
    return flag


will_check_courses = {
    "course-v1:TsinghuaX+30240184_2X+sp": {
        "msg": "数据结构(下)(自主模式):",
        "check_func_list": [check_es_tapapi]
    },
    "course-v1:TsinghuaX+30240184_p2+sp": {
        "msg": "数据结构-向量（微慕课):",
        "check_func_list": [check_es_tapapi]
    }
}


def main_check():
    """

    Returns: 不为空就是检查失败

    """
    error_msg = ""
    for item_course in will_check_courses:
        for check_func in will_check_courses[item_course]["check_func_list"]:
            if not check_func(item_course):
                error_msg += """{course} {msg}失败\n """.format(
                    course=will_check_courses[item_course]["msg"],
                    msg=check_func.__doc__
                )
    if error_msg:
        flag = False
    else:
        flag = True
        error_msg = "no error!"
    return flag, error_msg


if __name__ == "__main__":
    flag, msg = main_check()
    print msg






