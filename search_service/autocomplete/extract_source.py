# -*- coding=utf-8 -*-
import sys
import json
reload(sys)
sys.setdefaultencoding('utf-8')
from config_tool import es_instance


def split_doc(doc_list):
    token_wight = {
        "token"
    }
    for item_doc in doc_list:
        if item_doc["doc_type"] == "title":



def process_all_in_one(doc_list):
    pass


def get_course(num):
    """每次提取num文档,参考内存控制"""
    es_ = es_instance()
    body = {
      "query": {
      },
      "size": num
    }
    res = es_.search(index="course", body=body, scroll="1m")
    total_num = res["hits"]["total"]
    now_num = 0
    while True:
        doc_list = []
        print "now process {}----".format(100.0*now_num/total_num)
        assert res["timed_out"] == False, "已经超时，本次传输基本失败"
        _scroll_id = res["_scroll_id"]
        for item_doc in res["hits"]["hits"]:
            source_ = item_doc["_source"]
            now_num += 1
            doc_list.append(source_)
        process_all_in_one(doc_list)
        del doc_list
        res = es_.scroll(scroll_id=_scroll_id, scroll="1m")
        if len(res["hits"]["hits"]) == 0:
            break
    print total_num, now_num


if __name__ == "__main__":
    get_course(500)




