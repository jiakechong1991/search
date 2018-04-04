# -*- coding: utf-8 -*-

import hashlib
import os
import time
import json
from copy import deepcopy
import requests
from settings import conf, DEBUG_FLAG
from random import choice
import sys
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')


def print_time_json(your_dcit):
    dict_ = deepcopy(your_dcit)
    for key_ in dict_:
        if isinstance(dict_[key_], datetime.datetime):
            dict_[key_] = str(dict_[key_])
    print json.dumps(dict_)


def rename_key_for_dict(old_key, new_key, your_dict):
    assert isinstance(your_dict, dict), u"请使用一个字典"
    assert old_key in your_dict, u"old_key必须在dict中: {k}".format(k=old_key)
    assert new_key not in your_dict, u"new_key must not in dict: {k}".format(k=new_key)
    your_dict[new_key] = your_dict[old_key]
    del your_dict[old_key]
    return your_dict


def add_key_prefix_4_dict(key_prefix, your_dict):

    assert isinstance(key_prefix, str), u"must str"
    assert isinstance(your_dict, dict), u"must dict"
    key_list = your_dict.keys()
    for item_key in key_list:
        rename_key_for_dict(item_key, "{a}_{b}".format(a=key_prefix, b=item_key), your_dict)


def remove_key_prefix_4_dict(key_prefix, your_dict):
    assert isinstance(key_prefix, str), u"must str"
    assert isinstance(your_dict, dict), u"must dict"
    key_list = your_dict.keys()
    for item_key in key_list:
        if key_prefix in item_key:
            rename_key_for_dict(
                item_key,
                item_key.replace("{a}_".format(a=key_prefix),"", 1),
                your_dict)
    return your_dict


def post_es(param):
    token_list = []
    assert isinstance(param, dict), "请确保是字典参数"
    ip_list = conf["es"]["host"]
    url = """http://{ip}:9200/_analyze""".format(ip=choice(ip_list))
    r = requests.get(url, param)
    for item in json.loads(r.text).get('tokens',None):
        token_list.append(item['token'])
    return token_list


def print_debug(str_or_list):
    if DEBUG_FLAG:
        print str_or_list
def md5(text):

    m = hashlib.md5()
    m.update(text)
    return m.hexdigest()

def list_uniq_by_field(your_list,field):
    """
    :param your_list: 字典列表
    :param field: 用指定的域对列表中的字典进行去重
    :return:
    """
    temp_dict = {}
    for item in your_list:
        assert isinstance(item, dict)
        assert field in item.keys()
        temp_dict[item[field]] = item
    return temp_dict.values()


def rm_key(your_dict, remv_key):
    if remv_key in your_dict.keys():
        del your_dict[remv_key]



# def es_tool(method,ip,body,):
#         start_time = time.time()
#         cmd =
#         f = os.popen(cmd)
#         print cmd
#         data = f.readlines()
#         f.close()
#         print "消耗时间%f秒" % (time.time() - start_time)
#         if len(data) >= 1:
#             flag = json.loads(data[0]).setdefault("acknowledged", False)
#             print data[0]
#             print "索引创建成功" if flag else "索引创建失败"
#         else:
#             print "I am error"

if __name__ == "__main__":
    param = {
        "analyzer": "ik_max_word",
        "text": "%e5%a4%a7%e5%ad%a6%e5%8c%96%e5%ad%a6"
    }
    post_es(param)
    # build_logger.info("hao")



