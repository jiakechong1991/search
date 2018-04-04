# coding: utf-8
import json
import os
import time

from settings import conf
"""
用于创建索引
"""
ip = conf["es"]["host"]
print ip[0]
cmd = """curl -XPUT http://{host}:9200/{Index} -d @es_setting.json""".format(
    host=ip[0], Index='related_search')


def create_index(cmd):
    # 如果不能创建请使用sense删除旧索引
    start_time = time.time()
    f = os.popen(cmd)
    print cmd
    data = f.readlines()
    f.close()
    print "消耗时间%f秒" % (time.time() - start_time)
    if len(data) >= 1:
        flag = json.loads(data[0]).setdefault("acknowledged", False)
        print data[0]
        print "索引创建成功" if flag else "索引创建失败"
    else:
        print "I am error"

create_index(cmd)

