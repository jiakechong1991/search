# -*- coding: utf8 -*-

from elasticsearch import Elasticsearch
import time
import datetime
import os
import sys
sys.path.append(os.path.split(os.path.realpath(__file__))[0])
host = ["10.0.2.151", "10.0.2.152", "10.0.2.153", "10.0.2.154", "10.0.2.155"]
_ES_INSTANCE = Elasticsearch(host, sniffer_timeout=60, timeout=60)
Index = "monit_table"
Type = "search_add_index_kafka"
doc_id = "1"
five_min = datetime.timedelta(0, 5*60)


def read_es():
    res = _ES_INSTANCE.get(index=Index, id=doc_id, doc_type=Type)
    return res.get("_source", {})


def main_f():
    """

    Returns: 返回1是出现问题,返回0是正常

    """
    # 1 min 做一次检查
    res = read_es()
    s = time.strftime('%Y-%m-%d %X', time.localtime())
    now = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    gujia_kafka_time = datetime.datetime.strptime(res['gujia_kafka'], "%Y-%m-%d %H:%M:%S")
    mysql_kafka_time = datetime.datetime.strptime(res['mysql_kafka'], "%Y-%m-%d %H:%M:%S")
    tap_parent_kafka_time = datetime.datetime.strptime(res['tap_parent_kafka'], "%Y-%m-%d %H:%M:%S")
    xuetangyun_mysql_kafka = datetime.datetime.strptime(res['xuetangyun_mysql_kafka'], "%Y-%m-%d %H:%M:%S")
    status = True
    if ((now - gujia_kafka_time) > five_min) or ((now - mysql_kafka_time) > five_min) or \
            ((now - tap_parent_kafka_time) > five_min) or ((now - xuetangyun_mysql_kafka) > five_min):
        status = False

    return 0 if status else 1


if __name__ == "__main__":
    print main_f()
