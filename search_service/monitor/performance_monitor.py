# -*- coding: utf-8 -*-

import requests
import json
import datetime
import sys
import time
from elasticsearch import Elasticsearch

es_host = ["10.0.2.151", "10.0.2.152", "10.0.2.153", "10.0.2.154", "10.0.2.155"]
nginx_host = "10.0.0.160:9998"
url = "http://{host}/search?query=%E5%A4%9A%E5%85%83%E5%9B%9E%E5%BD%92&owner=xuetangx%3bedx&num=5".format(
    host=nginx_host)

_ES_INSTANCE = None


def es_instance():
    global _ES_INSTANCE
    if _ES_INSTANCE is None:
        _ES_INSTANCE = Elasticsearch(es_host, sniffer_timeout=60, timeout=60)
    return _ES_INSTANCE


"""
该监控的索引建立方式
PUT /search_jiekou
{
  "mappings": {
  "search_web":{
    "properties":{
      "test_time_now":{
        "type":"date"
      },
      "search_elaspe_time":{
        "type":"long"
      }
    }
  }
  }
}
"""


def get_performance_monitor():

    global _ES_INSTANCE
    _ES_INSTANCE = es_instance()

    now = time.time()
    while True:
        if time.time() - now > 60*2:
            now = time.time()
            try:
                res = requests.get(url=url, timeout=2)
                elaspe = time.time() - now
            except Exception, e:
                print e
                elaspe = 7

            doc = {
                "search_elaspe_time": elaspe*1000,
                "test_time_now": time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())
            }
            try:
                _ES_INSTANCE.index(index="search_jiekou", doc_type="search_web", body=doc)
                print "ok"
            except Exception, e:
                print e
                _ES_INSTANCE = es_instance()
        else:
            time.sleep(60*1)


if __name__ == "__main__":
    get_performance_monitor()



