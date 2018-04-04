# -*- coding: utf-8 -*-
import requests
import json
import datetime
import sys
import time


def get_es_status(IP):
    return get_es_health(IP)


def get_node_num(IP):
    try:
        url = 'http://{}:9200/_cluster/health'.format(IP)
        r = requests.get(url)
        js = r.json()
        node_num = int(js['number_of_nodes'])
        if node_num != 5:
            return '1'
        else:
            return '0'
    except Exception, e:
        return '1'


def get_es_health(IP):
    try:
        url = 'http://{}:9200/_cluster/health'.format(IP)
        r = requests.get(url)
        js = r.json()
        health = js['status']
        if health != 'green':
            return '1'
        else:
            return '0'
    except Exception, e:
        return '1'


def get_query(IP):
    try:
        data = {
            'query': {
                'query_string': {
                    'analyzer': 'ik_max_word',
                    'default_field': 'title',
                    'query': u'财务'
                }
            }
        }
        url = 'http://{}:9200/course/title/_search'.format(IP)
        resp = requests.get(url, data=json.dumps(data))
        result = resp.json()['hits']['total']
        if result == 0:
            return '1'
        else:
            return '0'
    except Exception, e:
        return '1'


def get_filter(IP):
    try:
        now = datetime.datetime.now()
        date = ''
        if now.hour < 8:
            date = datetime.datetime.strftime(now + datetime.timedelta(-2), "%Y-%m-%d")
        else:
            date = datetime.datetime.strftime(now + datetime.timedelta(-1), "%Y-%m-%d")
        data = {
            'query': {
                'filtered': {
                    'filter': {
                        'term': {
                            'date': date
                        }
                    }
                }
            }
        }
        url = 'http://{}:9200/rollup/course_active/_search?search_type=count'.format(IP)
        resp = requests.get(url, data=json.dumps(data))
        result = resp.json()['hits']['total']
        if result < 50:
            return '1'
        else:
            return '0'
    except Exception, e:
        return '1'


def get_tornado_status(IP, port):
    try:
        url = 'http://{}:{}/search?query=财务&qt=1'.format(IP, port)
        resp = requests.get(url)
        result = resp.json()
        if result['error_code'] != 0 or result['total'] == 0:
            return '1'
        else:
            return '0'
    except Exception, e:
        return '1'


def test_performance(nginx_host):
    """

    本监控是:监控主站搜索的Nginx的接口响应速度,每3分钟一次,连续3次  短信-邮件  报警。

    """
    try:
        url = "http://{host}:9998/search?query=%E5%A4%9A%E5%85%83%E5%9B%9E%E5%BD%92&owner=xuetangx%3bedx&num=5".format(
            host=nginx_host)
        now = time.time()
        res = requests.get(url=url, timeout=2)
        elaspe = time.time() - now
        if elaspe >= 2:
            return "1"
        else:
            return "0"
    except Exception, e:
        return '1'


if __name__ == "__main__":
    # 全部函数，返回 “0” 正常，返回“1”， 不正常
    cmd = sys.argv[1]
    IP = sys.argv[2]
    # t1 = time.time()
    if cmd == 'status':
        print int(get_es_status(IP))
    elif cmd == 'search':
        print int(get_query(IP))
    elif cmd == 'data':
        print int(get_filter(IP))
    elif cmd == 'tornado':
        print int(get_tornado_status(IP, port=9999))
    elif cmd == 'performance':
        print int(test_performance(IP))
    elif cmd == 'nodes':
        print int(get_node_num(IP))


# python ./monitor_es.py "performance" "10.0.0.160"   # 监控接口性能
# python ./monitor_es.py "status" "10.0.2.151" # 监控ES集群状态  green/red
# python ./monitor_es.py "search" "10.0.2.151"   # 用"财务"的DSL直接请求ES看是否有结果
# python ./monitor_es.py "tornado" "10.0.2.152"   # 监控 本机Nginx-tornado通路 的是否正常,
# python ./monitor_es.py "nodes" "10.0.2.152"  # 从两个节点上监控  es集群的集群node数



