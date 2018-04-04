#-*- coding=utf-8 -*-
'''
pip install elasticsearch
python get_suggest.py

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-suggesters-completion.html
'''
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from config.conf import ES_HOSTS, QT_TYPE_MAP
from config.conf import ONLINE_ES_INDEX as ES_INDEX

g_es_inst = Elasticsearch(ES_HOSTS, port=9200)

def get_estype_by_qt(qt):
    if QT_TYPE_MAP.has_key(qt):
        es_type = QT_TYPE_MAP[qt]
    else:
        es_type = ''
    return es_type


def get_search_json(query, query_type=1,num=5):
    es_type = get_estype_by_qt(query_type)
    query_json = {
        es_type : {
            "text" : query,
            "completion" : {
                "field" : es_type,
                "size": num + 1
            }
        }
    }
    return query_json


def get_sug(qt, query, num=5, debug=0):
    lst_ret = []
    lst_debug = []
    es_type = ''
    if QT_TYPE_MAP.has_key(qt):
        es_type = QT_TYPE_MAP[qt]
    else:
        return lst_ret
    print 'qt:{0}'.format(qt)
    print 'query:{0}'.format(query)
    print 'num:{0}'.format(num)
    search_json = get_search_json(query, qt, num)
    result = g_es_inst.suggest(body=search_json, index=ES_INDEX)
    data = result.get(es_type, [])
    # print json.dumps(data)
    for dic_tmp in data:
        lst_tmp = dic_tmp.get('options', [])
        for dic_one in lst_tmp:
            if 'platform' == es_type:
                if dic_one.has_key('text'):
                    word = dic_one['text']
                    if word == query:
                        continue
                    lst_ret.append(word)
            elif 'forum_user' == es_type:
                if dic_one.has_key('text'):
                    word = dic_one['text']
                    lst_ret.append({
                        'nickname':word,
                        'user_id':dic_one.get('payload', {}).get('user_id', '')
                    })
            elif 'course_name' == es_type:
                if dic_one.has_key('text'):
                    word = dic_one['text']
                    lst_ret.append({
                        'course_name':word,
                        'course_id':dic_one.get('payload', {}).get('course_id', '')
                    })
            if 0 != debug:
                if dic_one.has_key('payload') and dic_one['payload'].has_key('_ut'):
                    timestamp = int(dic_one['payload']['_ut'])
                    str_dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
                    dic_one['payload']['_ut'] = str_dt
                lst_debug.append(dic_one)
            if len(lst_ret) >= num:
                break
    return lst_ret, lst_debug


if __name__ == "__main__":
    print get_sug(3, '数据结', 10)


