# -*- coding: utf-8 -*-
import pymongo
import MySQLdb
import re
import json
import hashlib
from elasticsearch import Elasticsearch, helpers
from settings import conf
_MONGO_DB = None
_ES_INSTANCE = None
_ES_INSTANCE_TAP = None
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

noun_set = set()
manual_core_word_set = set()


def mongo_db():
    global _MONGO_DB
    if _MONGO_DB is None:
        mongo_host = conf['mongo']['host']
        mongo_port = conf['mongo']['port']
        client = pymongo.MongoClient(mongo_host, mongo_port)
        _MONGO_DB = client.edxapp
    return _MONGO_DB


def mysql_connection():
    mysql_host = conf['mysql']['host']
    mysql_user = conf['mysql']['user']
    mysql_password = conf['mysql']['password']
    conn_mysql = MySQLdb.connect(*[mysql_host, mysql_user, mysql_password, 'edxapp'], charset='utf8', use_unicode=True)
    return conn_mysql


def es_instance():
    global _ES_INSTANCE
    if _ES_INSTANCE is None:
        _ES_INSTANCE = Elasticsearch(conf["es"]["host"], sniffer_timeout=60, timeout=60)
    return _ES_INSTANCE


def es_instance_tap():
    global _ES_INSTANCE_TAP

    if _ES_INSTANCE_TAP is None:
        es_host = ['http://' + host for host in conf.get('es', {'host': ['localhost']}).get('host', ['localhost'])]
        _ES_INSTANCE_TAP = Elasticsearch(conf["tap_es"]["host"], sniffer_timeout = 60, timeout = 60)
    return _ES_INSTANCE_TAP


def md5(content):
    return hashlib.sha1(content).hexdigest()