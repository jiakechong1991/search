# -*- coding=utf-8 -*-
from elasticsearch import Elasticsearch

conf = {
    "es_host": ["10.0.2.151", "10.0.2.152", "10.0.2.153", "10.0.2.154", "10.0.2.155"]
}
_ES_INSTANCE = None


def es_instance():
    global _ES_INSTANCE
    if _ES_INSTANCE is None:
        _ES_INSTANCE = Elasticsearch(conf["es_host"], sniffer_timeout=60, timeout=60)
    return _ES_INSTANCE