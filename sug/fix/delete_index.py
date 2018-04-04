from definition import *
from elasticsearch import Elasticsearch

es_inst = Elasticsearch(DEF_ES_HOST, port=9200, timeout=300)
def delete_index(es_index):
    if es_inst.indices.exists(index=es_index):
        es_inst.indices.delete(es_index)


if __name__ == '__main__':
    es_index = DEF_SUG_INDEX
    delete_index(es_index)

