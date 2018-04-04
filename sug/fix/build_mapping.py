from definition import *
from elasticsearch import Elasticsearch

es_inst = Elasticsearch(DEF_ES_HOST, port=9200, timeout=300)
def build_mapping(es_index, doc_type):
    mapping_conf = {
      "properties" : {
        doc_type : {
          "type" : "completion",
          "index_analyzer" : "keyword",
          "search_analyzer" : "keyword",
          "payloads" : "true"
        }
      }
    }
    es_inst.indices.put_mapping(index=es_index, doc_type=doc_type, body=mapping_conf)

def build_all_mapping(es_index):
    if es_inst.indices.exists(index=es_index):
        es_inst.indices.delete(es_index)
    es_inst.indices.create(index=es_index)
    doc_type_nos = [1, 2, 3]
    for doc_type_no in doc_type_nos:
        doc_type =  DEF_DIC_QT[doc_type_no]
        build_mapping(es_index, doc_type)


if __name__ == '__main__':
    es_index = DEF_SUG_INDEX
    build_all_mapping(es_index)
