import logging
from elasticsearch import Elasticsearch
from config.conf import ES_HOSTS, ES_INDEX, LOGGING_FORMAT

index_mappings = {
    "platform": {
        "properties": {
            "platform": {
                "type": "completion",
                "analyzer": "keyword",
                "max_input_length": 50,
                "payloads": True,
                "preserve_separators": True,
                "preserve_position_increments": True
            }
        }
    },
    "forum_user": {
        "properties": {
            "forum_user": {
                "type": "completion",
                "analyzer": "keyword",
                "max_input_length": 50,
                "payloads": True,
                "preserve_separators": True,
                "preserve_position_increments": True
            }
        }
    },
    "course_name": {
        "properties": {
            "course_name": {
                "type": "completion",
                "analyzer": "keyword",
                "max_input_length": 50,
                "payloads": True,
                "preserve_separators": True,
                "preserve_position_increments": True
            }
        }
    }
}


def work():
    print index_mappings
    es_client = Elasticsearch(ES_HOSTS, port=9200, timeout=300)
    if es_client.indices.exists(index=ES_INDEX):
        es_client.indices.delete(ES_INDEX)
    es_client.indices.create(
        index = ES_INDEX,
        body = {'mappings': index_mappings}
    )   
    logging.info('init es finished')

if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    work()


