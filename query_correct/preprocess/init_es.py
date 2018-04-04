import argparse
import logging
import os
import sys

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))
from config.conf import ES_HOSTS, ES_INDEX, LOGGING_FORMAT

index_settings = {
    "analysis": {
        "analyzer": {
            "chinese_ngram_analyzer": {
                "tokenizer": "chinese_ngram_tokenizer"
            },  
            "non_chinese_ngram_analyzer": {
                "tokenizer": "non_chinese_ngram_tokenizer"
            }   
        },  
        "tokenizer": {
            "chinese_ngram_tokenizer": {
                "type": "nGram",
                "min_gram": "1",
                "max_gram": "2" 
            },  
            "non_chinese_ngram_tokenizer": {
                "type": "nGram",
                "min_gram": "2",
                "max_gram": "2" 
            }   
        }   
    }
}

index_mappings = {
    "chinese_ngram": {
        "properties": {
            "chinese_word": {
                "type": "keyword",
                "fields": {
                    "analyzed": {
                        "type": "text",
                        "analyzer": "chinese_ngram_analyzer"
                    }
                }
            },
            "weight": {"type": "integer"},
            "_ut": {"type": "date"}
        }
    },
    "non_chinese_ngram": {
        "properties": {
            "non_chinese_word": {
                "type": "keyword",
                "fields": {
                    "analyzed": {
                        "type": "text",
                        "analyzer": "non_chinese_ngram_analyzer"
                    }
                }
            },
            "weight": {"type": "integer"},
            "_ut": {"type": "date"}
        }
    },
    "similar_form_no_ngram": {
        "properties": {
            "similar_word": {"type": "keyword"},
            "output": {"type": "keyword"},
            "weight": {"type": "integer"},
            "_ut": {"type": "date"}
         }
    },
    "pinyin_ngram": {
        "properties": {
            "non_chinese_word": {
                "type": "keyword",
                "fields": {
                    "analyzed": {
                        "type": "text",
                        "analyzer": "non_chinese_ngram_analyzer"
                    }
                }
            },
            "output": {"type": "keyword"},
            "weight": {"type": "integer"},
            "_ut": {"type": "date"}
        }
    }
}


def work():
    es_client = Elasticsearch(ES_HOSTS, port=9200)
    if es_client.indices.exists(index=ES_INDEX):
        es_client.indices.delete(ES_INDEX)
    es_client.indices.create(
        index = ES_INDEX,
        body = {'settings': index_settings, 'mappings': index_mappings}
    )
    logging.info('init es finished')

if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    work()





