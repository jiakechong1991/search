# -*- coding: utf-8 -*-

from models import es_instance


def get_base(query, size):
    temp_dsl = {
        "size": size,
        "query": {
            "multi_match": {
              "query": query,
              "fields": ["word"]
            }
        }
    }
    return temp_dsl


def get_result(query, num):
    es = es_instance()
    base = get_base(query, size=1)
    result = es.search(body=base, index="related_search", doc_type="related_search")
    hits = result.get("hits", {}).get("hits", [])
    words = []
    if hits:  # len(hits) != 0
        hit = hits[0]
        source = hit.get("_source", {}).get("result", [])
        for s in source[:num]:
            words.append(s.get("word", ""))
    return words

