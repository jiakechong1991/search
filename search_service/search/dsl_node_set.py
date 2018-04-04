# -*- coding: utf-8 -*-

nested_base = {
    "nested": {
      "path": "course_info_list",
      "query": {
        "bool": {
          "should": [
          ],
          "filter": [
            {
              "range": {
                "status": {
                  "from": 1
                }
              }
            }
          ]
        }
      }
    }
  }

should_dsl = {  # 这其实是一个should 节点
    "multi_match": {
      "query": "大学",
      "fields": ["title"],
      "boost": 1.0,
      "operator": "and",
      "analyzer": "simple"
    }
  }

knowledge_dsl = {
  "bool": {
    "filter": [
      {
        "range": {
          "status": {
            "from": 0
           }
        }
      }
    ],
    "must": [],
    "should": [],
    "minimum_should_match": 1,
    }
}

base_inner_dsl = {
  "bool": {
    "filter": [],
    "must": [],
    "should": [],
    "minimum_should_match": 1,
    }
}

live_dsl_base_inner = {
  "bool": {
    "filter": [
      {
        "range": {
          "status": {
            "from": 1
           }
        }
      }
    ],
    "should": [],
    "minimum_should_match": 1,
    }
}

course_filter_dsl = {
    "has_parent": {
      "parent_type": "course",
      "query": {
        "bool": {
          "filter": [  # 父课程过滤条件
          ],
          "must_not": [
          ],
          "must": [],
          "should": []
        }
      }
    }
  }

has_child_dsl = {
  "has_child": {
    "score_mode": "sum",
    "type": "title",
    "query": {
      "bool": {
        "filter": [
            #
        ],
        "should": [
            #  子type的相关性匹配检索条件在这里
        ]
      }
    }
  }
}

base_dsl = {
  "min_score": 1,
  "size": None,
  "from": None,
  "query": {
    "bool": {
      "should": [
      ],
      "must": [
      ],
      "must_not": [
      ]
    }
  }
}

get_fragment_not_score_dsl = {
    "sort": [
        {
            "view_number": {
                "order": "desc"
            }
        },
        {
            "praise_number": {
                "order": "desc"
            }
        }
    ],
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "status": {
                            "from": 0
                        }
                    }
                }
            ]
        }
    },
    "from": 0,
    "size": 15
}

get_score_not_score_dsl = {
    "sort": [
        {
            "accumulate_num": {
                "order": "desc"
            }
        },
        {
            "status": {
                "order": "desc"
            }
        },
        {
            "course_id": {
                "order": "desc"
            }
        }
    ],
    "query": {
        "bool": {
            "filter": [],
            "must_not": []
        }
    },
    "from": 0,
    "size": 15
}

base_bool_dsl = {
    "bool": {
        "should": [],
        "minimum_should_match": 0
    }
}

count_course_by_org = {
    "query": {
        "bool": {
            "filter": [],
            "must_not": []
        }
    },
    "from": 0,
    "size": 0
}

query_org_no_score = {
  "sort": [
    {
      "course_num": {
        "order": "desc"
      }
    }
  ],
  "query": {
    "bool": {
      "filter": [
      ]
    }
  },
  "min_score": 0,
  "from": 0,
  "size": 10
}


query_staffinfo_no_score = {
  "sort": [
    {
      "staffinfo_course_num": {
        "order": "desc"
      }
    },
    {
        "staffinfo_company_md5": {
            "order": "desc"
        }
    }
  ],
  "query": {
    "bool": {
      "filter": [
      ]
    }
  },
  "min_score": 0,
  "from": 0,
  "size": 10
}

query_cid_dsl = {
  "query": {
    "bool": {
      "filter": []
    }
  },
  "size": 0,
  "aggs": {
    "cid_count": {
      "terms": {
        "field": "cid",
        "size": 500
      },
      "aggs": {
        "doc_info": {
          "top_hits": {
            "size": 1,
            "_source": ["cid", "cid_name"]
          }
        }
      }
    }
  }
}


