# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import time
import math
from datetime import datetime, timedelta, date
import os
from tool.models import es_instance
from micro_app_creat_dsl import precess_func_dict
from tool.settings import conf, DEBUG_FLAG
from tool.base_tool import print_debug
from tool.highlight import Highlighting
from tool.settings import IndexName, DEF_QUERY_TYPE_ORG, DEF_QUERY_TYPE_STAFF, \
    OrgInfo, StaffTypeName, DEF_QUERY_TYPE_CID, CourseTypeName, StaffInfo, CreditCourse, \
    DEF_QUERY_TYPE_CREDICT

import logging.config
from query_nlp import analyse_query_all
from create_search_dsl import create_dsl, get_main_info
search_logger = logging.getLogger('search.'+__name__)
Page_Size = 15
FORMAT_DATE = "%Y-%m-%d %H:%M:%S"


class SearchAppCore(object):
    max_query = 20
    es = es_instance()
    index = IndexName
    # obj.set_seg_query(lst_query)
    # str_abst = obj.abstract('艺术创造美,创业的现象,创和业的结。')
    # print str_abst

    qt_type = {
        DEF_QUERY_TYPE_ORG: OrgInfo,
        DEF_QUERY_TYPE_STAFF: StaffInfo,
        DEF_QUERY_TYPE_CID: CourseTypeName,
        DEF_QUERY_TYPE_CREDICT: CreditCourse

    }

    @staticmethod
    def param_check(param):
        print json.dumps(param)



    @staticmethod
    def get_result(param):

        # 参数解析
        hl_obj = Highlighting(w=56)
        SearchAppCore.param_check(param)
        highlight_tokens = set()

        # query 分析后的结果写在了param
        sum_token_score_w, sum_token_analyzer = analyse_query_all(param["query"], param, highlight_tokens, SearchAppCore.es)
        # print sum_token_score_w
        count = False
        # 创建DSL
        dsl = precess_func_dict[param["qt"]]["create_dsl"](sum_token_score_w, param, sum_token_analyzer, count)

        # 首次召回实体
        res = SearchAppCore.es.search(index=IndexName, doc_type=SearchAppCore.qt_type[param["qt"]],
                                      body=dsl, preference="_primary_first")

        # 二次填充数据
        res, object_num = precess_func_dict[param["qt"]]["process_res"](res, param, hl_obj, highlight_tokens)
        result_json = {
            "data": res,
            "param": param,
            "total_num": object_num,
            "error_code": 0,
            "error_msg": "",
        }

        return result_json


if __name__ == "__main__":
    pass
    param = {
            'qt': DEF_QUERY_TYPE_ORG,  # DEF_QUERY_TYPE_STAFF
            'query': u'清华',
            'pn': 0,
            'num': 15,
            'offset': 0,

            'org': None,  # 学校机构
            "staff": None,  # 指定教师ID

            # 这些参数作为预备参数，之后重定向调用到主站的接口函数
            # 'owner': [u'xuetangx', u'edx'],
            # 'process': 0,
            # 'group': None,
            # 'cid': None,
            # 'serialized': None,
            # 'expiration': '',
            # 'is_paid_only': 0,
            # 'st': 2,
            # 'hasTA': None,
            # 'course_id': None,
            # 'mode': None,
            # 'platform': 0,
            # 'course_type': 0
        }
    print SearchAppCore.get_result(param)







