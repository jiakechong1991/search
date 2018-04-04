# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import time
import requests
import math
from datetime import datetime, timedelta, date
import os
from tool.models import es_instance
from tool.settings import conf, DEBUG_FLAG
from tool.base_tool import print_debug
from tool.highlight import Highlighting
from tool.settings import IndexName
from tool.settings import DEF_QUERY_TYPE_MIX_CK, DEF_QUERY_TYPE_COURSE, \
    DEF_QUERY_TYPE_ENGINEER, DEF_QUERY_TYPE_KNOWLE, DEF_QUERY_TYPE_MICRODEGREE, \
    DEF_QUERY_TYPE_LIVE, DEF_SORT_TYPE_STATUS, DEF_SORT_TYPE_SCORE

import logging.config
from query_nlp import analyse_query_all
from create_search_dsl import create_dsl, get_main_info
search_logger = logging.getLogger('search.'+__name__)
Page_Size = 15
FORMAT_DATE = "%Y-%m-%d %H:%M:%S"

highlight_tokens = set()


def sort_func_by_status(x, y):
    # 选课人数--->课程状态---->课程ID字典顺序

    if x.get("accumulate_num", 0) > y.get("accumulate_num", 0):
        return 1
    elif x.get("accumulate_num", 0) < y.get("accumulate_num", 0):
        return -1
    else:
        if x.get("status", 0) > y.get("status", 0):
            return 1
        elif x.get("status", 0) < y.get("status", 0):
            return -1
        else:
            if x.get("course_id", 0) > y.get("course_id", 0):
                return 1
            elif x.get("course_id", 0) <= y.get("course_id", 0):
                return -1
            else:
                return 1


def process_result(res_out_list):

    def has_ta(item):
        if item["owner"] == "xuetangX":
            # 此处注意 and/or 的执行顺序
            if item["start"] != None and item["start"] < datetime.strftime(datetime.now(), FORMAT_DATE):
                if item["end"] == None or item["end"] > datetime.strftime(datetime.now(), FORMAT_DATE):
                    return "True"
        return "False"

    for item in res_out_list:

        item_type = item["item_type"]
        if item_type == "fragment":
            will_get_field = ["cid", "serialized", "cid_name", "last_chapter",
                              "enroll_num", "accumulate_num", "start", "end", "owner"]
            for course_fields_temp in will_get_field:
                item[course_fields_temp] = item["sub_course"][course_fields_temp]

            del item["sub_course"]
            will_del_field = ["frag_desc", "frag_title", "ut", "item_type", "status"]
            for item_del in will_del_field:
                del item[item_del]

        if item_type == "course":
            item["hasTA"] = has_ta(item)
            will_del_field = ["owner", "end", "item_type", "start", "status", "is_paid_only",
                              "expire", "org", "ut", "course_type", "mode"]
            for item_del in will_del_field:
                del item[item_del]
            item["accumulate_num"] = item.get("accumulate_num_v2", 0)

        if item_type == "live":
            will_del_field = []
            for item_del in will_del_field:
                del item[item_del]

        if item_type == "course":
            will_del_field = []
            for item_del in will_del_field:
                del item[item_del]


class SearchCore(object):

    max_query = 20
    es = es_instance()
    index = IndexName


    @staticmethod
    def high_light(item_class_dict, query_tokens, param):
        obj = Highlighting(w=56)
        obj.set_seg_query(query_tokens)

        for item_resource in item_class_dict.itervalues():

            if item_resource["item_type"] == "course":
                if "sub_about" in item_resource:
                    item_resource["highlight"]["about"] = obj.abstract(item_resource["sub_about"]["about"])
                    del item_resource["sub_about"]
                if "sub_title" in item_resource:
                    item_resource["highlight"]["title"] = obj.abstract(item_resource["sub_title"]["title"])
                    del item_resource["sub_title"]
                if "sub_staff" in item_resource:
                    for item in ["department", "position", "name", "company"]:
                        item_resource["highlight"]["staff-" + item] = obj.abstract(item_resource["sub_staff"][item])
                    del item_resource["sub_staff"]
                if "sub_structure" in item_resource:
                    display = item_resource["sub_structure"]["name"]
                    item_resource["sub_structure"]["structure"].append(display)
                    temp_str = " ".join(item_resource["sub_structure"]["structure"])
                    item_resource["highlight"]["chapter"] = obj.abstract(temp_str)
                    temp_dict = {
                        "center": {
                            "process": obj.abstract(display),
                            "display": display,
                            "location": item_resource["sub_structure"]["location"]
                        }
                    }
                    item_resource["highlight"]["structure"] = temp_dict
                    del item_resource["sub_structure"]
            if item_resource["item_type"] == "fragment":
                item_resource["highlight"]["frag_title"] = obj.abstract(item_resource["frag_title"])
                item_resource["highlight"]["frag_desc"] = obj.abstract(item_resource["frag_desc"])

            if item_resource["item_type"] == "live":
                item_resource["highlight"]["live_album_sub_title"] = obj.abstract(item_resource["live_album_sub_title"])
                item_resource["highlight"]["live_album_title"] = obj.abstract(item_resource["live_album_title"])
                item_resource["highlight"]["live_item_title"] = obj.abstract(item_resource["live_item_title"])

            if item_resource["item_type"] == "microdegree":
                item_resource["highlight"]["microdegree_sub_title"] = obj.abstract(item_resource["microdegree_sub_title"])
                item_resource["highlight"]["match_course"] = obj.abstract(item_resource["match_course"])
                item_resource["highlight"]["microdegree_title"] = obj.abstract(item_resource["microdegree_title"])
                item_resource["highlight"]["tips"] = obj.abstract("包含{q}相关的课程:".format(q=param["query"]))

    @staticmethod
    def param_check(param):
        qt = param["qt"]
        st = param["st"]
        version = param["version"]
        home = param["home"]
        log_str = u"param_check:参数检查失败"
        if version == 2:  # 新版接口(直播和微学位)
            pass
            if home == 1:
                # 检查 主页只能对这几种资源只能用相关性排序
                allow_qt = [
                    DEF_QUERY_TYPE_MIX_CK,
                    DEF_QUERY_TYPE_COURSE,
                    DEF_QUERY_TYPE_KNOWLE,
                    # DEF_QUERY_TYPE_ENGINEER, 此时不能使用工程硕士
                    DEF_QUERY_TYPE_LIVE,
                    DEF_QUERY_TYPE_MICRODEGREE
                ]
                assert qt in allow_qt, u"version=2,home=1,qt失败"
            elif home == 0:
                assert qt != DEF_QUERY_TYPE_MIX_CK, u"qt=1请搭配home=1"
                # 检查 live 和 microdegree只能用相关性排序
                if qt in [DEF_QUERY_TYPE_LIVE, DEF_QUERY_TYPE_MICRODEGREE]:
                    assert st == DEF_SORT_TYPE_SCORE, log_str

        elif version == 1:  # 旧版接口
            pass
            # 参数矫正,以使v1接口能使用v2接口:

            # app 的主筛选框
            if qt == DEF_QUERY_TYPE_MIX_CK and st == DEF_SORT_TYPE_STATUS:
                param["qt"] = DEF_QUERY_TYPE_COURSE

        else:
            raise Exception, "version参数有误"

    @staticmethod
    def get_result(param):
        # query = u"花卉学"
        # query = u"大学化学"
        SearchCore.param_check(param)
        query = param["query"]
        t1 = time.time()
        highlight_tokens.clear()
        # 配置分词器行为

        # query 分析后的结果写在了param
        sum_token_score_w, sum_token_analyzer = analyse_query_all(query, param, highlight_tokens, SearchCore.es)
        # search_logger.info(json.dumps(param))

        dsl_list, index_end = create_dsl(sum_token_score_w, param, sum_token_analyzer)
        # print "++++" * 15
        # print json.dumps(dsl_list)
        # print "++++" * 15

        # 搜索ES,获取指定的返回父文档结果
        dsl_all_in_body = ""
        query_dsl_head = {"index": IndexName, "preference": "_primary_first"}
        for item_dsl in dsl_list:
            dsl_all_in_body += json.dumps(query_dsl_head) + '\n' + json.dumps(item_dsl) + '\n'
        log_str = u"父得分首次 召回结束"
        result = SearchCore.es.msearch(body=dsl_all_in_body)
        # search_logger.info("{b}==={a}".format(b=log_str, a=str(time.time() - t1)))
        # print "------"*10
        # print json.dumps(result)
        # print "------" * 10

        if param['st'] == DEF_SORT_TYPE_SCORE and param["persent"] < 10:

            second_search = False
            # 测试是否需要二次检索: 只有all标签为0才进行二次检索
            if param["home"] == 1:  # 主搜索框的检索
                if param["qt"] == DEF_QUERY_TYPE_MIX_CK:
                    if result["responses"][0]["hits"]["total"] == 0:
                        # 全部标签为0
                        second_search = True
                        search_logger.info("all资源为0")

            else:  # 对单资源的检索
                assert param["qt"] in [DEF_QUERY_TYPE_MIX_CK, DEF_QUERY_TYPE_COURSE, DEF_QUERY_TYPE_KNOWLE,
                                       DEF_QUERY_TYPE_LIVE, DEF_QUERY_TYPE_MICRODEGREE], "参数检查失败2"

                if result["responses"][0]["hits"]["total"] == 0:
                    second_search = True
                    search_logger.info("单项召回时,资源为0")
            if second_search:
                search_logger.info(u"触发二次检索")
                param["must_tokens"] = []
                param["should_tokens"] = sum_token_score_w.keys()
                param["persent_scale"] = len(param["should_tokens"]) / 5 + 1 if len(param["should_tokens"]) > 4 else 0

                if DEBUG_FLAG:
                    search_logger.info('----二次should----')
                    for j in param["should_tokens"]:
                        search_logger.info(j)

                dsl_list, index_end = create_dsl(sum_token_score_w, param, sum_token_analyzer)
                # print json.dumps(dsl_list)
                # print "---**--"*10
                dsl_all_in_body = ""
                query_dsl_head = {"index": IndexName, "preference": "_primary_first"}
                for item_dsl in dsl_list:

                    dsl_all_in_body += json.dumps(query_dsl_head) + '\n' + json.dumps(item_dsl) + '\n'
                log_str = u"父得分二次 召回结束"
                result = SearchCore.es.msearch(body=dsl_all_in_body)
                search_logger.info("{b}==={a}".format(b=log_str, a=str(time.time() - t1)))

        # 二次检索结束
        # print "--0_0--" * 10
        # print json.dumps(result),
        item_class_dict = dict()

        all_sum = None
        course_sum = None
        knowledge_sum = None
        live_sum = None
        microdegree_sum = None
        if param["home"]:

            item_sum = [
                # "all_sum"
                # "course_sum"
                # "knowledge_sum"
                # "live_sum"
                # "microdegree_sum"
            ]
            # 多类资源召回
            for index_t, item_resource in enumerate(result["responses"]):
                # print item_resource
                assert item_resource["status"] == 200, u"ES结果报错"
                this_sum = item_resource["hits"]["total"]
                item_sum.append(this_sum)
                if (index_t+1) == index_end[0][0]:
                    # 这是要显示的资源栏
                    # print "—+++——"*10
                    # print len(item_resource["hits"]["hits"])
                    for item in item_resource["hits"]["hits"]:
                        # 创建 字典结构中的key
                        # print item["_type"], "+++++++"
                        # 工程硕士也是使用这个
                        if item["_type"] == "course":
                            couse_id = item["_source"]["course_id"]
                            item_id = couse_id

                        elif item["_type"] == "fragment":
                            knowledge_id = item["_source"]["course_id"] + item["_source"]["knowledge_id"]
                            item_id = knowledge_id

                        elif item["_type"] == "live":
                            live_id = item["_source"]["live_key"]
                            item_id = live_id

                        elif item["_type"] == "microdegree":
                            microdegree_id = "{t}_{k}".format(t="microdegree", k=item["_source"]["microdegree_key"])
                            item_id = microdegree_id
                            del item["_source"]["course_info_list"]

                        else:
                            raise Exception, "出现不明的异常类型"

                        # search_logger.info("这是一个{t}:{s}".format(t=item["_type"], s=item_id))

                        item_class_dict[item_id] = item["_source"]
                        item_class_dict[item_id]["item_type"] = item["_type"]
                        item_class_dict[item_id]["score"] = item["_score"]
                        item_class_dict[item_id]["highlight"] = dict()

            all_sum = item_sum[0]
            course_sum = item_sum[1]
            knowledge_sum = item_sum[2]
            live_sum = item_sum[3]
            microdegree_sum = item_sum[4]
        else:

            # 单类资源召回,就会循环一次
            # print json.dumps(result)
            # print "-++-"*10
            for index_t, item_resource in enumerate(result["responses"]):
                # print item_resource
                assert item_resource["status"] == 200, u"ES结果报错"
                all_sum = item_resource["hits"]["total"]
                # print index_end[0][0], "___"
                for item in item_resource["hits"]["hits"]:  # 这个会遍历进行
                    # 工程硕士也是使用这个
                    resource_type = item["_type"]
                    if item["_type"] == "course":
                        couse_id = item["_source"]["course_id"]
                        item_id = couse_id
                        course_sum = all_sum

                    elif item["_type"] == "fragment":
                        knowledge_id = item["_source"]["course_id"] + item["_source"]["knowledge_id"]
                        item_id = knowledge_id
                        knowledge_sum = all_sum

                    elif item["_type"] == "live":
                        live_id = item["_source"]["live_key"]
                        item_id = live_id
                        live_sum = all_sum

                    elif item["_type"] == "microdegree":
                        microdegree_id = "{t}_{k}".format(t="microdegree", k=item["_source"]["microdegree_key"])
                        item_id = microdegree_id
                        del item["_source"]["course_info_list"]
                        microdegree_sum = all_sum

                    else:
                        print  "---------"*10
                        print json.dumps(item)
                        raise Exception, "出现不明的异常类型"

                    # search_logger.info("这是一个{t}:{s}".format(t=item["_type"], s=item_id))

                    item_class_dict[item_id] = item["_source"]
                    item_class_dict[item_id]["item_type"] = item["_type"]
                    item_class_dict[item_id]["score"] = item["_score"]
                    item_class_dict[item_id]["highlight"] = dict()
            # 为兼容以前的接口返回参数

        # search_logger.info("all_sum:{a} course_sum:{b} knowledge_sum:{c} live_sum:{l} microdegree_sum:{m}".format(
        #     a=all_sum, b=course_sum, c=knowledge_sum, l=live_sum, m=microdegree_sum
        #     ))

        # print "_____"*10
        # print json.dumps(item_class_dict)
        # print "_____" * 10
        get_main_info(item_class_dict, param["query"], SearchCore.es, param)
        # print json.dumps(item_class_dict)
        # print "_____" * 10

        if param["st"] == DEF_SORT_TYPE_SCORE:
            SearchCore.high_light(item_class_dict, highlight_tokens, param)
            res_out_list = sorted(item_class_dict.values(), key=lambda x: x.get("score", 0), reverse=True)
        else:
            res_out_list = sorted(item_class_dict.values(), cmp=sort_func_by_status, reverse=True)

        # print json.dumps(res_out_list)
        # print "_*_*_*_*_" * 10

        process_result(res_out_list)
        # print json.dumps(course_list)

        result_json = {
            "data": res_out_list,
            "param": param,
            "total": {
                "microdegree": microdegree_sum if microdegree_sum else 0,
                "live": live_sum if live_sum else 0,
                "course": course_sum if course_sum else 0,
                "knowledge": knowledge_sum if knowledge_sum else 0,
                "all": all_sum if all_sum else 0
            },
            "error_code": 0,
            "error_msg": "",
        }

        correct_server = conf['correct']['host']
        # correct_server = "10.0.0.160"
        port = conf['correct']['port']
        if query != '':
            try:
                url = 'http://%s:%s/query_correct' % (correct_server, port)
                pyload = {'query': query}
                correct_result = requests.get(url, timeout=1, params=pyload)
                result_json['correct'] = correct_result.json().get('correct_result')
            except Exception, e:
                search_logger.warn("ERROR_CORRECT_SERVICE url={url} query = {q}".format(url=url, q=query))

        result_json["time"] = str(time.time() - t1)

        return result_json


if __name__ == "__main__":

    param = {'process': 0, 'group': None, 'qt': 1, 'cid': None, 'serialized': None, 'expiration': '', 'offset': 0, 'is_paid_only': 0, 'st': 2, 'hasTA': None, 'course_id': None, 'num': 15, 'mode': None, 'org': None, 'owner': [u'xuetangx', u'edx'], 'query': u'', 'pn': 0, 'platform': 0, 'course_type': 0}
    # SearchCore.get_tokens(u"Java工程师")
    # SearchCore.get_result(param)

    # course_list = [
    #     "course-v1:TsinghuaX+AP000001X+2017_T2",
    #     "course-v1:VPC_TsinghuaX+AP000000X+2016_T1"
    # ]*10
    # SearchCore().get_main_info(course_list, u"大学化学")
