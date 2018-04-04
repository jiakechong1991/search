# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import datetime
from collections import OrderedDict
from tool.settings import DEF_QUERY_TYPE_ORG, DEF_QUERY_TYPE_STAFF, \
        DEF_SORT_TYPE_SCORE, DEF_SORT_TYPE_STATUS, DEF_QUERY_TYPE_CID, DEF_QUERY_TYPE_CREDICT, \
        CreditCourse
from copy import deepcopy
from dsl_node_set import base_dsl, should_dsl, base_inner_dsl, base_bool_dsl,  \
    count_course_by_org, query_org_no_score, query_cid_dsl, query_staffinfo_no_score
from tool.settings import OrgInfo, StaffTypeName, IndexName, StaffInfo, CreditCourse
import logging.config
search_logger = logging.getLogger('search.'+__name__)
from create_search_dsl import CreateFilter
from create_search_dsl import k_scala
param_default = {
            "process": 0,
            "owner": ["xuetangx", "edX"],
            "platform": 0,
            "is_paid_only": 0,
            "course_type": 0,
            "expiration": "",
            "persent": 2
        }

org_type_weight = {
    OrgInfo: 1.0
}
org_type_fields = {
    OrgInfo: ["orginfo_name"]
}


staffinfo_type_weight = {
    StaffInfo: 1.0
}
staffinfo_type_fields = {
    StaffInfo: ["staffinfo_name"]
}
# -----------------------学分课---------------------
credit_type_weight = {
    CreditCourse: 1.0
}

credit_type_fields = {
    CreditCourse: ["creditcoursetp_course_id", "creditcoursetp_l1_name", "creditcoursetp_name"]
}


def create_org_dsl(sum_token_score_w, param, count=False):
    # 构造DSL
    size = param['num']
    offset = param['pn'] * size + param['offset']

    filter_list = [
        {
            "range": {
                "course_num": {
                    "from": 1
                }
            }
        }
    ]

    st = param['st']
    if count:
        size = 0
        offset = 0

    if st == DEF_SORT_TYPE_SCORE:
        org_dsl = deepcopy(base_dsl)
        org_dsl["size"] = size
        org_dsl["from"] = offset

        token_list_t = sorted(sum_token_score_w, key=lambda token: sum_token_score_w[token], reverse=True)
        should_has_child_list = list()
        must_has_child_list = list()
        must_not_has_child_list = list()

        for item_token in token_list_t:
            token_query_temp = list()
            for item_type in org_type_weight:

                dsl_node = deepcopy(should_dsl)
                if item_token == param["query"]:
                    # and 增强 query本身,保证完全匹配
                    dsl_node["multi_match"]["boost"] = k_scala * sum(sum_token_score_w.values()) * \
                                                       org_type_weight[item_type]
                    dsl_node["multi_match"]["analyzer"] = "ik_max_word"
                    dsl_node["multi_match"]["operator"] = "and"

                else:
                    dsl_node["multi_match"]["boost"] = sum_token_score_w[item_token] * org_type_weight[item_type]

                dsl_node["multi_match"]["fields"] = org_type_fields[item_type]
                dsl_node["multi_match"]["query"] = item_token
                token_query_temp.append(dsl_node)

            inner_bool_node = deepcopy(base_inner_dsl)
            inner_bool_node["bool"]["should"].extend(token_query_temp)
            # token_query_temp : 一个token在各域收集完成并封装成基本单元
            if item_token in param["must_tokens"]:
                must_has_child_list.append(inner_bool_node)

            elif item_token in param["should_tokens"]:
                should_has_child_list.append(inner_bool_node)

            elif item_token in param["must_not_tokens"]:
                must_not_has_child_list.append(inner_bool_node)
            else:
                search_logger.info(item_token)
                search_logger.info(param["must_tokens"])
                search_logger.info(param["should_tokens"])
                raise Exception, u"token 必须在某种must OR should 中"

        outer_node_conf = {
            "should": [should_has_child_list, param["should_p"]],
            "must": [must_has_child_list, param["must_p"]],
            "must_not": [must_not_has_child_list, param["must_not_p"]]

        }
        for item_key in outer_node_conf:
            temp_list2 = outer_node_conf[item_key][0]
            if temp_list2:
                outer2_bool_node = deepcopy(base_bool_dsl)
                outer2_bool_node["bool"]["should"] = temp_list2
                outer2_bool_node["bool"]["minimum_should_match"] = outer_node_conf[item_key][1]

                org_dsl["query"]["bool"][item_key].extend([outer2_bool_node])
        org_dsl["query"]["bool"]["filter"] = filter_list
    elif st == DEF_SORT_TYPE_STATUS:

        # 自然排序召回知识点
        org_no_score_dsl = deepcopy(query_org_no_score)
        org_no_score_dsl["size"] = size
        org_no_score_dsl["from"] = offset
        org_no_score_dsl["query"]["bool"]["filter"].extend(filter_list)
        org_dsl = org_no_score_dsl

    if st == DEF_SORT_TYPE_SCORE:
        org_dsl["min_score"] = 1.1

    return org_dsl


def create_staffinfo_dsl(sum_token_score_w, param, count=False):
    # 构造DSL
    size = param['num']
    offset = param['pn'] * size + param['offset']
    filter_list = [
        {
            "range": {
                "staffinfo_course_num": {
                    "from": 1
                }
            }
        }
    ]


    st = param['st']
    if count:
        size = 0
        offset = 0

    if st == DEF_SORT_TYPE_SCORE:
        staffinfo_dsl = deepcopy(base_dsl)
        staffinfo_dsl["size"] = size
        staffinfo_dsl["from"] = offset

        token_list_t = sorted(sum_token_score_w, key=lambda token: sum_token_score_w[token], reverse=True)
        should_has_child_list = list()
        must_has_child_list = list()
        must_not_has_child_list = list()

        for item_token in token_list_t:
            token_query_temp = list()
            for item_type in staffinfo_type_weight:

                dsl_node = deepcopy(should_dsl)
                if item_token == param["query"]:
                    # and 增强 query本身,保证完全匹配
                    dsl_node["multi_match"]["boost"] = k_scala * sum(sum_token_score_w.values()) * \
                                                       staffinfo_type_weight[item_type]
                    dsl_node["multi_match"]["analyzer"] = "ik_max_word"
                    dsl_node["multi_match"]["operator"] = "and"

                else:
                    dsl_node["multi_match"]["boost"] = sum_token_score_w[item_token] * staffinfo_type_weight[item_type]

                dsl_node["multi_match"]["fields"] = staffinfo_type_fields[item_type]
                dsl_node["multi_match"]["query"] = item_token
                token_query_temp.append(dsl_node)

            inner_bool_node = deepcopy(base_inner_dsl)
            inner_bool_node["bool"]["should"].extend(token_query_temp)
            # token_query_temp : 一个token在各域收集完成并封装成基本单元
            if item_token in param["must_tokens"]:
                must_has_child_list.append(inner_bool_node)

            elif item_token in param["should_tokens"]:
                should_has_child_list.append(inner_bool_node)

            elif item_token in param["must_not_tokens"]:
                must_not_has_child_list.append(inner_bool_node)
            else:
                search_logger.info(item_token)
                search_logger.info(param["must_tokens"])
                search_logger.info(param["should_tokens"])
                raise Exception, u"token 必须在某种must OR should 中"

        outer_node_conf = {
            "should": [should_has_child_list, param["should_p"]],
            "must": [must_has_child_list, param["must_p"]],
            "must_not": [must_not_has_child_list, param["must_not_p"]]

        }
        for item_key in outer_node_conf:
            temp_list2 = outer_node_conf[item_key][0]
            if temp_list2:
                outer2_bool_node = deepcopy(base_bool_dsl)
                outer2_bool_node["bool"]["should"] = temp_list2
                outer2_bool_node["bool"]["minimum_should_match"] = outer_node_conf[item_key][1]

                staffinfo_dsl["query"]["bool"][item_key].extend([outer2_bool_node])
        staffinfo_dsl["query"]["bool"]["filter"] = filter_list
    elif st == DEF_SORT_TYPE_STATUS:

        # 自然排序召回知识点
        org_no_score_dsl = deepcopy(query_staffinfo_no_score)
        org_no_score_dsl["size"] = size
        org_no_score_dsl["from"] = offset
        org_no_score_dsl["query"]["bool"]["filter"].extend(filter_list)
        staffinfo_dsl = org_no_score_dsl

    if st == DEF_SORT_TYPE_SCORE:
        staffinfo_dsl["min_score"] = 1.1

    # print json.dumps(staffinfo_dsl)

    return staffinfo_dsl


def create_cid_dsl(param, count):

    st = param['st']
    assert st == DEF_SORT_TYPE_STATUS, u"st 参数检查失败"
    # 自然排序召回知识点
    param_ = deepcopy(param_default)
    if param["org"]:
        param_["org"] = param["org"]
    filter_dsl = CreateFilter().get_filter_list(param_, "must")
    must_not_dsl = CreateFilter().get_filter_list(param_, "not")
    res_dsl = deepcopy(query_cid_dsl)
    res_dsl["query"]["bool"]["filter"] = filter_dsl
    res_dsl["query"]["bool"]["must_not"] = must_not_dsl

    return res_dsl


def credit_param_conf(param):
    param_filter = {
        "visible": ["terms", "creditcoursetp_visible", []],
        "is_enroll": ["terms", "creditcoursetp_is_enroll", []],
        "is_apply": ["terms", "creditcoursetp_is_apply", []],
        "stage": ["terms", "creditcoursetp_stage_id", []],
        "first_category": ["terms", "parent_id", []],
        "second_category": ["terms", "category_id", []],
        "order_time": ["sort", "creditcoursetp_start"],
        "order_user": ["sort", "creditcoursetp_student_num"],
        "time_status": ["range", "creditcoursetp_start", "creditcoursetp_end"],
        "school": ["terms", "creditcoursetp_l1_id", []]
    }

    filter_list = []
    order_list = []

    for item_key in param_filter:
        if item_key in param and param[item_key] is not None:
            if param_filter[item_key][0] == "sort":
                sort_ = {
                    param_filter[item_key][1]: {
                        "order": "desc"
                    }
                }
                order_list.append(sort_)
            elif param_filter[item_key][0] == "terms":
                filter_ = {
                    "terms": {
                        param_filter[item_key][1]: param[item_key]
                    }
                }
                filter_list.append(filter_)
            elif param_filter[item_key][0] == "range":
                print param[item_key], "--**--"*10
                if param[item_key] == -1:  # 结课- 结课时间比当前小
                    range_ = {"range": {
                                param_filter[item_key][2]: {
                                    "lt": "now"}}}
                    filter_list.append(range_)
                elif param[item_key] == 0:  # 正在开课
                    range_ = {"range": {  # 结课时间比当前大
                        param_filter[item_key][2]: {
                            "gte": "now"}}}
                    filter_list.append(range_)
                    range_ = {"range": {  # 开课时间比当前小
                        param_filter[item_key][1]: {
                            "lte": "now"}}}
                    filter_list.append(range_)

                elif param[item_key] == 1:  # 将开课- 开课时间比当前大
                    range_ = {"range": {
                        param_filter[item_key][1]: {
                            "gt": "now"}}}
                    filter_list.append(range_)
    return filter_list, order_list


def create_creditcourse_dsl(sum_token_score_w, param, sum_token_analyzer, count=False):
    # 构造DSL
    size = param['num']
    offset = param['pn'] * size + param['offset']

    filter_list, sort_list = credit_param_conf(param)
    print filter_list, sort_list, "---++++====="
    st = param['st']
    if count:
        size = 0
        offset = 0

    if st == DEF_SORT_TYPE_SCORE:
        org_dsl = deepcopy(base_dsl)
        org_dsl["size"] = size
        org_dsl["from"] = offset

        token_list_t = sorted(sum_token_score_w, key=lambda token: sum_token_score_w[token], reverse=True)
        should_has_child_list = list()
        must_has_child_list = list()
        must_not_has_child_list = list()

        for item_token in token_list_t:
            token_query_temp = list()
            for item_type in credit_type_weight:

                dsl_node = deepcopy(should_dsl)
                if item_token == param["query"]:
                    # and 增强 query本身,保证完全匹配
                    dsl_node["multi_match"]["boost"] = k_scala * sum(sum_token_score_w.values()) * \
                                                       credit_type_weight[item_type]
                    dsl_node["multi_match"]["analyzer"] = sum_token_analyzer.get(item_token, "ik_max_word")
                    dsl_node["multi_match"]["operator"] = "and"

                else:
                    dsl_node["multi_match"]["boost"] = sum_token_score_w[item_token] * credit_type_weight[item_type]

                dsl_node["multi_match"]["fields"] = credit_type_fields[item_type]
                dsl_node["multi_match"]["query"] = item_token
                token_query_temp.append(dsl_node)

            inner_bool_node = deepcopy(base_inner_dsl)
            inner_bool_node["bool"]["should"].extend(token_query_temp)
            # token_query_temp : 一个token在各域收集完成并封装成基本单元
            if item_token in param["must_tokens"]:
                must_has_child_list.append(inner_bool_node)

            elif item_token in param["should_tokens"]:
                should_has_child_list.append(inner_bool_node)

            elif item_token in param["must_not_tokens"]:
                must_not_has_child_list.append(inner_bool_node)
            else:
                search_logger.info(item_token)
                search_logger.info(param["must_tokens"])
                search_logger.info(param["should_tokens"])
                raise Exception, u"token 必须在某种must OR should 中"

        outer_node_conf = {
            "should": [should_has_child_list, param["should_p"]],
            "must": [must_has_child_list, param["must_p"]],
            "must_not": [must_not_has_child_list, param["must_not_p"]]

        }
        for item_key in outer_node_conf:
            temp_list2 = outer_node_conf[item_key][0]
            if temp_list2:
                outer2_bool_node = deepcopy(base_bool_dsl)
                outer2_bool_node["bool"]["should"] = temp_list2
                outer2_bool_node["bool"]["minimum_should_match"] = outer_node_conf[item_key][1]

                org_dsl["query"]["bool"][item_key].extend([outer2_bool_node])
        org_dsl["query"]["bool"]["filter"] = filter_list
    elif st == DEF_SORT_TYPE_STATUS:

        # 自然排序召回知识点
        org_no_score_dsl = deepcopy(query_org_no_score)
        org_no_score_dsl["size"] = size
        org_no_score_dsl["from"] = offset
        org_no_score_dsl["query"]["bool"]["filter"].extend(filter_list)
        org_no_score_dsl["sort"] = sort_list
        org_dsl = org_no_score_dsl

    if st == DEF_SORT_TYPE_SCORE:
        org_dsl["min_score"] = 0

    print json.dumps(org_dsl), "----------"
    return org_dsl


def process_credit_res(res, param, hl_obj, highlight_tokens):
    object_list = list()
    total_num = res["hits"]["total"]
    from tool.base_tool import remove_key_prefix_4_dict
    for item in res["hits"]["hits"]:
        item_source = item["_source"]
        item_source = remove_key_prefix_4_dict(CreditCourse+"tp", item_source)
        item_source["score"] = item["_score"]

        list_ = []
        if "category_id" in item_source:
            if not isinstance(item_source["category_id"], list):
                item_source["category_id"] = [item_source["category_id"]]
                item_source["category_name"] = [item_source["category_name"]]
            assert len(item_source["category_id"]) == len(item_source["category_name"]), u"长度必须一致"
            for index in range(len(item_source["category_id"])):
                dict_ = {}
                dict_["id"] = item_source["category_id"][index]
                dict_["name"] = item_source["category_name"][index]
                list_.append(dict_)
            item_source["categorys"] = list_

        if "parent_id" in item_source:
            list_ = []
            if not isinstance(item_source["parent_id"], list):
                item_source["parent_id"] = [item_source["parent_id"]]
                item_source["parent_name"] = [item_source["parent_name"]]
            assert len(item_source["parent_name"]) == len(item_source["parent_id"]), u"长度必须一致parent_name"
            for index in range(len(item_source["parent_id"])):
                dict_ = {}
                dict_["id"] = item_source["parent_id"][index]
                dict_["name"] = item_source["parent_name"][index]
                list_.append(dict_)
            item_source["parent"] = list_

        if "stage_id" in item_source:
            list_ = []
            print item_source["stage_id"]
            if not isinstance(item_source["stage_id"], list):
                item_source["stage_id"] = [item_source["stage_id"]]
                item_source["stage_name"] = [item_source["stage_name"]]
                item_source["stage_weight"] = [item_source["stage_weight"]]

            assert len(item_source["stage_id"]) == len(item_source["stage_id"]), u"长度必须一致"
            assert len(item_source["stage_id"]) == len(item_source["stage_weight"]), u"长度必须一致"
            for index in range(len(item_source["stage_id"])):
                dict_ = {}
                dict_["id"] = item_source["stage_id"][index]
                dict_["name"] = item_source["stage_name"][index]
                dict_["weight"] = item_source["stage_weight"][index]
                list_.append(dict_)

            item_source["stages"] = list_

        for key_ in ["stage_id", "parent_id", "parent_name", "stage_name", "stage_weight", "category_id", "category_name"]:
            try:
                del item_source[key_]
            except Exception, e:
                pass

        if param["st"] == DEF_SORT_TYPE_SCORE:
            hl_obj.set_seg_query(highlight_tokens)
            item_source["l1_name"] = hl_obj.abstract(item_source["l1_name"])
            item_source["name"] = hl_obj.abstract(item_source["name"])
        # 添加课程的开课状态
        def get_datetime(time_str):
            try:
                time_t = datetime.datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S')
                return time_t
            except Exception, e:
                print e, time_str
                return None
        start_t = get_datetime(item_source["start"])
        end_t = get_datetime(item_source["end"])
        now = datetime.datetime.now()
        if end_t and end_t < now:  # 结课
            time_status = "-1"
        elif start_t and end_t and start_t <= now <= end_t:  # 开课中
            time_status = "0"
        elif start_t and now < start_t:  # 将开课
            time_status = "1"
        else:
            time_status = "1"

        item_source["time_status"] = time_status

        object_list.append(item_source)

    return object_list, total_num


def process_cid_res(res, param, hl_obj, highlight_tokens):

    size = param['num']
    offset = param['pn'] * size + param['offset']
    cid_list = list()
    cid_name_list = list()
    cid_doc_count = list()
    for item_bulk in res["aggregations"]["cid_count"]["buckets"]:

        if item_bulk["doc_count"]:
            key_ = item_bulk["key"]
            cid_list.append(key_)
            cid_doc_count.append(item_bulk["doc_count"])
            cid_name_list_ = item_bulk["doc_info"]["hits"]["hits"][0]["_source"]["cid_name"]
            cid_list_ = item_bulk["doc_info"]["hits"]["hits"][0]["_source"]["cid"]
            cid_name_dict_ = dict(zip(cid_list_, cid_name_list_))
            cid_name_list.append(cid_name_dict_[key_])

    res = list()
    for item_inex in range(len(cid_list)):
        temp_ = {"cid": cid_list[item_inex],
                 "cid_name": cid_name_list[item_inex],
                 "doc_count": int(cid_doc_count[item_inex])
                 }
        res.append(temp_)
    res = sorted(res, key=lambda x: x["doc_count"], reverse=True)
    total_num = len(res)

    return res[offset:offset+size], total_num


def create_dsl(sum_token_score_w, param, sum_token_analyzer, count=False):

    if param["qt"] == DEF_QUERY_TYPE_ORG:
        dsl = create_org_dsl(sum_token_score_w, param, count)

    elif param["qt"] == DEF_QUERY_TYPE_STAFF:
        dsl = create_staffinfo_dsl(sum_token_score_w, param, count)
    elif param["qt"] == DEF_QUERY_TYPE_CID:
        dsl = create_cid_dsl(param, count)
    elif param["qt"] == DEF_QUERY_TYPE_CREDICT:
        dsl = create_creditcourse_dsl(sum_token_score_w, param, sum_token_analyzer, count)
    else:
        raise Exception, u"程序运行出现未知的状态"

    return dsl


def process_res(res, param):

    def get_count_dsl(param, filter_kv):

        param_ = deepcopy(param_default)
        if param["qt"] == DEF_QUERY_TYPE_ORG:
            # 生成org对应的DSL(计算开课数)
            param_["org"] = filter_kv["orginfo_org"].lower()
            filter_dsl = CreateFilter().get_filter_list(param_, "must")
            must_not_dsl = CreateFilter().get_filter_list(param_, "not")
            # print json.dumps(filter_dsl), json.dumps(must_not_dsl)
            # 自然排序召回course
            course_dsl = deepcopy(count_course_by_org)
            course_dsl["query"]["bool"]["filter"] = filter_dsl
            course_dsl["query"]["bool"]["must_not"] = must_not_dsl
            count_dsl = course_dsl

        elif param["qt"] == DEF_QUERY_TYPE_STAFF:
            pass
            
            # 生成staff对应的DSL(计算开课数)
            count_dsl = ""
        return count_dsl

    object_list = list()
    count_type_list = list()
    total_num = res["hits"]["total"]
    for item in res["hits"]["hits"]:
        item_source = item["_source"]
        item_source["score"] = item["_score"]
        if item_source["orginfo_org"] not in count_type_list:
            count_type_list.append(item_source["orginfo_org"])
        object_list.append(item_source)

    dsl_all_in_body = ""
    query_dsl_head = {"index": IndexName}
    for item_object in count_type_list:
        if param["qt"] == DEF_QUERY_TYPE_ORG:
            filter_kv = {"orginfo_org": item_object}
        elif param["qt"] == DEF_QUERY_TYPE_STAFF:
            pass
            # filter_kv = {"orginfo_org": item_object}
        item_dsl = get_count_dsl(param, filter_kv)
        dsl_all_in_body += json.dumps(query_dsl_head) + '\n' + json.dumps(item_dsl) + '\n'
    from micro_app_search import SearchAppCore
    result = SearchAppCore.es.msearch(body=dsl_all_in_body)
    count_type_res_list = []  # 计算各种类型的开课数量
    for item_res in result["responses"]:
        count_type_res_list.append(item_res["hits"]["total"])
    type_count_dict = dict(zip(count_type_list, count_type_res_list))
    for item_object in object_list:
        if param["qt"] == DEF_QUERY_TYPE_ORG:
            key_ = item_object["orginfo_org"]
        elif param["qt"] == DEF_QUERY_TYPE_STAFF:
            key_ = ""
        item_object["course_num"] = type_count_dict[key_]

    # 封装删减润色res

    return object_list, total_num


def process_staffinfo_res(res, param, hl_obj, highlight_tokens):
    object_list = list()
    total_num = res["hits"]["total"]
    for item in res["hits"]["hits"]:
        item_source = dict()
        for item_key in item["_source"]:
            new_key = item_key
            if "staffinfo" in item_key:
                new_key = item_key.replace("staffinfo_", "")

            item_source[new_key] = item["_source"][item_key]
        item_source["score"] = item["_score"]
        del item_source["company_md5"]
        # if param["st"] ==  DEF_SORT_TYPE_SCORE:
        #     hl_obj.set_seg_query(highlight_tokens)
        #     item_source["orginfo_name"] = hl_obj.abstract(item_source["orginfo_name"])



        object_list.append(item_source)

    return object_list, total_num


def process_org_res(res, param, hl_obj, highlight_tokens):
    object_list = list()
    total_num = res["hits"]["total"]
    for item in res["hits"]["hits"]:
        item_source = item["_source"]
        item_source["score"] = item["_score"]
        # if param["st"] ==  DEF_SORT_TYPE_SCORE:
        #     hl_obj.set_seg_query(highlight_tokens)
        #     item_source["orginfo_name"] = hl_obj.abstract(item_source["orginfo_name"])
        object_list.append(item_source)

    return object_list, total_num


precess_func_dict = {
    # org 的process pipeline
    DEF_QUERY_TYPE_ORG: {
        "create_dsl": create_dsl,
        "process_res": process_org_res
    },
    # staff 的process pipeline
    DEF_QUERY_TYPE_STAFF: {
        "create_dsl": create_dsl,
        "process_res": process_staffinfo_res
    },
    DEF_QUERY_TYPE_CID: {
        "create_dsl": create_dsl,
        "process_res": process_cid_res
    },
    DEF_QUERY_TYPE_CREDICT: {
        "create_dsl": create_dsl,
        "process_res": process_credit_res
    }

}