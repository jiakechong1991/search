# -*- coding: utf-8 -*-

import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from copy import deepcopy
from datetime import datetime, timedelta, date
from tool.settings import DEF_QUERY_TYPE_MIX_CK, DEF_QUERY_TYPE_COURSE, \
    DEF_QUERY_TYPE_ENGINEER, DEF_QUERY_TYPE_KNOWLE, DEF_QUERY_TYPE_MICRODEGREE, \
    DEF_QUERY_TYPE_LIVE
from tool.settings import IndexName, DEF_SORT_TYPE_STATUS, DEF_COURSE_ORG, \
    DEF_SORT_TYPE_SCORE
from dsl_node_set import should_dsl, knowledge_dsl, course_filter_dsl, has_child_dsl, \
        base_dsl, get_fragment_not_score_dsl, get_score_not_score_dsl, nested_base, base_bool_dsl, live_dsl_base_inner
import logging.config
search_logger = logging.getLogger('search.'+__name__)
FORMAT_DATE = "%Y-%m-%d %H:%M:%S"

k_scala = 1


def get_cfg(name, param):

    knowledge_type_weight = {
        "fragment": 85
    }

    knowledge_type_fields = {
        "fragment": ["frag_title", "frag_desc"]
    }

    course_type_weight = {
        'title': 170,  # 85
        'about': 40,
        'structure': 8,
        'staff': 6,
        # 'subtitle': 45,  # 没有这个字段
        'category': 85,
    }

    course_type_fields = {  # 每个type下进行检索的字段
        "about": ["about"],
        "staff": ["position", "company", "name"],  # "department"
        "structure": ["name", "structure"],
        "category": ["cid_name"],
        "title": ["title"],
    }

    live_type_weight = {
        "live": 200
    }
    live_type_fields = {
        "live": ["live_album_title", "live_item_title", "live_album_sub_title"]
    }

    microdegree_type_weight = {
        "microdegree": 370
    }
    # 这是一种不同于上面的权重描述方式
    microdegree_type_fields = {  # 权重请全部使用小数,因为指定了总的权重,权重使用衰减的方式
        "microdegree": [
            {"microdegree_title": 1.0},
            {"microdegree_sub_title": 0.7},
            {"course_info_list": [0.7,
                                  {"name": 1.0},
                                  {"subtitle": 0.7},
                                  {"about": 0.5},
                                  {"comment_org": 0.7},
                                  {"staffs_searchable": 0.7}
                                  ]
             }
        ]
    }
    if name == "knowledge_type_weight":
        res = knowledge_type_weight
    elif name == "knowledge_type_fields":
        res = knowledge_type_fields
    elif name == "course_type_weight":
        if param["fields_v"]:
            for item in ["title", "about", "structure", "category"]:
                del course_type_weight[item]
        res = course_type_weight
    elif name == "course_type_fields":
        if param["fields_v"]:
            course_type_fields = {  # 每个type下进行检索的字段
                "staff": ["name"],  # "department"
            }
        res = course_type_fields
    elif name == "live_type_weight":
        res = live_type_weight
    elif name == "live_type_fields":
        res = live_type_fields
    elif name == "microdegree_type_weight":
        res = microdegree_type_weight
    elif name == "microdegree_type_fields":
        res = microdegree_type_fields

    return res


class CreateFilter(object):
    def get_filter_list(self, param, mode):
        # 构造该参数组所对应的过滤器
        must_list = []
        must_not_list = []
        filter_list = [
            self.get_owner_filter(param),
            self.get_org_filter(param),
            self.get_serialized_filter(param),
            self.get_course_id_filter(param),
            self.get_ta_filter(param),
            self.get_course_process(param),
            self.get_mode(param),
            self.get_expiration(param),
            self.get_course_type(param),
            self.get_is_paid_only(param)]
        # 工程硕士
        group = param.get("group")
        if None == group:
            filter_list.append(self.get_cid_filter(param))
        filter_result = []
        for item in filter_list:
            if None != item:
                if isinstance(item, list):
                    for i in item:
                        filter_result.append(i)
                else:
                    filter_result.append(item)
        status = param.get("platform")
        if status == 0:
            filter_result.append(self.get_filter('range', 'status', {'from': 0}))
        elif status == 1:
            filter_result.append({"not": self.get_filter('term', 'status', -1)})

        for item_filter in filter_result:
            if "not" in item_filter:  # 如果这是一个not过滤器
                must_not_list.append(item_filter["not"])
            else:
                must_list.append(item_filter)

        return must_list if mode == "must" else must_not_list

    def get_owner_filter(self, param):
        owner = param.get("owner")
        if owner:
            if type(owner) == list:
                return self.get_filter('terms', 'owner', owner)
            elif type(owner) == str:
                return self.get_filter('term', 'owner', owner)
            else:
                return None
        else:
            return None

    def get_org_filter(self, param):
        org = param.get("org")
        if org:
            if type(org) == list:
                return self.get_filter('terms', 'org', org)
            elif type(org) == unicode:
                if org == u"others":
                    return {"not": self.get_filter('terms', 'org', DEF_COURSE_ORG)}
                else:
                    return self.get_filter('term', 'org', org)
            else:
                return None
        else:
            return None

    def get_cid_filter(self, param):
        cid = param.get("cid")
        if None != cid:
            if type(cid) == list:
                # return self.get_filter('term', 'cid', cid)
                return self.get_filter('terms', 'cid', cid)
            elif type(cid) == int:
                return self.get_filter('term', 'cid', cid)
            else:
                return None
        else:
            return None

    def get_categorygroupid_filter(self, param):
        group = param.get("group")
        if None != group:
            return self.get_filter('term', 'group', group)
        else:
            return None

    def get_serialized_filter(self, param):
        serialized = param.get("serialized")
        if None != serialized:
            assert type(serialized) == int
            return self.get_filter('range', 'serialized', {'from': serialized})
        else:
            return None

    def get_course_type(self, param):
        course_type = param.get("course_type")
        if 0 != course_type:
            _course_type = 1 if course_type == 1 else 0
            return self.get_filter('term', 'course_type', _course_type)
        else:
            return None

    def get_is_paid_only(self, param):
        is_paid_only = param.get("is_paid_only")
        if 0 != is_paid_only:
            is_paid_only = 1 if is_paid_only == 1 else 0
            return self.get_filter('term', 'is_paid_only', is_paid_only)
        else:
            return None

    def get_course_id_filter(self, param):
        course_id = param.get("course_id")
        if None != course_id:
            assert type(course_id) == str
            return self.get_filter('term', 'course_id', course_id)
        else:
            return None

    def get_ta_filter(self, param):
        hasTA = param.get("hasTA")
        if None != hasTA:
            return [self.get_owner_filter(owner='xuetangX'),
                    self.get_filter('range', 'start',
                                    {'to': datetime.strftime(datetime.now(), FORMAT_DATE)}),
                    self.get_filter('range', 'end',
                                    {'from': datetime.strftime(datetime.now(), FORMAT_DATE)})]
        else:
            return None

    def get_course_process(self, param):
        process = param.get("process")
        if not process:
            return None
        elif 1 == process:
            return self.get_filter('range', 'start',
                                   {'gt': datetime.strftime(datetime.now(), FORMAT_DATE)}
                                   )
        elif 2 == process:
            return [self.get_filter('range', 'start',
                                    {'lte': datetime.strftime(datetime.now(), FORMAT_DATE)}
                                    ),
                    self.get_filter('range', 'end',
                                    {'gte': datetime.strftime(datetime.now(), FORMAT_DATE)}
                                    )]
        elif 3 == process:
            return self.get_filter('range', 'end',
                                   {'lt': datetime.strftime(datetime.now(), FORMAT_DATE)}
                                   )

    def get_mode(self, param):
        mode = param.get("mode")
        if mode:
            return self.get_filter("terms", "mode", mode)
        else:
            return None

    def get_expiration(self, param):
        expiration = param.get("expiration")
        if expiration == "":
            return None
        else:
            now = datetime.utcnow()

            def cond_expiration():
                date = datetime.strftime(now, "%Y-%m-%d %X")
                return self.get_filter("range", "expire", {"gte": date})

            def cond_start():
                date = datetime.strftime(now - timedelta(days=expiration), "%Y-%m-%d %X")
                return {"bool": {"must": [
                    self.get_filter("term", "expire", "-"),
                    self.get_filter("range", "start", {"gte": date})
                ]}}

            result = {"bool": {"should": [cond_expiration(), cond_start()]}}
            return result

    def get_filter(self, op, name, value):
        return {
            op: {
                name: value
            }
        }


def create_mic_ck_dsl(sum_token_score_w, param, count=False):
    mic_ck_dsl = deepcopy(base_dsl)
    size = param['num']
    offset = param['pn'] * size + param['offset']
    st = param['st']
    assert st == DEF_SORT_TYPE_SCORE, "mic_ck:请检查你的st参数"
    if count:
        size = 0
        offset = 0
    if st == DEF_SORT_TYPE_SCORE:
        for key, dsl_fun in create_dsl_dict.items():
            if key in [1, 4]:  # 过滤掉自身对自身的调用 和 工程硕士
                continue
            res_dsl = dsl_fun(sum_token_score_w, param, count)
            # print json.dumps(res_dsl), "++)))"
            mic_ck_dsl["query"]["bool"]["should"].append(res_dsl["query"])
    elif st == DEF_SORT_TYPE_STATUS:
        mic_ck_dsl = {
            "from": 0,
            "size": 0
        }
    if st == DEF_SORT_TYPE_SCORE:
        mic_ck_dsl["min_score"] = 1.1
    mic_ck_dsl["from"] = offset
    mic_ck_dsl["size"] = size
    # print json.dumps(mic_ck_dsl)
    return mic_ck_dsl


def create_course_dsl(sum_token_score_w, param, count=False):
    # 构造DSL

    filter_dsl = CreateFilter().get_filter_list(param, "must")
    must_not_dsl = CreateFilter().get_filter_list(param, "not")
    # print json.dumps(filter_dsl), json.dumps(must_not_dsl)

    size = param['num']
    offset = param['pn'] * size + param['offset']
    st = param['st']
    if count:
        size = 0
        offset = 0

    if st == DEF_SORT_TYPE_SCORE:

        course_dsl = deepcopy(base_dsl)
        course_dsl["size"] = size
        course_dsl["from"] = offset

        course_filter_temp = deepcopy(course_filter_dsl)
        course_filter_temp["has_parent"]["query"]["bool"]["filter"] = filter_dsl
        course_filter_temp["has_parent"]["query"]["bool"]["must_not"] = must_not_dsl
        should_has_child_list = list()
        must_has_child_list = list()
        must_not_has_child_list = list()

        for item_token in sum_token_score_w:
            token_query_temp = list()
            for item_type in get_cfg("course_type_weight", param):
                temp_has_child_temp = deepcopy(has_child_dsl)
                dsl_node = deepcopy(should_dsl)
                if item_token == param["query"]:
                    # and 增强 query本身,保证完全匹配
                    dsl_node["multi_match"]["boost"] = k_scala * sum(sum_token_score_w.values()) * \
                                                       get_cfg("course_type_weight", param)[item_type]
                    dsl_node["multi_match"]["analyzer"] = "ik_max_word"
                    dsl_node["multi_match"]["operator"] = "and"

                else:
                    dsl_node["multi_match"]["boost"] = sum_token_score_w[item_token] * get_cfg("course_type_weight", param)[item_type]

                dsl_node["multi_match"]["fields"] = get_cfg("course_type_fields", param)[item_type]
                dsl_node["multi_match"]["query"] = item_token

                temp_has_child_temp["has_child"]["query"]["bool"]["should"].append(dsl_node)
                temp_has_child_temp["has_child"]["type"] = item_type
                temp_has_child_temp["has_child"]["query"]["bool"]["filter"].append(course_filter_temp)
                temp_has_child_temp["has_child"]["query"]["bool"]["minimum_should_match"] = 1 # 非常重要
                token_query_temp.append(temp_has_child_temp)

            inner_bool_node = deepcopy(base_bool_dsl)
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

                course_dsl["query"]["bool"][item_key].extend([outer2_bool_node])

    elif st == DEF_SORT_TYPE_STATUS:

        # 自然排序召回course
        course_dsl = deepcopy(get_score_not_score_dsl)
        course_dsl["query"]["bool"]["filter"] = filter_dsl
        course_dsl["query"]["bool"]["must_not"] = must_not_dsl
        course_dsl["size"] = size
        course_dsl["from"] = offset

    if st == DEF_SORT_TYPE_SCORE:
        course_dsl["min_score"] = 1.1

    # print json.dumps(course_dsl)
    return course_dsl


def create_engineer_dsl(sum_token_score_w, param, count=False):
    # 构造DSL

    size = param['num']
    offset = param['pn'] * size + param['offset']
    st = param['st']
    assert st == DEF_SORT_TYPE_STATUS, "engineer:请检查你的st参数"
    if count:
        size = 0
        offset = 0

    if st == DEF_SORT_TYPE_SCORE:
        engineer_dsl = dict()

    elif st == DEF_SORT_TYPE_STATUS:

        engineer_dsl = deepcopy(base_dsl)
        engineer_dsl["query"]["bool"]["filter"] = [
            {
                'range': {'status': {'from': 0}}
            }
        ]

        del engineer_dsl["min_score"]
        dsl_small = deepcopy(has_child_dsl)
        dsl_small["has_child"]["type"] = "category"
        del dsl_small["has_child"]["score_mode"]
        # del dsl_small["has_child"]["query"]["bool"]["minimum_should_match"]
        del dsl_small["has_child"]["query"]["bool"]["should"]
        # del dsl_small["has_child"]["query"]["bool"]["must"]
        if param["group"]:
            temp_dsl_s = {
                "term": {
                    "group": param["group"]
                }
            }
            dsl_small["has_child"]["query"]["bool"]["filter"].append(temp_dsl_s)

        if param["cid"] != None:
            temp_dsl_s = {
                "terms": {
                    "cid": param["cid"]
                }
            }
            dsl_small["has_child"]["query"]["bool"]["filter"].append(temp_dsl_s)
        engineer_dsl["query"]["bool"]["filter"].append(dsl_small)
        process_dsl = CreateFilter().get_course_process(param)
        if process_dsl:
            if not isinstance(process_dsl, list):
                process_dsl = [process_dsl]
            engineer_dsl["query"]["bool"]["filter"].extend(process_dsl)
        engineer_dsl["size"] = size
        engineer_dsl["from"] = offset

    if st == DEF_SORT_TYPE_SCORE:
        engineer_dsl["min_score"] = 1.1

    # print json.dumps(engineer_dsl)

    return engineer_dsl


def create_knowle_dsl(sum_token_score_w, param, count=False):
    # 构造DSL

    size = param['num']
    offset = param['pn'] * size + param['offset']
    st = param['st']
    if count:
        size = 0
        offset = 0

    if st == DEF_SORT_TYPE_SCORE:
        fragment_dsl = deepcopy(base_dsl)
        fragment_dsl["size"] = size
        fragment_dsl["from"] = offset

        token_list_t = sorted(sum_token_score_w, key=lambda token: sum_token_score_w[token], reverse=True)
        should_has_child_list = list()
        must_has_child_list = list()
        must_not_has_child_list = list()

        for item_token in token_list_t:
            token_query_temp = list()
            for item_type in get_cfg("knowledge_type_weight", param):

                dsl_node = deepcopy(should_dsl)
                if item_token == param["query"]:
                    # and 增强 query本身,保证完全匹配
                    dsl_node["multi_match"]["boost"] = k_scala * sum(sum_token_score_w.values()) * \
                                                       get_cfg("knowledge_type_weight", param)[item_type]
                    dsl_node["multi_match"]["analyzer"] = "ik_max_word"
                    dsl_node["multi_match"]["operator"] = "and"

                else:
                    dsl_node["multi_match"]["boost"] = sum_token_score_w[item_token] * get_cfg("knowledge_type_weight", param)[item_type]

                dsl_node["multi_match"]["fields"] = get_cfg("knowledge_type_fields", param)[item_type]
                dsl_node["multi_match"]["query"] = item_token
                token_query_temp.append(dsl_node)

            inner_bool_node = deepcopy(knowledge_dsl)
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

                fragment_dsl["query"]["bool"][item_key].extend([outer2_bool_node])

    elif st == DEF_SORT_TYPE_STATUS:

        # 自然排序召回知识点
        fragment_dsl = deepcopy(get_fragment_not_score_dsl)
        fragment_dsl["size"] = size
        fragment_dsl["from"] = offset

    if st == DEF_SORT_TYPE_SCORE:
        fragment_dsl["min_score"] = 1.1

    # print json.dumps(fragment_dsl)
    return fragment_dsl


def create_microdegree_dsl(sum_token_score_w, param, count=False):
    size = param['num']
    offset = param['pn'] * size + param['offset']
    st = param['st']
    if count:
        size = 0
        offset = 0
    assert st == DEF_SORT_TYPE_SCORE, "microdegree:请检查st参数"

    if st == DEF_SORT_TYPE_SCORE:
        microdegree_dsl = deepcopy(base_dsl)
        microdegree_dsl["size"] = size
        microdegree_dsl["from"] = offset

        token_list_t = sorted(sum_token_score_w, key=lambda token: sum_token_score_w[token], reverse=True)
        should_has_child_list = list()
        must_has_child_list = list()
        must_not_has_child_list = list()

        for item_token in token_list_t:
            token_query_temp = list()
            for item_type in get_cfg("microdegree_type_weight", param):
                for item_field_weight in get_cfg("microdegree_type_fields", param)[item_type]:  # 该type的所有字段
                    # item_field 是  k-v对
                    this_field = item_field_weight.keys()[0]
                    nested_sun_field_list = item_field_weight[this_field]

                    if isinstance(nested_sun_field_list, list):
                        # 这个item_field对应了一个nested类型
                        for nested_field_kv in nested_sun_field_list:
                            # 分别处理nested中的每个字段
                            if isinstance(nested_field_kv, dict):  # 过滤该nested一级字段本身的权重值
                                nested_dsl_node = deepcopy(nested_base)
                                nested_dsl_node["nested"]["path"] = this_field
                                this_nested_field = nested_field_kv.keys()[0]
                                this_nested_weight = nested_field_kv.values()[0]
                                dsl_node = deepcopy(should_dsl)
                                if item_token == param["query"]:
                                    # and 增强 query本身,保证完全匹配
                                    dsl_node["multi_match"]["boost"] = k_scala * sum(sum_token_score_w.values()) * \
                                        get_cfg("microdegree_type_weight", param)[item_type] * nested_sun_field_list[0] * this_nested_weight
                                    dsl_node["multi_match"]["analyzer"] = "ik_max_word"
                                    dsl_node["multi_match"]["operator"] = "and"
                                else:

                                    dsl_node["multi_match"]["boost"] = sum_token_score_w[item_token] * \
                                        get_cfg("microdegree_type_weight", param)[item_type] * nested_sun_field_list[0] * this_nested_weight
                                dsl_node["multi_match"]["query"] = item_token
                                dsl_node["multi_match"]["fields"] = "{a}.{b}".format(
                                    a=this_field, b=this_nested_field)
                                nested_dsl_node["nested"]["query"]["bool"]["should"].append(dsl_node)
                                nested_dsl_node["nested"]["query"]["bool"]["minimum_should_match"] = 1
                                token_query_temp.append(nested_dsl_node)

                    else:  # 这是一个普通的字段类型
                        dsl_node = deepcopy(should_dsl)
                        if item_token == param["query"]:
                            # and 增强 query本身,保证完全匹配
                            dsl_node["multi_match"]["boost"] = k_scala * sum(sum_token_score_w.values()) * \
                                get_cfg("microdegree_type_weight", param)[item_type] * nested_sun_field_list
                            dsl_node["multi_match"]["analyzer"] = "ik_max_word"
                            dsl_node["multi_match"]["operator"] = "and"

                        else:
                            dsl_node["multi_match"]["boost"] = sum_token_score_w[item_token] * \
                                get_cfg("microdegree_type_weight", param)[item_type] * nested_sun_field_list

                        dsl_node["multi_match"]["fields"] = this_field
                        dsl_node["multi_match"]["query"] = item_token
                        token_query_temp.append(dsl_node)

            inner_bool_node = deepcopy(base_bool_dsl)
            inner_bool_node["bool"]["minimum_should_match"] = 1
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

                microdegree_dsl["query"]["bool"][item_key].extend([outer2_bool_node])

    elif st == DEF_SORT_TYPE_STATUS:

        # 自然排序召回知识点
        microdegree_dsl = {}

    if st == DEF_SORT_TYPE_SCORE:
        microdegree_dsl["min_score"] = 1.1

    # print json.dumps(microdegree_dsl)
    return microdegree_dsl


def create_live_dsl(sum_token_score_w, param, count=False):
    size = param['num']
    offset = param['pn'] * size + param['offset']
    st = param['st']
    assert st == DEF_SORT_TYPE_SCORE, "live:请检查st参数"
    if count:
        size = 0
        offset = 0

    if st == DEF_SORT_TYPE_SCORE:
        live_dsl = deepcopy(base_dsl)
        live_dsl["size"] = size
        live_dsl["from"] = offset

        token_list_t = sorted(sum_token_score_w, key=lambda token: sum_token_score_w[token], reverse=True)
        should_has_child_list = list()
        must_has_child_list = list()
        must_not_has_child_list = list()

        for item_token in token_list_t:
            token_query_temp = list()
            for item_type in get_cfg("live_type_weight", param):
                dsl_node = deepcopy(should_dsl)
                if item_token == param["query"]:
                    # and 增强 query本身,保证完全匹配
                    dsl_node["multi_match"]["boost"] = k_scala * sum(sum_token_score_w.values()) * \
                                                       get_cfg("live_type_weight", param)[item_type]
                    dsl_node["multi_match"]["analyzer"] = "ik_max_word"
                    dsl_node["multi_match"]["operator"] = "and"

                else:
                    dsl_node["multi_match"]["boost"] = sum_token_score_w[item_token] * get_cfg("live_type_weight", param)[item_type]

                dsl_node["multi_match"]["fields"] = get_cfg("live_type_fields", param)[item_type]
                dsl_node["multi_match"]["query"] = item_token
                token_query_temp.append(dsl_node)

            inner_bool_node = deepcopy(live_dsl_base_inner)
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

                live_dsl["query"]["bool"][item_key].extend([outer2_bool_node])


    elif st == DEF_SORT_TYPE_STATUS:

        # 自然排序召回知识点
        live_dsl = {}

    if st == DEF_SORT_TYPE_SCORE:
        live_dsl["min_score"] = 1.1

    # print json.dumps(live_dsl)
    return live_dsl

create_dsl_dict = {
    DEF_QUERY_TYPE_MIX_CK: create_mic_ck_dsl,
    DEF_QUERY_TYPE_COURSE: create_course_dsl,
    DEF_QUERY_TYPE_KNOWLE: create_knowle_dsl,
    DEF_QUERY_TYPE_ENGINEER: create_engineer_dsl,
    DEF_QUERY_TYPE_LIVE: create_live_dsl,
    DEF_QUERY_TYPE_MICRODEGREE: create_microdegree_dsl
}


def get_main_info(item_class_dict, query, es, param):
    dsl_all_in_body = ""
    course_for_knowledge = []
    microdegree_items = []
    for item_id in item_class_dict:

        # 如果 该item  是  课程项(工程硕士本质上也是一门课,他和这个共用)
        if item_class_dict[item_id]["item_type"] == "course":
            for item_type in get_cfg("course_type_fields", param):
                base_dsl_head = {"index": IndexName, "type": item_type}
                field_list = get_cfg("course_type_fields", param)[item_type]
                search_sub_course_dsl = {
                    "size": 1,
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "term": {
                                        "course_id": {
                                            "value": item_id
                                        }
                                    }
                                }
                            ],
                            "must": [
                                {
                                    "multi_match": {
                                        "query": query,
                                        "fields": field_list,
                                        "analyzer": "ik_max_word"
                                    }
                                }
                            ]
                        }
                    }
                }
                # print json.dumps(search_course_dsl)
                dsl_all_in_body += json.dumps(base_dsl_head) + '\n' + json.dumps(search_sub_course_dsl) + '\n'

        # 如果 该item  是  知识点
        elif item_class_dict[item_id]["item_type"] == "fragment":
            course_for_knowledge.append(item_class_dict[item_id]["course_id"])

        # if 是其他的2种类型的资源
        elif item_class_dict[item_id]["item_type"] == "live":
            # 因为live的主信息中已经包含了全部需要的信息,所以不用再召回子信息了
            pass

        elif item_class_dict[item_id]["item_type"] == "microdegree":
            temp_id = item_class_dict[item_id]["microdegree_key"]
            microdegree_items.append(temp_id)

        else:
            raise Exception, u"程序出现 异常"

    if list(set(course_for_knowledge)):
        # 如果需要召回知识点,集中使用dsl压缩
        base_dsl_head = {"index": IndexName, "type": "course"}
        search_course_dsl = {
            "size": len(course_for_knowledge),
            "query": {
                "terms": {
                    "course_id": list(set(course_for_knowledge))
                }
            }
        }
        dsl_all_in_body += json.dumps(base_dsl_head) + '\n' + json.dumps(search_course_dsl) + '\n'

    # print list(set(microdegree_items)), "现在有微学位的课程"
    if list(set(microdegree_items)):

        # 如果需要召回 微学位
        base_dsl_head = {"index": IndexName, "type": "microdegree"}
        search_course_dsl = {
            "_source": ["microdegree_key"],
            "query": {
                "bool": {
                    "filter": [
                        {
                            "terms": {
                                "microdegree_key": list(set(microdegree_items))
                            }
                        }
                    ],
                    "should": [
                        {
                            "nested": {
                                "path": "course_info_list",
                                "query": {
                                    "multi_match": {
                                        "query": query,
                                        "analyzer": "ik_max_word",
                                        "fields": [
                                            "course_info_list.about",
                                            "course_info_list.name",
                                            "course_info_list.comment_org",
                                            "course_info_list.staffs_searchable",
                                            "course_info_list.subtitle"
                                        ]
                                    }
                                },
                                "inner_hits": {
                                    "highlight": {
                                        "fields": {
                                            "course_info_list.about": {},
                                            "course_info_list.name": {},
                                            "course_info_list.comment_org": {},
                                            "course_info_list.staffs_searchable": {},
                                            "course_info_list.subtitle": {}
                                        }
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
        dsl_all_in_body += json.dumps(base_dsl_head) + '\n' + json.dumps(search_course_dsl) + '\n'
    # print dsl_all_in_body, "####"
    if dsl_all_in_body:
        result = es.msearch(body=dsl_all_in_body)
    else:
        result = {
            "responses": []
        }

    # 收集各种子类型的组件
    sub_type_list = []
    for i in result["responses"]:
        if i["hits"]["hits"]:
            sub_type_list.extend(i["hits"]["hits"])

    # print "*****" * 10
    # print json.dumps(sub_type_list)
    # print "*****" * 10

    for item_sub_doc in sub_type_list:
        item_type = item_sub_doc["_type"]

        # 如果是课程所需要的组件:
        if item_type in get_cfg("course_type_fields", param):
            course_id = item_sub_doc["_source"]["course_id"]
            item_class_dict[course_id]["sub_" + item_type] = item_sub_doc["_source"]

        # 如果是知识点所需要的组件
        if item_type in ["course"]:
            # 把这个course info 插入 对应的knowledge中
            course_id = item_sub_doc["_source"]["course_id"]
            for item_father in item_class_dict.values():
                if item_father["item_type"] == "fragment":
                    if course_id == item_father["course_id"]:
                        item_father["sub_course"] = item_sub_doc["_source"]

        # 如果是微学位所需要的组件
        if item_type in ["microdegree"]:
            microdegree_id_t = "{a}_{b}".format(a="microdegree", b=item_sub_doc["_source"]["microdegree_key"])
            course_list_t = item_sub_doc["inner_hits"]["course_info_list"]["hits"]["hits"]
            first_match_course_name = None
            for item in course_list_t:
                first_match_course_name = item["_source"]["name"]
                break
            item_class_dict[microdegree_id_t]["match_course"] = first_match_course_name


def create_dsl(sum_token_score_w, param, sum_token_analyzer):
    # sum_token_analyzer 用于指定特别token的分词器
    qt = param["qt"]
    res_dsl = []
    index_res = []

    if param["home"] == 0:
        # 工程硕士检索
        # 各种单项的资源检索(qt=1是单资源检索)
        res = create_dsl_dict[qt](sum_token_score_w, param, False)
        res_dsl.append(res)
        #  dsl列表中的第一个为展示栏, 检索qt
        index_res.append((1, param["qt"]))

    else:
        # 网页端的输入框 主检索页面
        dsl_list = [  # 全部
                      # 课程
                      # 知识点
                      # 直播
                    ]  # 微学位
        index_temp = 1
        index_end = [0, 0]

        for key, dsl_fun in create_dsl_dict.items():
            if key == DEF_QUERY_TYPE_ENGINEER:  # 跳过工程硕士
                continue

            if param["qt"] == key:
                index_end[0] = index_temp
                index_end[1] = key

            res_t = dsl_fun(sum_token_score_w, param, param["qt"] != key)
            dsl_list.append(res_t)
            index_temp += 1

        res_dsl = dsl_list
        index_res.append((index_end[0], index_end[1]))
    return res_dsl, index_res


if __name__ == "__main__":
    pass

