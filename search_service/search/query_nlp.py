# -*- coding: utf-8 -*-


import sys
import jieba
import jieba.posseg
import jieba.analyse
import json
import time
import requests
import math
from datetime import datetime, timedelta, date
import os
from copy import deepcopy
reload(sys)
sys.setdefaultencoding('utf-8')
SEARCH_SERVICE = os.path.dirname(os.path.split(os.path.realpath(__file__))[0])
sys.path.append(SEARCH_SERVICE)
from tool.settings import single_word, ban_single_token
from tool.models import noun_set, manual_core_word_set
from tool.base_tool import print_debug
import logging.config
search_logger = logging.getLogger('search.'+__name__)
from tool.settings import conf, DEBUG_FLAG
from tool.settings import IndexName
from tool.models import es_instance
word_flag = {
    "n": 2,
    "ns": 2,
    "nt": 2,
    "nz": 2,
    "nl": 2,
    "ng": 2,
    "m": 2,
    "t": 2,
    "x": 0.5,
    "v": 1.2,
    "c": 1
}


def get_tokens(query, es):
    token_list = []
    if query:
        res = es.indices.analyze(index=IndexName, body=query, analyzer='ik_max_word')
        for term in res.get("tokens", []):
            token_list.append(term["token"])
    return token_list


def extarct_keyword(query=u"java工程工程师"):
    key_list = jieba.analyse.extract_tags(sentence=query, topK=20, withWeight=True)
    keyword_score_w = dict()
    for item_pair in key_list:
        if item_pair[1] <= 5:
            score = 2**item_pair[1]
        if 5 < item_pair[1]:
            score = 32 + 2*(item_pair[1]-5)
        keyword_score_w[item_pair[0]] = score
    return keyword_score_w


def extract_keyword_by_relu(sum_token_score_w, query):
    for item_token in sum_token_score_w:
        if u"学" in item_token:
            if item_token not in [u"大学", u"中学", u"小学"]:
                sum_token_score_w[item_token] *= 1.1
            else:
                sum_token_score_w[item_token] *= 0.8

    def get_token_index_score(token, query):
        split_tokens = query.split(token)
        # "只对query的2切分处理"
        if len(split_tokens) == 2:
            return (len(split_tokens[0]) + 0.5 * len(token)) * 0.1
        else:
            return 0

    # token score 位置 提升
    if len(set(sum_token_score_w.values())) < len(sum_token_score_w):

        score_dict = dict()
        for token in sum_token_score_w:
            if sum_token_score_w[token] in score_dict:
                score_dict[sum_token_score_w[token]].append(token)
            else:
                score_dict[sum_token_score_w[token]] = [token]
        for same_tokens in score_dict.values():
            if len(same_tokens) > 1:
                for item_token in same_tokens:
                    sum_token_score_w[item_token] += get_token_index_score(item_token, query)
    # 使用 手动 核心词
    for item_token in sum_token_score_w:
        if item_token in manual_core_word_set:
            sum_token_score_w[item_token] = sum(sum_token_score_w.values())
    return sum_token_score_w


def analyse_word_flag(query=u"java工程工程师"):
    flag_w = dict()
    words = jieba.posseg.cut(query)
    for word, flag in words:
        # print word, flag
        flag_w[word] = word_flag.get(flag, 1)
    return flag_w


def filter_token(sum_token_score_w):
    if len(sum_token_score_w) >= 2:
        order_tokens = sorted(sum_token_score_w.keys(), key=lambda token: sum_token_score_w[token], reverse=True)
        # allow_set = set()
        ban_set = set()
        for token in order_tokens:
            if len(token) == 1 and token in ban_single_token:
                ban_set.add(token)
        for ii in ban_set:
            del sum_token_score_w[ii]

        order_tokens = sorted(sum_token_score_w.keys(), key=lambda token: sum_token_score_w[token], reverse=True)
        for item_token in order_tokens:
            try:
                int(item_token)
                del sum_token_score_w[item_token]
            except Exception:
                pass

        # 截断只取得分靠前的 6个token
        order_tokens = sorted(sum_token_score_w.keys(), key=lambda token: sum_token_score_w[token], reverse=True)
        ban_over_7 = order_tokens[6:]
        for item_token in ban_over_7:
            del sum_token_score_w[item_token]

    return sum_token_score_w


def analyse_query(highlight_tokens, es, query=u"java工程工程师", ):
    # highlight_tokens  收集要高亮的tokens
    # 获取query的分词和权值向量
    # NLP(获取query中的词权重)
    # 获取query的权值向量
    # 获取参数特征的加权向量
    sum_token_score_w = dict()
    sum_token_analyzer = dict()

    # 此处要做意图识别

    # search_logger.info(u"一共有专有名词{n}".format(n=len(noun_set)))
    if query in noun_set:  # 专有 人名名词 ik_max_word
        sum_token_score_w[query] = 1
        search_logger.info(u"这是专有名词:{word}".format(word=query))
        highlight_tokens.add(query)
        return sum_token_score_w, sum_token_analyzer
    if u"/" in query:  # simple
        sum_token_score_w[query] = 1
        sum_token_analyzer[query] = "keyword"

        return sum_token_score_w, sum_token_analyzer

    token_list = get_tokens(query, es)
    if token_list:

        query_token_frequency_w = dict()  # query分词中重复出现加权重
        single_word_w = dict()  # 单字减权重
        highlight_tokens.add(query)  # 添加query本身
        for item_token in token_list:
            highlight_tokens.add(item_token)
            if item_token in query_token_frequency_w:
                query_token_frequency_w[item_token] += 1
            else:
                query_token_frequency_w[item_token] = 1
            if len(item_token) == 1:
                single_word_w[item_token] = 1
            else:
                single_word_w[item_token] = 3*len(item_token)

        keyword_score_w = extarct_keyword(query)  # 提权关键字权重
        flag_w = analyse_word_flag(query)  # 词性分析权重

        for token in token_list:
            # search_logger.info(token)
            print_debug([query_token_frequency_w.get(token, 1), single_word_w.get(token, 1),
                        keyword_score_w.get(token, 1), flag_w.get(token, 1)])
            sum_token_score_w[token] = (query_token_frequency_w.get(token, 1)
                                     * single_word_w.get(token, 1)
                                     * keyword_score_w.get(token, 1)
                                     * flag_w.get(token, 1))
            # print sum_token_score_w[token],"----"
        if DEBUG_FLAG:
            print_debug("----" * 5)
            for i in sum_token_score_w:
                search_logger.info("{i}=={s}".format(i=i, s=sum_token_score_w[i]))
            print_debug("----" * 5)

        sum_token_score_w = filter_token(sum_token_score_w)
        sum_token_score_w = extract_keyword_by_relu(sum_token_score_w, query)

        if DEBUG_FLAG:
            for i in sum_token_score_w:
                search_logger.info("{i}=={s}".format(i=i, s=sum_token_score_w[i]))
            print_debug("----" * 5)

        more_tokens = sorted(sum_token_score_w, key=lambda token: sum_token_score_w[token], reverse=True)[10:]
        for item_token in more_tokens:
            del sum_token_score_w[item_token]

    return sum_token_score_w, sum_token_analyzer


def get_intention_from_query(query):
    # """意图识别-专有不可分名词"""
    pass


def analyse_query_all(query, param, highlight_tokens, es):
    persent = param["persent"]   # 0-10 最松--最严
    if query:
        # 如果有query
        sum_token_score_w, sum_token_analyzer = analyse_query(highlight_tokens, es, query)
        # print sum_token_score_w
        if persent >= 10:
            sum_token_score_w = dict()
            sum_token_score_w[query] = 1.0
        #
        # 在这里应该形成 must  should  DSL组件
        must_tokens = list()
        should_tokens = list()
        must_not_tokens = list()
        token_list_by_score = sorted(sum_token_score_w, key=lambda token: sum_token_score_w[token], reverse=True)
        param["should_p"] = 0
        param["must_p"] = 0
        param["must_not_p"] = 0

        if "+" in query and "++" not in query:
            should_tokens.extend(token_list_by_score)
            param["should_p"] = 1
        elif len(token_list_by_score) == 0:
            pass
        elif len(token_list_by_score) == 1:
            should_tokens.extend(token_list_by_score)
            param["should_p"] = 1
        elif len(token_list_by_score) == 2:  # 花卉学
            max_token = token_list_by_score[0]
            min_token = token_list_by_score[1]
            # 双子词 分成 两个 字
            if len(max_token) == 1 and len(min_token) == 1:
                should_tokens.extend([min_token, max_token])
                param["should_p"] = 1
            # 切分关系
            elif min_token not in max_token and max_token not in min_token:
                must_tokens.append(max_token)
                should_tokens.append(min_token)
                param["should_p"] = 0
            else:  # 包含关系: 全词  + 部分
                if len(min_token) != 1:
                    must_tokens.append(min_token)
                    should_tokens.append(max_token)
                    param["should_p"] = 0
                else:  # "部分"是单字
                    must_tokens.append(max_token)
                    should_tokens.append(min_token)
                    param["should_p"] = 0

        elif len(token_list_by_score) == 3:

            token_l = token_list_by_score

            if token_l[0] == param["query"] and token_l[1] in token_l[0] and token_l[2] in token_l[0]:
                must_tokens.extend([token_l[1]])
                should_tokens.extend([token_l[0], token_l[2]])
                param["should_p"] = 0
            elif token_l[1] not in token_l[0] and token_l[2] not in token_l[0] and token_l[2] not in token_l[1]:
                must_tokens.extend([token_l[0]])
                should_tokens.extend([token_l[1], token_l[2]])
                param["should_p"] = 0
            else:
                core_token = token_l[2] if token_l[2] in token_l[0] else token_l[0]
                must_tokens.extend([core_token])
                token_l.remove(core_token)
                should_tokens.extend(token_l)
                param["should_p"] = 0
        else:
            token_l = token_list_by_score
            if token_l[0] != param["query"]:
                core_token = token_l[0]
            else:
                core_token = token_l[1]
            must_tokens.append(core_token)
            token_l.remove(core_token)
            should_tokens.extend(token_l)
            # 3 -- 0
            # 4 -- 1
            # 5 -- 2
            param["should_p"] = len(should_tokens) / 4 + 1 if len(should_tokens) > 4 else 0

        param["must_tokens"] = must_tokens
        if len(param["must_tokens"]) >= 1 and param["must_p"] == 0:  # 如果忘记设置
            param["must_p"] = 1

        param["should_tokens"] = should_tokens
        param["must_not_tokens"] = must_not_tokens
        if len(param["must_not_tokens"]) >= 1 and param["must_not_p"] == 0:  # 如果忘记设置
            param["must_not_p"] = 1

        if query not in param["should_tokens"]:
            param["should_tokens"].append(query)
        if query not in sum_token_score_w:
            sum_token_score_w[query] = 1
    else:
        sum_token_analyzer = dict()
        sum_token_score_w = dict()
        param["must_tokens"] = list()
        param["should_tokens"] = list()
        param["must_not_tokens"] = list()
        param["should_p"] = 0
        param["must_p"] = 0
        param["must_not_p"] = 0
    return sum_token_score_w, sum_token_analyzer


if __name__ == "__main__":

    param = {
        "query": u"投资学 微学位",  # 投资学 微学位
        "persent": 10
    }
    persent = param["persent"]
    es = es_instance()
    highlight_tokens = set()
    sum_token_score_w, sum_token_analyzer = analyse_query_all(param["query"], param, highlight_tokens, es)
    if True:
        print "query={a}".format(a=param["query"])
        print '----must----'
        for i in param["must_tokens"]:
            print i
        print '----should----'
        for j in param["should_tokens"]:
            print j
        print '----must_not----'
        for i in param["must_not_tokens"]:
            print i
    print "控制比例是:should_p={a} || must_p={b} || must_not_p={c} ||".format(
        a=param["should_p"], b=param["must_p"], c=param["must_not_p"])

