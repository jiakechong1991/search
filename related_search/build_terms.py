#-*- coding: utf-8 -*-
from indexing import Searchable
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import re
import jieba
import time
import json
import urllib2
from related_model import *
from models import mysql_connection, es_instance, md5
from elasticsearch import Elasticsearch, helpers
import datetime

BLACK_LIST = ["angelina", "II地的", "II哦我"]


def filter_dup_space(data):
    data = re.sub(r'\s+', ' ', data)
    return data

# 自行初始化的停用词字典
stopwords = set()


def init_stopwords(filename):
    f = open(filename)
    for word in f:
        stopwords.add(word.strip().decode("utf-8"))
    f.close()
'''
取出章节序号及无意义的词
'''


def remove_ordinal_num(data):
    characters = [
            # 章节前缀
            u'\(?[第]?[一二三四五六七八九十零1234567890]+[\.]?[一二三四五六七八九十零1234567890]*[章节春秋]?\)?',
            # 作业考试
            u'作业|习题',
            u'期[中末]考试']
    for character in characters:
        data = re.sub(character, '', data)
    return data.strip()


def conv2lower(data):
    """全部转换为小写"""
    data = data.lower()
    return data


def strQ2B(data):
    """全角转半角"""
    rstring = ""
    for uchar in data:
        inside_code = ord(uchar)
        # 全角空格直接转换
        if inside_code == 12288:
            inside_code = 32
        # 全角字符（除空格）根据关系转化
        elif (inside_code >= 65281) and (inside_code <= 65374):
            inside_code -= 65248

        rstring += unichr(inside_code)
    data = rstring
    return data


def remove_tag(text):
    string = re.sub("[a-zA-Z0-9_]+|[\s+\.\!\/_,$%^*()+\"\']+|[，。？！、￥（）【】]+".decode("utf-8"), "", text)
    return string


def add_node(tree, text, text_type):
    words = list(jieba.cut(text))
    cut_words = []
    # 移除文本的停用词
    for word in words:
        if word in stopwords or word in BLACK_LIST:
            continue
        else:
            cut_words.append(word)
    # 添加 term级别分词
    for word in cut_words:
        if len(word) > 1:
            print word, 'structure----term'
            tree.add_node(word.encode("utf-8"), text_type)
    # 添加分词的 2-gram级别分词
    for i in range(len(cut_words) - 1):
        if len(cut_words[i]) > 1 and len(cut_words[i+1]) > 1:
            temp_term = cut_words[i] + " " + cut_words[i + 1]
            tree.add_node(temp_term.encode("utf-8"), text_type)


def cal_term(text):
    words = list(jieba.cut(text))
    cut_words = []
    # remove stopwords
    for word in words:
        if word in stopwords or word in BLACK_LIST:
            continue
        else:
            cut_words.append(word)
    result = 0
    for word in cut_words:
        if len(word) > 1:
            result += 1
    for i in range(len(cut_words) - 1):
        if len(cut_words[i]) > 1 and len(cut_words[i+1]) > 1:
            result += 1
    return result


def build_terms():
    conn = mysql_connection()
    cursor = conn.cursor()
    # 拿到所有的课程ID
    cursor.execute("select course_id from course_meta_course where owner = 'xuetangX' and status > -1")
    results = cursor.fetchall()
    init_stopwords("stopwords.txt")
    s = Searchable()
    # 构建一颗空树
    rel_tree = Tree()
    for result in results[:]:
        course_id = result[0]
        # print course_id
        # 获得这门课的搜索信息
        course = s.get_course(course_id)
        # 对词做一些停用词过滤
        course_name = process_word(course.course_name)

        tree = Tree()
        # 添加"课程名称"  类型节点(这个类型是为了防止   这个词对应的节点的类型被冲掉)
        print course_name,"course_name"
        tree.add_node(course_name.encode('utf-8'), COURSE_NAME)
        for child in course.children:
            text = child.searchable_text.decode("utf-8")
            text = process_word(text)
            # 添加"课程章节"  类型节点

            add_node(tree, text, STRUCTURE)
        for st in course.staff:
            # 添加"课程教师" 类型节点
            st_temp = st["searchable_text"]["name"].encode('utf-8')
            print st_temp, 'staff'
            tree.add_node(st_temp, COURSE_STAFF)
        # 初始化  本棵树 内的 节点相关性,两两单词之间相关性为0
        tree.traverse_relation()
        # 树合并
        rel_tree.combine(tree)
    return rel_tree    


def process_word(word):
    return remove_tag(remove_ordinal_num(strQ2B(word)))


def load_log(filename='search.log_back'):
    try:
        f = open(filename)
        result = []
        count = 0
        # load and filter
        for line in f:
            [uid, session, time, cid, query, org] = line.strip().split("|||")
            query = urllib2.unquote(query).strip()
            print query, 'query'
            words = list(jieba.cut(query))
            # 将短query添加进分析列表中
            if time > "2015-08-18" and len(words) < 10 and len(query) > 3 and not query in BLACK_LIST and not query.startswith("-"):
                result.append([session, time, query])
                count += 1
        def sort_func(x, y):
            if x[0] > y[0]:
                return 1
            elif x[0] < y[0]:
                return -1
            else:
                if x[1] > y[1]:
                    return 1
                else:
                    return -1
        # 记录按照 先session再time的方式排序
        result.sort(cmp=sort_func)
    except IOError, e:
        result = e
    return result


def get_time(t1, t2):
    d1 = datetime.datetime.strptime(t1, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.datetime.strptime(t2, "%Y-%m-%d %H:%M:%S")
    s = (d2 - d1).seconds
    if s == 0:
        s = 1
    return s


def add_log(tree, log_result):
    last_session = ""
    last_time = ""
    last_query = ""
    for session, time, query in log_result:
        #print session, time, query
        query = query.replace('+', ' ').strip()
        # 如果这个记录和上个记录是同一个session
        if session == last_session:
            # 计算这两次的时间间隔
            t = get_time(last_time, time)
            if query == last_query:
                # 前后两个query一样,但是时间很短
                if t <= 4:
                    # 丢弃
                    continue
                # 过了好几秒再重新搜索相同的query,这两次已经没有关系了
                else:
                    last_session = session
                    last_time = time
                    last_query = query
                    tree.add_log_node(query)
                    # 系统错误
            else:
                if t < 90:  # 短时间内的不同query,有相关性
                    tree.add_log_node(query)
                    # 在relation网络中添加这两个点的相关性
                    tree.add_relation(tree.get_node(last_query), tree.get_node(query), 10/float(t))
                    tree.add_relation(tree.get_node(query), tree.get_node(last_query), 2/float(t))
                else:  # 过了好几秒再重新搜索相同的query,这两次已经没有关系了
                    last_session = session
                    last_time = time
                    last_query = query
                    tree.add_log_node(query)

        else:  # 初始化
            last_session = session
            last_time = time
            last_query = query
            tree.add_log_node(query)


def importes(tree):
    es = es_instance()
    chunks = []
    for (word, node) in tree.nodes.items():
        try:
            source = dict()
            source["word"] = word.encode("utf-8")
            source["result"] = node.children
            source["_index"] = "related_search"
            source["_type"] = "related_search"
            source["_id"] = md5(source["word"].encode("utf-8"))
            chunks.append(source)  # es.index_op(source, id = md5(word.encode("utf-8")))
        except Exception, e:
            print e
    print len(chunks), '-----'
    helpers.bulk(es, chunks)


if __name__ == '__main__':
    t1 = time.time()
    tree = build_terms()
    # 本树内的 relation网,两两单词求初始得分
    tree.build_relation()

    result = load_log()
    # 添加 搜索日志 中的 query 相关性
    add_log(tree, result)
    # 刷新整个树的节点关心
    print 'add_log ok'
    tree.get_related_result()
    # 导入ES
    importes(tree)
    t2 = time.time()
    print t2 - t1

