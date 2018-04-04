# -*- coding=utf-8 -*-

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.autoreload
import tornado.web
from tornado.options import define, options
from search.search_core import SearchCore
from search.micro_app_search import SearchAppCore
from old.update_index import *
import urllib
import json
import requests
import sys
import os
from tool.settings import web_dict_file, es_assist_word_index, es_core_word_type, es_noun_word_type
from tool.settings import get_data_conf_setting
import threading
import time
from elasticsearch import helpers
from old.models import upsert_course, get_course_info_specify_fields
from tool.models import es_instance
import traceback
from tool.models import noun_set, manual_core_word_set
from logging import config, getLogger
from tool.settings import DEF_SORT_TYPE_STATUS, DEF_SORT_TYPE_SCORE, DEF_QUERY_TYPE_MIX_CK
reload(sys)
sys.setdefaultencoding('utf-8')
DEF_CUR_FILE_PATH = os.path.split(os.path.realpath(__file__))[0]
sys.path.append(DEF_CUR_FILE_PATH)

config.fileConfig('{0}/tool/logging.conf'.format(DEF_CUR_FILE_PATH))
search_logger = getLogger('search')
update_logger = getLogger('update')
define("port", default=9999, help="run on the given port", type=int)


class MyThread(threading.Thread):
    def __init__(self, func, args, name=''):
        threading.Thread.__init__(self)
        self.name = name
        self.func = func
        self.args = args
        self.res = None

    def getResult(self):
        return self.res

    def run(self):
        search_logger.info("{name}线程starting".format(name=self.name))
        self.res = apply(self.func, self.args)
        print self.res
        search_logger.info("{name}线程finished".format(name=self.name))


def get_all_tap_es(doc_type):
    def get_scroll_search_res(query_dsl, scroll='5m', index=es_assist_word_index,
                              doc_type=doc_type, timeout="1m"):
        es_result = helpers.scan(
            client=es_instance(),
            query=query_dsl,
            scroll=scroll,
            index=index,
            doc_type=doc_type,
            timeout=timeout
        )
        return es_result

    doc_list = list()
    query_dsl = {
        "query": {
            "match_all": {}
        }
    }
    es_result = get_scroll_search_res(query_dsl)
    for item in es_result:
        doc_list.append(item['_source'])

    word_dict = list()
    for item_doc in doc_list:
        word_dict.append(item_doc["word"])

    return word_dict


def read_noun_dict():
    temp_noun_set = set()
    try:
        res = requests.get(web_dict_file, timeout=1)
        for word in res.text.split(u"\n"):
            temp_noun_set.add(word)

        search_logger.info(u"mysql专有名词共: {num}".format(num=len(temp_noun_set)))
        es_word_list = get_all_tap_es(es_noun_word_type)
        search_logger.info(u"es专有名词共: {num}".format(num=len(es_word_list)))
        for item in es_word_list:
            temp_noun_set.add(item)
        # 只有web 接口真的更新成功,才会清除旧的web字典。
        noun_set.clear()
        for i in temp_noun_set:
            noun_set.add(i)
        search_logger.info(u"一共有专有名词{n}".format(n=len(noun_set)))

        es_word_list = get_all_tap_es(es_core_word_type)
        # 如果上面函数失败,就会崩溃退出,不会清除。
        manual_core_word_set.clear()
        for item in es_word_list:
            manual_core_word_set.add(item)
        search_logger.info(u"es手动核心词汇共: {num}".format(num=len(manual_core_word_set)))

        return True
    except Exception, e:
        search_logger.info(e)
        return False


def read_noun_dict_main():  # 添加你的函数
    # 每天三点执行
    ISOTIMEFORMAT = '%H'
    has_done = False
    flag = read_noun_dict()
    search_logger.info(u"首次noun更新{s}".format(s = u"成功" if flag else u"失败"))
    while True:
        try:
            now_hour = int(time.strftime(ISOTIMEFORMAT, time.localtime()))
            if now_hour == 3:
                if not has_done:
                    time.sleep(1 * 60)
                    flag = read_noun_dict()
                    search_logger.info(u"noun更新{s}".format(s = u"成功" if flag else u"失败"))
                    if flag:
                        has_done = True
                else:
                    pass
                    time.sleep(10 * 60)
            else:
                time.sleep(10 * 60)
                has_done = False
        except Exception, e:
            search_logger.info(u"noun字典更新线程出现异常")
            search_logger.info(e)


funList = [read_noun_dict_main]  # 这是函数列表


def process():  # 启动线程列表，并且监控它
    thread_list = []
    for item_function in funList:
        t = MyThread(item_function, (), item_function.__name__)
        time.sleep(3)
        thread_list.append(t)
    for item_theard in thread_list:
        item_theard.start()
    search_logger.info("线程全部启动,进行监控")


class SearchHandler(tornado.web.RequestHandler):

    def to_int(self, num, default):
        if num == "":
            return default
        else:
            return int(num)

    def to_list(self, s):
        if s == "":
            return None
        else:
            return s.split(";")

    def to_str(self, s):
        if s == "":
            return None
        else:
            return str(s)

    def get(self):
        mes_string = "state:{state}|||query:{query}|||time:{time}"
        t_start = time.time()
        try:
            self.set_header("Content-Type", "application/json")
            # 解析各种传入参数
            query = self.get_argument("query", "")
            owner = self.get_argument("owner", "")
            cid = self.get_argument("cid", "")  # 主站的分类标签
            group = self.get_argument("group", "")
            serialized = self.get_argument("serialized", "")
            org = self.get_argument("org", "")  # 主战上的学校标签
            pn = self.get_argument("pn", "")
            offset = self.get_argument("offset", "")
            num = self.get_argument("num", "")
            qt = self.get_argument("qt", "")
            # 废弃st参数,以为st参数等价于query
            version = self.get_argument("version", "")
            home = self.get_argument("home", "")
            course_id = self.get_argument("course_id", "")
            hasTA = self.get_argument("hasTA", "")
            persent = self.get_argument("persent", "")
            process = self.get_argument("process", "")
            mode = self.get_argument("mode", "")  # 对应主站的权威标签,里面的"签字认证"和"认证开放"是传参一样的
            course_type = self.get_argument("course_type", "")
            is_paid_only = self.get_argument("is_paid_only", "")  # 是否是付费课程
            platform = self.get_argument("platform", "")
            expiration = self.get_argument("expiration", "")
            fields_v = self.get_argument("fields_v", "")

            param = {}

            if query == "":
                # 空query,就按照自然排名
                # 默认召回课程
                param['st'] = DEF_SORT_TYPE_STATUS
            else:
                # 不空query,就按照得分
                param['st'] = DEF_SORT_TYPE_SCORE
                query = urllib.unquote(query).decode('utf-8').replace('/', '').replace('\\', '')
                # 默认使用课程和知识点混合搜索
                # print type(query)
                # print
            param["persent"] = self.to_int(persent, 6)
            param['qt'] = self.to_int(qt, DEF_QUERY_TYPE_MIX_CK)
            param['home'] = self.to_int(home, 0)
            # version
            # 1: 旧版
            # 2: 新版
            param["version"] = self.to_int(version, 1)
            param["query"] = query
            owner = self.to_list(owner)
            if owner:
                # 对owner列表进行转小写
                owner = [item.lower() for item in owner]
            param['owner'] = owner
            cid = self.to_list(cid)
            if cid:
                cid = [int(item) for item in cid]
            group = self.to_str(group)
            param['cid'] = cid  # 课程的所属分类列表
            param['group'] = group  #
            param['serialized'] = self.to_int(serialized, None)
            if org == "others":
                param['org'] = org
            else:
                org = self.to_list(org)
                if org:
                    org = [item.lower() for item in org]
                param['org'] = org
            param['pn'] = self.to_int(pn, 0)
            param['offset'] = self.to_int(offset, 0)
            param['num'] = self.to_int(num, 10)
            # if param["num"] >= 20:
            #     param["num"] = 10  # 抓取的控制:大于20,返回10
            param['course_id'] = self.to_str(course_id)
            param['hasTA'] = self.to_str(hasTA)
            param['process'] = self.to_int(process, 0)  # 主站开课状态
            param["fields_v"] = self.to_int(fields_v, 0)  # 表示主站搜索哪些字段
            param['mode'] = self.to_list(mode)
            param['course_type'] = self.to_int(course_type, 0)  # 主站模式标签 eg.随访模式
            param['is_paid_only'] = self.to_int(is_paid_only, 0)
            param['platform'] = self.to_int(platform, 0)
            param['expiration'] = self.to_int(expiration, "")
            # get result
            # search_logger.info("-+-"*10)
            # search_logger.info(json.dumps(param))
            # if param['num'] > 100:
            #     raise "size too lage"
            result = SearchCore.get_result(param)
            search_logger.info(mes_string.format(state=0, query=query, time=time.time()-t_start))
            self.write(json.dumps(result))
        except Exception, e:
            traceback.print_exc()
            search_logger.info(mes_string.format(state=-1, query=query, time=time.time()-t_start))
            result = {'data': [], 'total': {'all': 0, 'course': 0, 'knowledge': 0}, 'error_code': -1, 'error_msg': str(e)}
            self.write(json.dumps(result))


class SearchAppHandler(tornado.web.RequestHandler):
    def to_int(self, num, default):
        if num == "":
            return default
        else:
            return int(num)

    def to_list(self, s):
        if s == "":
            return None
        else:
            return s.split(";")

    def get(self):
        mes_string = "state:{state}|||query:{query}|||time:{time}"
        t_start = time.time()
        try:
            self.set_header("Content-Type", "application/json")
            # 解析各种传入参数
            query = self.get_argument("query", "")
            cid = self.get_argument("cid", "")  # 主站的分类标签

            org = self.get_argument("org", "")  # 主战上的学校标签
            pn = self.get_argument("pn", "")
            offset = self.get_argument("offset", "")
            num = self.get_argument("num", "")
            qt = self.get_argument("qt", "")
            staff = self.get_argument("staff", "")  # 指定教师
            persent = self.get_argument("persent", "")

            visible = self.get_argument("visible", "")
            is_enroll = self.get_argument("is_enroll", "")
            is_apply = self.get_argument("is_apply", "")
            stage = self.get_argument("stage", "")
            first_category = self.get_argument("first_category", "")
            second_category = self.get_argument("second_category", "")
            order_time = self.get_argument("order_time", "")
            order_user = self.get_argument("order_user", "")
            time_statue = self.get_argument("time_status", "")

            param = {}
            if query == "":
                param['st'] = DEF_SORT_TYPE_STATUS
            else:
                param['st'] = DEF_SORT_TYPE_SCORE
                query = urllib.unquote(query).decode('utf-8').replace('\\', '')
            param["qt"] = int(qt)
            assert param['qt'], u"请使用qt参数"
            staff = self.to_list(staff)   # 用;隔开
            if staff:
                staff = [int(item) for item in cid]
            param["staff"] = staff

            param["query"] = query

            cid = self.to_list(cid)
            if cid:
                cid = [int(item) for item in cid]
            param['cid'] = cid  # 课程的所属分类列表
            param["visible"] = self.to_list(visible)
            param["is_enroll"] = self.to_list(is_enroll)
            param["is_apply"] = self.to_list(is_apply)
            param["stage"] = self.to_list(stage)
            param["first_category"] = self.to_list(first_category)
            param["second_category"] = self.to_list(second_category)
            param["order_time"] = self.to_list(order_time)
            param["order_user"] = self.to_list(order_user)
            if time_statue:
                param["time_status"] = int(time_statue)

            if org == "others":
                param['org'] = org
            else:
                org = self.to_list(org)
                if org:
                    org = [item.lower() for item in org]
                param['org'] = org

            param['pn'] = self.to_int(pn, 0)
            param['offset'] = self.to_int(offset, 0)
            param['num'] = self.to_int(num, 10)
            param["persent"] = self.to_int(persent, 6)

            if param["num"] >= 20:
                param["num"] = 10  # 抓取的控制:大于20,返回10
            result = SearchAppCore.get_result(param)
            search_logger.info(mes_string.format(state=0, query=query, time=time.time()-t_start))
            self.write(json.dumps(result))
        except Exception, e:
            traceback.print_exc()
            search_logger.info(mes_string.format(state=-1, query=query, time=time.time()-t_start))
            result = {'data': [], 'total': {'all': 0, 'course': 0, 'knowledge': 0}, 'error_code': -1, 'error_msg': str(e)}
            self.write(json.dumps(result))


class Home(tornado.web.RequestHandler):
    def get(self):
        self.write(json.dumps("this is xuetang"))


class Update(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        course_id_param = self.get_argument("course_id", "")
        special_field = self.get_argument("spc_field", "")
        specila_type = self.get_argument("spc_ty", "")

        if course_id_param == "":
            result = {"code": 0}
            self.write(json.dumps(result))
            return
        else:
            result = {"code": 1}
        try:
            course_id_param = course_id_param.strip().replace(" ", "+")
            cids = course_id_param.split(",")
            course_result = {}
            op_flag = True
            if special_field:
                # 更新指定字段
                op_flag, course_result = upsert_course(cids, specila_type, special_field)
            else:
                for course_id in cids:
                    if course_id != "":
                        status = update_index(course_id)
                        if status:
                            course_result[course_id] = 1
                        else:
                            course_result[course_id] = 0
                            op_flag = False
                    else:
                        course_result[course_id] = -1
                        op_flag = False
            result['course_result'] = course_result
            result['code'] = 1 if op_flag else 0  # 1 :正常   0:失败
        except Exception, e:
            result["code"] = 0
            result["error_msg"] = e.__str__()
        self.write(json.dumps(result))


class Data(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        data_type = self.get_argument("data_type", "")
        # todo
        course_id = self.get_argument("course_id", "").split(",")
        fields = self.get_argument("fields", "").split(",")
        result = dict()
        # print data_type, course_id, fields, "----"
        assert data_type in get_data_conf_setting, u"请输入合法data_type"
        the_index = get_data_conf_setting[data_type]["index"]
        the_type = get_data_conf_setting[data_type]["type"]

        try:
            data_list, num = get_course_info_specify_fields(fields, course_id, the_index, the_type)
            result['data'] = data_list
            result['error_code'] = 0  # 正常
            result["error_msg"] = "ok"
        except Exception, e:
            result["error_code"] = -1
            result["error_msg"] = e.__str__()
        self.write(json.dumps(result))


if __name__ == "__main__":
    search_logger.info("tornado启动")
    process()
    tornado.options.parse_command_line()
    settings = dict(gzip=True)
    app = tornado.web.Application(
        handlers=[
            (r"/search", SearchHandler),
            (r"/search_app", SearchAppHandler),
            (r"/home", Home),
            (r"/update_index", Update),
            (r"/course_data", Data),
            # (r"/autocomplete", ""),
            # (r"/related_search", ""),
            # (r"/suggest", "")
        ], **settings
    )
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()


# curl http://localhost:9998/wrap -d text="Lorem+ipsum+dolor+sit+amet,+consectetuer+adipiscing+elit.&width=100"
# http://192.168.9.243:9999/search?query=%E6%8E%A8%E7%90%86&qt=3&st=1
