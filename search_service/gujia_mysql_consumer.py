# -*- coding: utf8 -*-

from kafka import KafkaConsumer, KafkaProducer
import threading
import time
import Queue
from Queue import Empty
import json
import random
import traceback
import os
import datetime
from logging import config, getLogger
from old.indexing import Searchable
from tool.models import es_instance, es_instance_tap
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.append(os.path.split(os.path.realpath(__file__))[0])
DEF_CUR_FILE_PATH = os.path.split(os.path.realpath(__file__))[0]
config.fileConfig('{0}/tool/logging.conf'.format(DEF_CUR_FILE_PATH))
logger = getLogger('search')
host_list = ['10.0.0.68:9092', '10.0.0.69:9092', '10.0.0.70:9092']
ACTIVE_COURSE_ID_TOPIC = 'active_course_id'
COURSE_TOPIC_MYSQL = 'edxapp'
TAP_PARENT_COURSE = 'course-edit'

# es
tap_index_name = "course_ancestor"
tap_type_name = "course_ancestor"

GROUP_ID1 = 'search_index_update_gujia'
GROUP_ID2 = 'search_index_update_gujia'
GROUP_ID3 = 'search_index_update_tap'
course_queue = Queue.Queue()  # size = infinite

Index = "monit_table"
Type = "search_add_index_kafka"
doc_id = "1"
es = es_instance()
es_tap = es_instance_tap()
last_time = 0


def write_es(kafka_source):
    time_str = time.strftime('%Y-%m-%d %X', time.localtime())
    assert kafka_source in ("gujia_kafka", "mysql_kafka", "tap_parent_kafka"), "请输入正确的键"
    doc = {
        "doc": {
            kafka_source: time_str
        }

    }
    es.update(index=Index, doc_type=Type, id=doc_id, body=doc, retry_on_conflict=5)


def write_tap_es(course_id, parent_id):
    try:
        dsl = {
            "query": {
                "term": {
                    "course_id": {
                        "value": course_id
                    }
                }
            },
            "script": {
                "inline": "ctx._source.parent_id = params.parent_id;ctx._source._ut = params._ut;",
                "lang": "painless",
                "params": {
                    "parent_id": parent_id,
                    "_ut": time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())
                }
            }
        }
        res = es_tap.update_by_query(index=tap_index_name, doc_type=tap_type_name, body=dsl, conflicts="proceed")
        assert res["updated"] <= 1, "courseId 是唯一的"
        if res["failures"]:
            logger.info(u" tap 下面course的parent 更新错误")
            logger.info(res["failures"])
            raise Exception
        return True
    except Exception, e:
        logger.info(e)
        return False


def get_all_tap_es():
    from elasticsearch import helpers

    def get_scroll_search_res(query_dsl, scroll='5m', index=tap_index_name, doc_type=tap_type_name, timeout="1m"):
        es_result = helpers.scan(
            client=es_tap,
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

    print len(doc_list)
    for item_doc in doc_list:
        res = write_tap_es(item_doc["course_id"], item_doc["parent_id"])
        if not res:
            print item_doc["course_id"]


def get_es(parent_id):
    course_list = list()
    query = {
      "size": 0,
      "query": {
        "term": {
          "parent_id": {
            "value": parent_id
          }
        }
      }
    }
    res = es_tap.search(index=tap_index_name, doc_type=tap_type_name, body=query)
    query["size"] = res["hits"]["total"]
    res = es_tap.search(index=tap_index_name, doc_type=tap_type_name, body=query)
    for item in res["hits"]["hits"]:
        course_list.append(item["_source"]["course_id"])
    return course_list


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
        print '%s starting\n' % (self.name)
        self.res = apply(self.func, self.args)
        print self.res
        print '%s finished\n' % (self.name)


def print_log_by_time(str_log):
    global last_time
    now = time.time()
    if now - last_time >= 60:
        logger.info(str_log)
        write_es("mysql_kafka")
        last_time = now


def get_change_course_mysql():
    consumer = KafkaConsumer(COURSE_TOPIC_MYSQL, group_id=GROUP_ID2, bootstrap_servers=host_list)
    while True:
        try:
            msg = next(consumer)
            print_log_by_time(u"应该每分钟一次,以证明binlog kafka还活着")
            record = json.loads(msg.value)
            if record['tableName'] == "course_meta_course":  # 过滤消息
                course_id = None
                for item in record['afterColumns']:
                    if item['name'] == "course_id":
                        course_id = item['value']
                if course_id:
                    course_queue.put(course_id, block=True, timeout=None)
                    logger.info("mysql - courseid:{course}".format(course=course_id))
                    logger.info("mysql - message offset:{offset}".format(offset=msg.offset))
                else:
                    logger.error("本条course_meta_course的binlog中没有course_id,错误如下")
        except Exception, e:
            logger.error(traceback.print_exc())


def get_change_course_kafka():
    consumer = KafkaConsumer(ACTIVE_COURSE_ID_TOPIC, group_id=GROUP_ID1, bootstrap_servers=host_list)
    while True:
        try:
            try:
                msg = next(consumer)
                course_id = json.loads(msg.value)['course_id']
                course_queue.put(course_id, block=True, timeout=None)
                logger.info("gj_kafka - courseid:{course}".format(course=course_id))
                logger.info("gj_kakka - message offset:{offset}".format(offset=msg.offset))
            except KeyError:
                logger.error(u"keyError,这是一个心跳包,证明kafka还活着")
                write_es("gujia_kafka")
        except Exception, e:
            logger.error(traceback.print_exc())


def get_change_course_tap_kafka():
    # from yuanping`s kafka get
    consumer = KafkaConsumer(TAP_PARENT_COURSE, group_id=GROUP_ID3, bootstrap_servers=host_list)
    while True:
        try:
            course_list = list()
            msg = next(consumer)
            msg_json = json.loads(msg.value)
            course_id = msg_json["course_id"]
            if course_id == "xxxx":
                logger.info(u"这是一个心跳包,证明tap kafka还活着")
                write_es("tap_parent_kafka")
            else:
                parent_id_cur_temp = msg_json["parent_id_cur"]
                parent_id_pre_temp = msg_json["parent_id_pre"]
                logger.info(u" tap course:{course} parent_cur:{cur} parent_pre:{pre}".format(
                    course=course_id, cur=parent_id_cur_temp, pre=parent_id_pre_temp))
                course_list.extend(get_es(parent_id_cur_temp))
                course_list.extend(get_es(parent_id_pre_temp))
                write_tap_es(course_id, parent_id_cur_temp)
                course_list.append(course_id)
                logger.info(u"tap本次一共{num}个".format(num=len(set(course_list))))
                for item_course in set(course_list):
                    course_queue.put(item_course, block=True, timeout=None)
                logger.info(u"本次tap的course 都填充进队列")
        except Exception, e:
            logger.error(traceback.print_exc())
            logger.error(u"tap kakfa发生崩溃错误")


def write_heart_tap_kafka():
    # write yuanping`s kafka get
    producer = KafkaProducer(bootstrap_servers=host_list, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        try:
            time.sleep(60)
            template_msg = {
             'course_id': 'xxxx',
             'parent_id_pre': 'xxxx',
             'parent_id_cur': 'xxxx'
            }
            producer.send(TAP_PARENT_COURSE, template_msg)  # 序列化值
            logger.info(u"tap kafka 一次写入心跳包")
        except KeyError:
            logger.error(u"tap kafka 一次写入心跳包出错")


def process_course():
    course_set = set()
    max_pop_time = 3
    while True:
        try:
            time.sleep(10)  # 为了去重
            if course_queue.empty():
                if int(random.random()*10) in [1]:
                    logger.info("当前队列为空")
                continue
            start_time = time.time()
            logger.info("当前队列元素数:{num}".format(num=course_queue.qsize()))
            while True:
                try:
                    diff_time = time.time() - start_time
                    if diff_time > max_pop_time:
                        logger.error("清空操作已经耗时:{time}".format(time=diff_time))
                        logger.error("当前队列元素数:{num}".format(num=course_queue.qsize()))
                    item = course_queue.get(block=False, timeout=None)
                    course_set.add(item)
                except Empty:
                    break  # 已经清空,可以退出
            elapse_time = time.time() - start_time
            logger.info("本次清空队列耗时{elapse_time}".format(elapse_time=elapse_time))

            s = Searchable()  # 建立数据库连接
            sucess_course_sum = 0
            for item_course in course_set:
                try:
                    logger.info("now index course:{course}".format(course=item_course))
                    c = s.get_course(item_course)
                    if c:
                        c.build_chunks()  # 构建bigTable结构体
                        c.flush_chunks()  # 刷结构体入ES
                        c.close_mysql()
                        sucess_course_sum += 1
                    else:
                        logger.error("这个课程不存在,courseid:{id}".format(id=item_course))
                except Exception, e:
                    logger.error(e)
                    logger.info(traceback.print_exc())
                    logger.error("build index error,courseid:{id}".format(id=item_course))
            s.close()
            logger.info("本批次总课程数:{num1} 成功课程数:{num2} 消耗时间:{time}".format(
                    num1=len(course_set), num2=sucess_course_sum, time=str(time.time()-start_time)
                    ))
            course_set = set()
        except Exception, e:
            logger.info(traceback.print_exc())
            logger.error("消费者出现问题,主动退出")


funList = [get_change_course_mysql, get_change_course_kafka,
           get_change_course_tap_kafka, write_heart_tap_kafka,
           process_course
           ]


def process():
    thread_list = []
    for item_function in funList:
        t = MyThread(item_function, (), item_function.__name__)
        time.sleep(1)
        thread_list.append(t)

    for item_theard in thread_list:
        item_theard.start()
        time.sleep(1)
    logger.info("线程全部启动")
    for item_theard in thread_list:
        item_theard.join()
    logger.info("线程全部退出")


if __name__ == "__main__":
    # get_change_course_tap_kafka()
    # get_change_course_tap_kafka()
    # write_heart_tap_kafka()
    # get_es("SEU/00034237/2015_T1")
    # write_tap_es("course-v1:XJTUSPOC+COMP200153+2016_T2", "XJTUSPOC/COMP1023/2015_T2")
    # get_all_tap_es()
    process()
    # 消费者测试
    # temp = "course-v1:Yifang+ASEC121+2017_T1"
    # course_queue.put(temp)
    # process_course()
    # get_change_course_mysql()
    # get_change_course_kafka()
    # "gujia_kafka", "mysql_kafka"
    # write_es("gujia_kafka")



