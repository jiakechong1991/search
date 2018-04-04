# -*- coding:utf-8 -*-
import sys
import os
import traceback
reload(sys)
from tool.models import mysql_connection
from old.indexing import Searchable
from spider.spider_live import spider_live, spider_microdegree, spider_orginfo, spider_staffinfo
sys.setdefaultencoding('utf-8')
from logging import config, getLogger
sys.path.append(os.path.split(os.path.realpath(__file__))[0])
DEF_CUR_FILE_PATH = os.path.split(os.path.realpath(__file__))[0]
config.fileConfig('{0}/tool/logging.conf'.format(DEF_CUR_FILE_PATH))
logger = getLogger('build')


def usage():
    return """
    usage:
    python build_index.py cmd
    cmd=index: build es index
    cmd=index-file: build index from file
    """


def build_index():
    conn = mysql_connection()
    cursor = conn.cursor()
    s = Searchable()  # 建立数据库连接

    # 所有要处理的所有课程的ID
    cursor.execute("select course_id from course_meta_course")
    courses = cursor.fetchall()
    course_sum = len(courses)
    print 'course count: ', course_sum
    count = 0
    index = 0
    for course in courses[0:]:
        index += 1
        course_id = course[0]
        try:
            temp_info = "index "+str(index)+"  ID: "+course_id
            logger.info(temp_info)
            c = s.get_course(course_id)
        except Exception, e:
            logger.error('GET_COURSE_ERROR: ' + course_id)
            logger.error(traceback.print_exc())
            c = None
        if None != c:
            try:
                c.build_chunks()  # 构建bigTable结构体
                c.flush_chunks()  # 刷结构体入ES
                c.close_mysql()
                count += 1
            except Exception, e:
                logger.warn('INSERT_ES_ERROR ' + course_id)
                print traceback.print_exc()
    s.close()
    print '课程索引ok,正确数目:',count,"错误数目:",(course_sum-count)

    print "开始索引live"
    # spider_live([1], "livecast_livecastitem", "ALL")
    # spider_live([1], "livecast_event", "ALL")
    print "live索引ok!"

    print "开始索引微学位"
    spider_microdegree([1], "ALL")
    print "微学位索引ok!"

    print u"开始索引orginfo"
    spider_orginfo([1], "ALL")
    print u"开始索引orginfo索引结束"

    print u"开始索引staffinfo"
    spider_staffinfo([u"邓俊辉"], "ALL")
    print u"开始索引staffinfo索引结束"


def build_index_from_file(file_name):
    s = Searchable()
    f = open(file_name)
    course_list = set([])
    for line in f:
        index_info = line.strip().split('\t')
        if len(index_info) == 3:
            [cmd, course_num, source] = index_info
            if cmd == "old":
                course_id = s.old_course_num_to_course_id(course_num)
            elif cmd == "new":
                course_id = s.new_course_num_to_course_id(course_num)
            else:
                course_id = None
        # two fields or one field, the first field is course_id
        else:
            course_id = index_info[0]
        if course_id == None:
            pass
        elif course_id not in course_list:
            course_list.add(course_id)
    # 至此重新获得course_id列表
    for course_id in course_list:
        try:
            course = s.get_course(course_id)
        except Exception, e:
            logger.warn('GET_COURSE_ERROR: ' + course_id)
            print e
            course = None
        if course != None:
            course.build_chunks()
            course.flush_chunks()
            course.close_mysql()
    f.close()
    s.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        cmd = "index"
        #print usage()
    else:
        cmd = sys.argv[1]
    if cmd == 'index':
        import time
        t1 = time.time()
        build_index()
        t2 = time.time()
        print t2-t1
    elif cmd == 'index-file':
        if len(sys.argv) < 2:
            print usage()
        else:
            file_name = sys.argv[2]
            build_index_from_file(file_name)
    else:
        print usage()
