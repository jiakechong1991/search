# -*- coding: utf-8 -*-
import re
import json
import hashlib
from models import NewCourseDescriptor, OldCourseDescriptor, mysql_connection, mongo_db

class Searchable(object):
    def __init__(self):
        # 建立数据库连接
        self.conn = mysql_connection()
        self.cur = self.conn.cursor()
        self.db = mongo_db()
        
    def _check_course(self, course_id):
        # 通过课程ID判断是新格式ID还是旧格式ID

        # new pattern: xxx:xxx+xxx+xxx
        new_pattern = re.compile(r'[\w.%-]+:[\w.%-]+\+[\w.%-]+\+[\w.%-]+')
        match = new_pattern.match(course_id)
        if match:
            return True
        # old pattern: xxx/xxx/xxx
        old_pattern = re.compile(r'[\w.%-]+/[\w.%-]+/[\w.%-]+')
        match = old_pattern.match(course_id)
        if match:
            return False
        return None

    def get_course(self, course_id):
        self.cur.execute("select * from course_meta_course where course_id = '{}'".format(course_id))
        course = self.cur.fetchall()
        if len(course) == 0:  # 如果课程存在
            return None
        _type = self._check_course(course_id)  # 获取课程ID的新旧类型
        if _type == True:
            return NewCourseDescriptor(course_id)
        elif _type == False:
            return OldCourseDescriptor(course_id)
        else:
            return None

    def old_course_num_to_course_id(self, course_info):
        info = course_info.split('/')
        if len(info) == 2:
            [org, num] = info
            query = {
                "_id.org" : org,
                "_id.course" : num,
                "_id.category": "course"
            }
            coll = self.db.modulestore.find_one(query)
            if coll != None:
                return '/'.join([org, num, coll['_id']['name']])
            else:
                sql = "select course_id from course_meta_course where org = '{}' and course_num = '{}'".format(org, num)
        elif len(info) == 3:
            [org, num, run] = info
            sql = "select course_id from course_meta_course where org = '{}' and course_num = '{}' and run = '{}'".format(org, num, run)
        self.cur.execute(sql)
        result = self.cur.fetchall()
        if result != ():
            return result[0][0]
        else:
            return None

    def new_course_num_to_course_id(self, course_info):
        info = course_info.split('+')
        if len(info) == 2:
            [org, num] = info
            query = {
                "org" : org,
                "course" : num,
            }
            coll = self.db.modulestore.active_versions.find_one(query)
            if coll != None:
                return "course-v{}:".format(coll['schema_version']) + "+".join([org, num, coll['run']])
            else:
                sql = "select course_id from course_meta_course where org = '{}' and course_num = '{}'".format(org, num)
        elif len(info) == 3:
            [org, num, run] = info
            query = {
                "org" : org,
                "course" : num,
                "run" : run
            }
            coll = self.db.modulestore.active_versions.find_one(query)
            if coll != None:
                return "course-v{}:".format(coll['schema_version']) + "+".join([org, num, run])
            else:
                sql = "select course_id from course_meta_course where org = '{}' and course_num = '{}' and run = '{}'".format(org, num, run)
        self.cur.execute(sql)
        result = self.cur.fetchall()
        if result != ():
            return result[0][0]
        else:
            return None
 
    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
