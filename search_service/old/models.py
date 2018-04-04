# -*- coding: utf-8 -*-
import pymongo
import MySQLdb
import re
import json
import hashlib
import logging
from elasticsearch import Elasticsearch, helpers
import datetime
import sys
import requests
from tool.settings import conf, IndexName
import logging.config
logger = logging.getLogger('build.'+__name__)
from tool.models import es_instance, mongo_db, md5, mysql_connection


import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def get_course_enrollment(es, course_id):
    # 从晨康那里拿到该course_id 对应的历史选课人数
    accumulate_server = conf['accumulate']['host']
    # accumulate_server = "10.0.0.76:9000"
    url = 'http://%s/data/student_courseenrollment' % accumulate_server
    pyload = {'course_id': course_id, "app_id": "201803240000000001"}
    search_json = {
        "query": {
            "term": {
                "course_id": {
                    "value": course_id
                }
            }
        }
    }
    count = 0
    flage = True
    temp_accumulate = 0
    last_accumulate = 0
    while flage:
        try:
            accumulate_result = requests.get(url, timeout=3, params=pyload)
            temp_accumulate = int(accumulate_result.json().get('data')[0]['acc_enrollment_num'])
            res = es.search(index="course", doc_type='course', body=search_json)
            last_accumulate = res['hits']['hits'][0]['_source']['accumulate_num']
            flage = False
        except Exception, e:
            logger.warn("ERROR_ACCUMULATE_SERVICE %s" % url)
            logger.warn(e)
            count += 1
            if count == 3:
                flage = False
                msg_e = u"选课人数TAP接口或者ES集群访问出现问题，报错停止更新"
                logger.warn("ERROR_ACCUMULATE_ES course_id={c},error={e},info={info}".format(
                    c=course_id, e=e, info=msg_e
                ))
                assert 1 == 0, msg_e

    res_num = temp_accumulate if temp_accumulate >= last_accumulate else last_accumulate
    logger.info("course_id:{c},temp_accumulate:{t},last_accumulate:{a}, last:{ll}".format(
        c=course_id, t=temp_accumulate, a=last_accumulate, ll=res_num)
    )
    res_num = temp_accumulate if temp_accumulate >= last_accumulate else last_accumulate
    return res_num


def get_course_enrollment_v2(es, course_id):
    # 从晨康那里拿到该course_id 对应的历史选课人数_v2版本
    accumulate_server = conf['accumulate']['host']
    # accumulate_server = "10.0.0.76:9000"
    url = 'http://%s/data/student_course_enrollment' % accumulate_server
    pyload = {'course_id': course_id, "app_id": "201803240000000001"}
    search_json = {
        "query": {
            "term": {
                "course_id": {
                    "value": course_id
                }
            }
        }
    }
    count = 0
    flage = True
    temp_accumulate = 0
    last_accumulate = 0
    while flage:
        try:
            accumulate_result = requests.get(url, timeout=3, params=pyload)
            temp_accumulate = int(accumulate_result.json().get('data')[0]['acc_enrollment_num'])
            res = es.search(index="course", doc_type='course', body=search_json)
            last_accumulate = res['hits']['hits'][0]['_source'].get('accumulate_num_v2', 0)
            flage = False
        except Exception, e:
            logger.warn("ERROR_ACCUMULATE_SERVICE_V2 %s" % url)
            logger.warn(e)
            count += 1
            if count == 3:
                flage = False
                msg_e = u"选课人数TAP接口或者ES集群访问出现问题，报错停止更新"
                logger.warn("ERROR_ACCUMULATE_ES_V2 course_id={c},error={e},info={info}".format(
                    c=course_id, e=e, info=msg_e
                ))
                assert 1 == 0, msg_e
    res_num = temp_accumulate if temp_accumulate >= last_accumulate else last_accumulate
    logger.info("V2: course_id:{c},temp_accumulate:{t},last_accumulate:{a}, last:{ll}".format(
        c=course_id, t=temp_accumulate, a=last_accumulate, ll=res_num)
    )
    return res_num


class CourseDescriptor(object):
    def __init__(self, course_id):
        # base info
        self.course_id = course_id
        self.course_meta_id = -1
        self.id_ = ""
        self.org = ""
        self.course = ""
        self.run = ""
        self.course_hash = ""
        self.owner = ""
        self.serialized = ""
        self.cid = []
        self.groupcid = []
        self.start = ""
        self.end = ""
        self.status = 0
        self.last_chapter = ""
        self.mode = []
        self.expire = "-"
        self.is_paid_only = 0
        # searchable info
        self.course_name = ""
        self.about = ""
        self.prerequisites = ""
        self.title_type = 'title'
        self.about_type = 'about'
        self.staff_type = 'staff'
        self.category_type = 'category'
        self.subtitle_type = 'subtitle'
        self.prerequisites_type = 'prerequisites'
        # chunks
        self.title_chunks = []
        self.about_chunks = []
        self.structure_chunks = []
        self.category_chunks = []
        # self.problem_chunks = []
        # self.html_chunks = []
        self.staff_chunks = []
        self.prerequisites_chunks = []
        self.subtitle_chunks = []
        self.fragment_chunks = []
        # structure info
        self.children = []  # children列表,用来构建一种children的子type
        self.chapters = []
        self.staff = []  # 这个列表包含了多个结构体,每个结构体都是一个老师的人员信息
        self.fragment = []  # 这个课程ID对应的知识点信息(course_meta_fragmentknowledge)
        # 数据库连接
        self.db = mongo_db()
        self.es = es_instance()
        self.mysql_conn = mysql_connection()
        self.cursor = self.mysql_conn.cursor()
        # 继续初始化相关课程信息
        self._init_course_data()
        self.location = [self.course_id]
        self.course_chunks = []
        self.ut = datetime.datetime.now()

    def _init_course_data(self):

        '''
        1 course_num:课程编号
        2 org:课程所属组织
        3 run:-
        4 name:课程标题
        5 about:课程介绍
        6 owner:课程拥有者
        7 serialized:
        8 id:主键ID
        9 start:开课时间
        10 end:结课时间
        11 prerequisites:预备知识
        12 subtitle:课程副标题
        13 status:课程状态是否激活
        14 course_type:
        15 is_paid_only:是否付费课程

        '''

        self.cursor.execute("""\
                select course_num,
                org,
                run,
                name,
                about,
                owner,
                serialized,
                id,
                start,
                end,
                prerequisites,
                subtitle,
                status,
                course_type,
                is_paid_only
                from course_meta_course where course_id = '{}';
                """.format(self.course_id))
        course_info = self.cursor.fetchall()[0]
        self.course = course_info[0]
        self.org = course_info[1]
        self.run = course_info[2]
        self.course_name = course_info[3]
        self.about = course_info[4]
        self.owner = course_info[5].lower()
        self.serialized = course_info[6]
        self.course_meta_id = int(course_info[7])
        if self.owner == 'xuetangx':  # 如果是自家课程
            cate_sql = """
                select
                    a.id, a.name, c.id gid
                from
                    course_meta_coursecategory a, course_meta_course_category b, course_meta_categorygroup c
                where
                    a.id = b.coursecategory_id and a.group_id = c.id and b.course_id = '{}' and c.owner = '{}' and c.slug in ('xuetangx', 'engineering_category') """.format(
                self.course_meta_id, self.owner)
            # logger.info(cate_sql)
        else:
            cate_sql = """
                select
                    a.id, a.name, c.id gid
                from
                    course_meta_coursecategory a, course_meta_course_category b, course_meta_categorygroup c
                where
                    a.id = b.coursecategory_id and a.group_id = c.id and b.course_id = '{}' and c.owner = '{}'""".format(
                self.course_meta_id, self.owner)
        # logger.info(cate_sql)
        self.cursor.execute(cate_sql)
        self.cid = []
        self.cid_name = []
        self.groupcid = []
        self.groupcid_name = []
        category_list = self.cursor.fetchall()
        # print category_list
        # 从测试结果知道这个返回列表经常是空的
        for category in category_list:
            # 工程硕士搜索
            if int(category[2]) == 431:
                self.groupcid.append(category[0])
                self.groupcid_name.append(category[1])
            else:
                self.cid.append(category[0])
                self.cid_name.append(category[1])
        # 开课结课时间的清洗
        if course_info[8] == None:
            self.start = '2030-01-01 00:00:00'
        else:
            self.start = datetime.datetime.strftime(course_info[8], "%Y-%m-%d %H:%M:%S")

        if course_info[9] == None:
            self.end = '2030-01-01 00:00:00'
        else:
            self.end = datetime.datetime.strftime(course_info[9], "%Y-%m-%d %H:%M:%S")
        _id = {
            'category': 'meta',
            'tag': 'i4x',
            'course': course_info[0],
            'org': course_info[1],
            'name': course_info[2],
            'revision': 'null'
        }
        self.id_ = json.dumps(_id)
        # 预备知识
        self.prerequisites = course_info[10]
        self.subtitle = course_info[11]
        self.course_hash = md5(self.course_id)
        self.status = int(course_info[12])
        self.course_type = int(course_info[13])
        self.is_paid_only = int(course_info[14])
        self.cursor.execute(
            "select count(*) from student_courseenrollment where course_id = '{}' and is_active = 1".format(
                self.course_id))
        self.enroll_num = int(self.cursor.fetchall()[0][0])  # 本轮选课人数
        self.accumulate_num = get_course_enrollment(self.es, self.course_id)  # 历史选课人数
        self.accumulate_num_v2 = get_course_enrollment_v2(self.es, self.course_id)  # 历史选课人数_v2版本
        self.cursor.execute(
            "select mode_slug, expiration_datetime from course_modes_coursemode where course_id = '{}'".format(
                self.course_id))
        mode = self.cursor.fetchall()
        # 获得课程的认证模式和过期时间,一个课程可能会有多组(模式,过期时间)
        # 但是下面的程序会有问题,如果有多组,那过期时间会用最后一个算

        if not mode:
            self.mode = ["honor"]
        else:
            for item in mode:
                self.mode.append(item[0])
                if item[1]:
                    # 模式的过期时间
                    self.expire = datetime.datetime.strftime(item[1], "%Y-%m-%d %X")
        # 碎片化 课程知识点
        self._init_fragment()
        # 教师
        self._init_staff()
        # logger.info("CourseDescriptor is ok@@@")

    def _init_staff(self):
        # 这个sql将课程ID关联的老师信息连表查询出来
        temp_sql = """\
                select re.id,
                st.name,
                st.company,
                st.department,
                st.position
                from course_meta_staff as st,
                course_meta_coursestaffrelationship as re,
                course_meta_course as co
                where st.id = re.staff_id
                and co.id = re.course_id
                and co.id = '{}'\
                        """.format(self.course_meta_id)
        # logger.info(json.dumps(temp_sql))
        self.cursor.execute(temp_sql)
        staff_list = self.cursor.fetchall()
        for staff in staff_list:
            self.staff.append({
                "relation_id": staff[0],
                "searchable_text": {
                    "name": staff[1],  # 教师名字
                    'company': staff[2],  # 教师单位
                    'department': staff[3],  # 教师部门院系
                    'position': staff[4],  # 教师职位
                    # 把上面信息合并成一个大的可检索的文本
                    'searchable_text': ' '.join(staff[1:]).strip()
                }
            })

    def _init_fragment(self):

        temp_sql = """
        SELECT a.title, a.description, a.id, a.is_active, a.cover_image, a.view_number, b.praise_number FROM
              (SELECT fr.title, fr.description, fr.id, fr.is_active, fr.cover_image, fr.topic_id, fr.view_number
				FROM course_meta_fragmentknowledge AS fr WHERE course_id = '{course_id}') a
			  LEFT JOIN
				forum_topic b ON  b.id = a.topic_id
        """.format(course_id=self.course_id)

        self.cursor.execute(temp_sql)
        fragment_list = self.cursor.fetchall()
        for fragment in fragment_list:
            temp_dict = {
                "fragment_id": fragment[2],
                "status": int(fragment[3]) - 1,
                "cover_image": fragment[4],
                "searchable_text": {  # 检索这些字段内容
                    "title": fragment[0],
                    "desc": fragment[1]
                },
                "praise_number": fragment[6],
                "view_number": fragment[5]
            }
            self.fragment.append(temp_dict)

    def _traverse_children(self):
        chapter_list = []
        sequential_list = []
        vertical_list = []
        now = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
        last = ""
        for item in self.children:
            if item.category == "chapter":
                if item.chapter_start == "":
                    continue
                if item.chapter_start <= now:
                    last = item.searchable_text
                else:
                    break
        self.last_chapter = last
        for item in self.children:
            if item.category == "chapter":
                item.location += [item.item_id]
                chapter_list.append(item)
                # print item
            elif item.category == "sequential":
                sequential_list.append(item)
            elif item.category == "vertical":
                vertical_list.append(item)

        def _init_location(parent_list, child_list):
            for parent in parent_list:
                for child in child_list:
                    for parent_child in parent.children:
                        if child.item_id == parent_child[1]:
                            child.location = parent.location + [child.item_id]
                            break

        _init_location(chapter_list, sequential_list)
        _init_location(sequential_list, vertical_list)

    def build_chunks(self):
        # course是作为父类的
        self.build_course_chunks()
        # 下面这些type的建立都是设定了父类的routing
        self.build_title_chunks()
        self.build_about_chunks()
        self.build_staff_chunks()
        self.build_category_chunks()
        # 一个课程相关的碎片化的知识
        self.build_fragment_chunks()
        for child in self.children:
            self.build_child_chunks(child)

    def flush_chunks(self):
        chunks_type = {
            'course': self.course_chunks,
            'title': self.title_chunks,
            'category': self.category_chunks,
            'about': self.about_chunks,
            'staff': self.staff_chunks,
            # children就在下面这个type中
            'structure': self.structure_chunks,
            'fragment': self.fragment_chunks}
        for (name, chunks) in chunks_type.items():
            # 将相应的type文档刷入相应的索引中。

            for chunk in chunks:
                chunk["_index"] = IndexName
                chunk["_type"] = name  # 选择type
                if chunk["_type"] in ["staff", "structure"]:
                    query_delete_dsl = {
                          "query": {
                            "term": {
                              "course_id": {
                                "value": chunk["_source"]["course_id"]
                              }
                            }
                          }
                        }
                    self.es.delete_by_query(index=IndexName, body=query_delete_dsl, doc_type=chunk["_type"], conflicts="proceed")

            helpers.bulk(self.es, chunks)

    def build_course_chunks(self):
        source = self.get_course_base()
        self.course_chunks = [{
            "_source": source,
            "_id": self.course_hash
        }]

    def build_title_chunks(self):
        source = self.get_search_base()
        source['item_type'] = self.title_type
        source['title'] = self.course_name
        if self.course_name != "":
            self.title_chunks = [{
                "_source": source,
                "_id": self.course_hash,
                "_parent": self.course_hash
            }]

    def build_about_chunks(self):
        source = self.get_search_base()
        source['item_type'] = self.about_type
        source['about'] = self.get_about_searchable_text()
        if self.get_about_searchable_text() != "":
            self.about_chunks = [{
                "_source": source,
                "_id": self.course_hash,
                "_parent": self.course_hash
            }]

    def build_category_chunks(self):
        source = self.get_search_base()
        source['item_type'] = self.category_type
        if self.course_name != "":
            if self.groupcid == []:
                source['cid_name'] = ' '.join(self.cid_name)
                source['cid'] = self.cid
                source['group'] = 'xuetangx'
                self.category_chunks.append({
                    "_source": source,
                    "_id": self.course_hash,
                    "_parent": self.course_hash
                })
            else:
                source['cid_name'] = ' '.join(self.groupcid_name)
                source['cid'] = self.groupcid
                source['group'] = 'engineer'
                self.category_chunks.append({
                    "_source": source,
                    "_id": self.course_hash,
                    "_parent": self.course_hash
                })

    def build_staff_chunks(self):
        for _staff in self.staff:
            relation_id = _staff['relation_id']
            source = self.get_search_base()
            info = _staff['searchable_text']
            source['item_type'] = self.staff_type + '-info'
            text = info.pop('searchable_text')
            if text == "":
                continue
            for (k, v) in info.items():
                source[k] = v.strip()
            staff_hash = md5(self.course_hash + '_' + str(info['name']) + '_info')
            self.staff_chunks.append({
                "_source": source,
                "_id": staff_hash,
                "_parent": self.course_hash
            })

    def build_fragment_chunks(self):
        for fr in self.fragment:
            fragment_id = fr['fragment_id']
            source = self.get_search_base()
            source['knowledge_id'] = str(fragment_id)
            source['status'] = fr['status']
            source['cover_image'] = fr['cover_image']
            source['praise_number'] = fr["praise_number"]
            source['view_number'] = fr["view_number"]
            is_empty = True
            for (name, value) in fr['searchable_text'].items():
                if value != "":
                    is_empty = False
                source['frag_' + name] = value
            if not is_empty:
                temp_dict = {
                    "_source": source,
                    "_id": fragment_id,
                }
                self.fragment_chunks.append(temp_dict)

    def build_child_chunks(self, child):
        source = self.get_search_base()
        source['item_type'] = child.item_type
        source['name'] = child.searchable_text
        if child.searchable_text == "":
            return
        if child.item_type == 'structure':
            source['location'] = child.location
            source['order_num'] = child.order_num
            source['structure'] = child.flash
            self.structure_chunks.append({
                "_source": source,
                "_id": child.item_hash(),
                "_parent": self.course_hash
            })

    def get_searchable_text(self, data):
        if data:
            p = re.compile(r'<.*?>')
            tag_removed = p.sub('', data)
            tag_removed = tag_removed.replace('<', '&lt;').replace('>', '&gt;')
            return tag_removed
        else:
            return ""

    def get_about_searchable_text(self):
        return self.get_searchable_text(self.about)

    def get_course_base(self):
        source = {
            'course_id': self.course_id,
            'mode': self.mode,
            'org': self.org.lower(),
            'owner': self.owner,
            'serialized': self.serialized,
            'cid': self.cid,
            'cid_name': self.cid_name,
            'start': self.start,
            'end': self.end,
            'last_chapter': self.last_chapter,
            'enroll_num': self.enroll_num,
            'accumulate_num': self.accumulate_num,
            'accumulate_num_v2': self.accumulate_num_v2,
            'course_type': self.course_type,
            'is_paid_only': self.is_paid_only,
            'ut': self.ut,
            'status': self.status,
            'expire': self.expire
        }
        return source

    def get_search_base(self):
        source = {'course_id': self.course_id,
                  'ut': self.ut
                  }
        """
        source = {
                'status': self.status,
                'course_id': self.course_id,
                'enroll_num': self.enroll_num,
                'last_chapter': self.last_chapter,
                'owner': self.owner,
                'start': self.start,
                'end': self.end,
                'serialized': self.serialized
                }
        """
        return source

    def close_mysql(self):
        if self.cursor:
            self.cursor.close()
        if self.mysql_conn:
            self.mysql_conn.close()


class OldCourseDescriptor(CourseDescriptor):
    def __init__(self, course_id):
        CourseDescriptor.__init__(self, course_id)
        self._init_children()
        self._traverse_children()

    def _init_children(self):
        temp_cmd = {'_id.org': self.org, '_id.course': self.course, '_id.category': 'course'}
        # logger.info(json.dumps(temp_cmd))
        course = self.db.modulestore.find_one(temp_cmd)
        if course:
            course_info = course.get('definition', {}).get('children', [])
            order_num = [0, 0]
            flash = []  # 存储课程对应的mongo中的可检索文本
            for chapter in course_info:
                chapter_id = chapter.split('/')[-1]
                order_num[0] += 1
                chapter_item = self.db.modulestore.find_one(
                    {'_id.org': self.org, '_id.course': self.course, '_id.category': 'chapter', '_id.name': chapter_id})
                if chapter_item != None:
                    item = OldStructureItem(self, chapter_item, order_num)
                    flash.append(item.searchable_text)
                    self.children.append(item)
                    for sequential in chapter_item.get('definition', {}).get('children', []):
                        seq_id = sequential.split('/')[-1]
                        seq_item = self.db.modulestore.find_one(
                            {'_id.org': self.org, '_id.course': self.course, '_id.category': 'sequential',
                             '_id.name': seq_id})
                        order_num[1] += 1
                        if seq_item != None:
                            item = OldStructureItem(self, seq_item, order_num)
                            flash.append(item.searchable_text)
                            self.children.append(item)
                order_num[1] = 0
            n = 2
            length = len(flash)
            for i in range(length):
                item = self.children[i]
                if i < n:
                    item.flash = flash[:2 * n + 1]
                elif i > length - n - 1:
                    item.flash = flash[length - 2 * n - 1: length]
                else:
                    item.flash = flash[i - n:i + n + 1]
                    # else:
                    #     logger.warn("GET_MONGO_ERROR: " + self.course_id)


class NewCourseDescriptor(CourseDescriptor):
    def __init__(self, course_id):
        CourseDescriptor.__init__(self, course_id)
        self._init_children()
        self._traverse_children()

    def _init_children(self):
        course = self.db.modulestore.active_versions.find_one({'org': self.org, 'course': self.course, 'run': self.run},
                                                              {'versions': 1})
        if course:
            version = course['versions']['published-branch']
            course_struct = self.db.modulestore.structures.find_one({'_id': version})
            order_num = [0, 0]
            flash = []
            blocks = course_struct.get('blocks', [])
            for block in blocks:
                block_type = block['block_type']
                block_id = block['block_id']
                # print block_type
                if block_type == 'course':
                    chapters = block.get('fields', {}).get('children', [])
                    for chapter in chapters:
                        chapter_id = chapter[1]
                        for chapter_item in blocks:
                            if chapter_item['block_id'] == chapter_id:
                                order_num[0] += 1
                                item = NewStructureItem(self, chapter_item, order_num)
                                flash.append(item.searchable_text)
                                self.children.append(item)
                                for sequential in chapter_item.get('fields', {}).get('children', []):
                                    seq_id = sequential[1]
                                    for seq_item in blocks:
                                        if seq_item['block_id'] == seq_id:
                                            order_num[1] += 1
                                            item = NewStructureItem(self, seq_item, order_num)
                                            flash.append(item.searchable_text)
                                            self.children.append(item)
                                order_num[1] = 0

            n = 2
            length = len(flash)
            for i in range(length):
                item = self.children[i]
                if i < n:
                    item.flash = flash[:2 * n + 1]
                elif i > length - n - 1:
                    item.flash = flash[length - 2 * n - 1: length]
                else:
                    item.flash = flash[i - n:i + n + 1]
                    # else:
                    #     logger.warn("GET_MONGO_ERROR: " + self.course_id)

    def build_index(self):
        # title
        pass


class Module(object):
    def __init__(self, course):
        # add course info for module
        self.course_id = course.course_id
        self.org = course.org
        self.course = course.course
        self.run = course.run
        self.category = ""
        self.item_id = ""
        # _id = hash(course_id)
        # self.id_ = course.id_
        self.course_hash = course.course_hash
        # course tree
        self.location = [course.course_id]
        self.db = course.db

    def item_hash(self):
        _id = {
            'tag': 'i4x',
            'org': self.org,
            'course': self.course,
            'name': self.item_id,
            'category': self.category,
            'revision': 'null'
        }
        return hashlib.sha1(json.dumps(_id)).hexdigest()


class StructureItem(Module):
    def __init__(self, course, order_num):
        Module.__init__(self, course)
        self.index = 'structure-index'
        self.item_type = 'structure'
        self.chapter_start = ''
        order_str = []
        for item in order_num:
            if item < 10:
                order_str.append("0" + str(item))
            else:
                order_str.append(str(item))
        self.order_num = '.'.join(order_str)

    def get_searchable_text(self, data):
        pass


class NewStructureItem(StructureItem):
    def __init__(self, course, data, order_num):
        StructureItem.__init__(self, course, order_num)
        self.item_id = data.get('block_id', '')
        self.searchable_text = self.get_searchable_text(data)
        self.category = data.get('block_type', '')
        self.children = data.get('fields', {}).get('children', [])
        if self.category == "chapter":
            _start = data.get('fields', {}).get("start", "2099-12-31T00:00:00Z")
            if isinstance(_start, str) or isinstance(_start, unicode):
                _start = _start.replace("T", " ").strip("Z")
                _start = datetime.datetime.strptime(_start[:16], "%Y-%m-%d %H:%M")

            self.chapter_start = datetime.datetime.strftime(_start + datetime.timedelta(hours=8), "%Y-%m-%d %H:%M:%S")

    def get_searchable_text(self, data):
        return data.get('fields', {}).get('display_name', '')


class OldStructureItem(StructureItem):
    def __init__(self, course, data, order_num):
        StructureItem.__init__(self, course, order_num)
        self.item_id = data['_id']['name']
        self.category = data['_id']['category']
        #  构造可检索文本
        self.searchable_text = self.get_searchable_text(data)
        children = data.get('definition', {}).get('children', [])
        self.children = map(lambda x: x.split('/')[-2:], children)
        if self.category == "chapter":
            _start = data.get('metadata', {}).get("start", "2099-12-31T00:00:00Z")
            if isinstance(_start, str) or isinstance(_start, unicode):
                _start = _start.replace("T", " ").strip("Z")
                _start = datetime.datetime.strptime(_start[:16], "%Y-%m-%d %H:%M")
            self.chapter_start = datetime.datetime.strftime(_start + datetime.timedelta(hours=8), "%Y-%m-%d %H:%M:%S")

    def get_searchable_text(self, data):
        return data.get('metadata', {}).get('display_name', '')


def upsert_course(course_id, specila_type, special_field):
    sucess_flag = True
    assert isinstance(course_id, list)
    es = es_instance()
    # assert es.indices.exists_type(index="course", doc_type=specila_type)
    assert specila_type == "course"
    if special_field == "accumulate_num":  # 为了替换成新的接口
        special_field = "accumulate_num_v2"
    res = es.indices.get_mapping(index="course", doc_type=specila_type)
    assert special_field in res["course"]["mappings"]["course"]["properties"].keys()
    result = {}
    chunks = []
    for item_course in course_id:
        if item_course != "":
            enrollment_num_v2 = get_course_enrollment_v2(es, item_course)
            doc_id = md5(item_course)  # 并不是每种type的id都是这样计算的
            if es.exists(index="course", doc_type=specila_type, id=doc_id):
                result[item_course] = 1  # 正确
                doc = {
                    "_type": specila_type,
                    "doc": {
                        "ut": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                        special_field: enrollment_num_v2
                    },
                    "_index": "course",
                    "doc_as_upsert": "true",
                    "_id": doc_id,
                    "_op_type": "update"
                }
                chunks.append(doc)

            else:
                result[item_course] = 0  # 不存在
                sucess_flag = False
        else:
            result[item_course] = -1
            sucess_flag = False
    sucess, error = helpers.bulk(es, chunks)
    for item_error in error:
        result[item_error] = 0
    return sucess_flag, result


def get_course_info_specify_fields(fields, course_ids, index="course", type="course"):
    assert isinstance(course_ids, list), u"必须使用list结构的course_ids"
    assert isinstance(fields, list), u"必须使用list结构的fields"
    dsl_body = {
        "query": {
            "terms": {
                "course_id": course_ids
            }
        }
    }
    res = es_instance().search(index=index, doc_type=type, body=dsl_body, preference="_primary_first")
    res_dict_t = dict()
    data_num = res["hits"]["total"]
    for item_doc in res["hits"]["hits"]:
        key_ = item_doc["_source"]["course_id"]
        res_dict_t[key_] = item_doc["_source"]
    res_list = list()
    for item_course_id in course_ids:
        item_dict_ = dict({"course_id": item_course_id})
        for item_field in fields:
            item_dict_[item_field] = res_dict_t.get(item_course_id, {}).get(item_field, None)
        res_list.append(item_dict_)
    return res_list, data_num














