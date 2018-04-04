# -*- coding: utf-8 -*-

import sys
import os
import requests
reload(sys)
import MySQLdb
import urllib
sys.setdefaultencoding('utf-8')
SEARCH_SERVICE = os.path.dirname(os.path.split(os.path.realpath(__file__))[0])
sys.path.append(SEARCH_SERVICE)
from elasticsearch import helpers
import json
from tool.models import mysql_connection, es_instance
from copy import deepcopy
from tool.settings import IndexName, LiveTypeName, MicrodegreeTypeName, OrgInfo, StaffInfo, CreditCourse
import time
from tool.models import md5
from tool.base_tool import print_time_json, add_key_prefix_4_dict
from copy import deepcopy
from collections import defaultdict

def query2urlcode(query):
    if type(query) == unicode:
        query = str(query)
    assert type(query) == str, u"请使用str"
    return urllib.quote(query)


live_info_base = {  # equal  mapping
    "live_key": None,  # table_name + id
    "live_item_title": None,  # 检 两种 live item公用的title
    "thumbnail": None,  # 缩略图连接
    "start_time": None,
    "end_time": None,
    "status": None,
    "live_cast_reading_id": None,

    "ut": None,
    "live_album_title": None,  # 检
    "live_album_sub_title": None,  # 检
    "live_album_thumbnail": None,
    "live_album_slug": None,
    # 第2种live的特有字段
    "location": None,
    "image_pc": None
}
microdegree_info_base = {
    "microdegree_key": None,
    "microdegree_title": None,
    "ut": None,
    "microdegree_sub_title": None,
    "picture": None,
    "course_ids": None,
    # "course_info_list": [] 这个是在建造过程中初始化的
}

course_info_base = {
    "course_id": None,
    "name": None,
    "subtitle": None,
    "about": None,
    "comment_org": None,
    "status": None,
    # "staffs_searchable": "" 这个是在建造过程中初始化的

}


def get_live_base_info(row_id, live_type, cursor, live_info_base):
    assert live_type in ("livecast_livecastitem", "livecast_event")
    if live_type == "livecast_livecastitem":
        sql_label_text = """
            SELECT
            A.* , B.title AS live_album_title, A.sub_title AS live_album_sub_title,
            B.thumbnail AS live_album_thumbnail, B.slug AS live_album_slug
        FROM
            (SELECT
              CONCAT_WS("_","{table_name}",id) AS live_key, title AS live_item_title, thumbnail,
              end_time AS end_time, start_time AS start_time,status, live_cast_reading_id, sub_title
             FROM livecast_livecastitem WHERE id = {row_id}) A
            LEFT JOIN
                livecast_livecast AS B
            ON
                A.live_cast_reading_id = B.id
            """.format(table_name=live_type, row_id=row_id)
    else:
        sql_label_text = """
          SELECT
            CONCAT_WS("_","{table_name}",id) AS live_key, title AS live_item_title, image_pc,
            start_time AS start_time, end_time AS end_time, location
          FROM livecast_event WHERE id = {row_id}
          """.format(table_name=live_type, row_id=row_id)

    cursor.execute(sql_label_text)
    res = cursor.fetchall()
    # live_info_base  # 做各种赋值
    desc = cursor.description
    chunks = [dict(zip([col[0] for col in desc], row)) for row in res]
    for item in chunks:
        for item_field in live_info_base.keys():
            live_info_base[item_field] = item.get(item_field, None)
    live_info_base["ut"] = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())


def spider_live(row_id_list, live_type, mode):
    conn = mysql_connection()
    cursor = conn.cursor()
    assert live_type in ("livecast_livecastitem", "livecast_event")
    assert isinstance(row_id_list, list), "请使用list结构"

    if mode == "ALL":
        row_id_list = []
        sql_text = "SELECT id FROM {table_name}".format(table_name=live_type)
        cursor.execute(sql_text)
        res = cursor.fetchall()
        for item in res:
            row_id_list.append(item[0])

    sum = len(row_id_list)
    counter = 0
    live_bulk_list = []
    for item_id in row_id_list:
        # 抽取组装这个course_id对应的内容
        live_dict = deepcopy(live_info_base)
        counter += 1
        process_msg = "{}%; sum={}; now={}".format((counter * 100 / sum), sum, item_id)
        print process_msg
        get_live_base_info(item_id, live_type, cursor, live_dict)
        if live_dict["start_time"]:
            live_dict["start_time"] = live_dict["start_time"].strftime("%Y-%m-%dT%H:%M:%S+08:00")
        if live_dict["end_time"]:
            live_dict["end_time"] = live_dict["end_time"].strftime("%Y-%m-%dT%H:%M:%S+08:00")
        live_bulk_list.append(live_dict)
    conn.close()
    for chunk in live_bulk_list:
        chunk["_index"] = IndexName
        chunk["_type"] = LiveTypeName
        chunk["_id"] = str(chunk['live_key'])
        # print json.dumps(chunk)
    sucess_num, error_list = helpers.bulk(es_instance(), live_bulk_list)
    print "一共有效更新%d" % (sucess_num)
    if error_list:
        print "error list:", error_list
    return sucess_num


def get_microdegree_base_info(row_id, cursor, microdegree_info_base):
    sql_label_text = """
      SELECT
        E.id AS microdegree_key, E.title AS microdegree_title, E.sub_title AS microdegree_sub_title, E.picture,
        E.status, GROUP_CONCAT(E.course_id) AS course_ids
      FROM
        ( SELECT D.id, D.title, D.sub_title, D.picture, D.status, C.course_id
          FROM
            (SELECT A.*, B.course_id
                FROM
                    (SELECT * FROM livecast_microdegree WHERE id ={row_id}) A
                LEFT JOIN
                    livecast_microdegreecourses AS B
                ON
                    A.id = B.microdegree_id) D
          LEFT JOIN
            course_meta_course AS C
          ON
            D.course_id = C.id) E
      GROUP BY E.id
        """.format(row_id=row_id)
    cursor.execute(sql_label_text)
    res = cursor.fetchall()
    # microdegree_info_base  # 做各种赋值
    desc = cursor.description
    chunks = [dict(zip([col[0] for col in desc], row)) for row in res]
    for item in chunks:
        for item_field in microdegree_info_base.keys():
            microdegree_info_base[item_field] = item.get(item_field, None)
    microdegree_info_base["course_info_list"] = []
    if microdegree_info_base["course_ids"]:
        for item_course_id in microdegree_info_base["course_ids"].split(u","):
            course_info_temp = deepcopy(course_info_base)
            # get 课程主体信息
            sql_text1 = """
              SELECT
                course_id,name, subtitle, about, comment_org, status
              FROM
                course_meta_course
              WHERE
                course_id = "{course_id}"
            """.format(course_id=item_course_id)
            cursor.execute(sql_text1)
            res = cursor.fetchall()
            # course_info_base  # 做各种赋值
            desc = cursor.description
            chunks = [dict(zip([col[0] for col in desc], row)) for row in res]
            for item in chunks:
                for item_field in course_info_temp.keys():
                    course_info_temp[item_field] = item.get(item_field, None)

            course_info_temp["staffs_searchable"] = u""
            # get 本课程的的教师信息
            sql_text2 = """
                SELECT CONCAT_WS(" ",D.name, D.company, D.about, D.position, D.department) as searchable FROM
                    (SELECT A.id as course_id, B.staff_id FROM
                        (SELECT id from course_meta_course WHERE course_id = "course-v1:microdegree+15_390x_2017T1+2017") A
                    left join
                        course_meta_coursestaffrelationship as B
                    on
                        A.id = B.course_id) C
                LEFT JOIN
                    course_meta_staff AS D
                ON
                    C.staff_id = D.id
            """.format(course_id=item_course_id)
            cursor.execute(sql_text2)
            staff_list = cursor.fetchall()
            for staff_text in staff_list:  # 每个元素是一个教师信息文本
                course_info_temp["staffs_searchable"] += staff_text[0].decode("utf-8")
            microdegree_info_base["course_info_list"].append(course_info_temp)

    microdegree_info_base["ut"] = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())


def spider_microdegree(row_id_list, mode):
    conn = mysql_connection()
    cursor = conn.cursor()
    assert isinstance(row_id_list, list), "请使用list结构"

    if mode == "ALL":
        row_id_list = []
        sql_text = "SELECT id FROM livecast_microdegree"
        cursor.execute(sql_text)
        res = cursor.fetchall()
        for item in res:
            row_id_list.append(item[0])

    sum = len(row_id_list)
    counter = 0
    microdegree_bulk_list = []
    for item_id in row_id_list:
        # 抽取组装这个microdegree_id对应的内容
        microdegree_dict = deepcopy(microdegree_info_base)
        counter += 1
        process_msg = "{}%; sum={}; now={}".format((counter * 100 / sum), sum, item_id)
        print process_msg
        get_microdegree_base_info(item_id, cursor, microdegree_dict)
        microdegree_bulk_list.append(microdegree_dict)
    conn.close()
    for chunk in microdegree_bulk_list:
        del chunk["course_ids"]
        chunk["_index"] = IndexName
        chunk["_type"] = MicrodegreeTypeName
        chunk["_id"] = str(chunk['microdegree_key'])
        # print json.dumps(chunk)
    sucess_num, error_list = helpers.bulk(es_instance(), microdegree_bulk_list)
    print "一共有效更新%d" % (sucess_num)
    if error_list:
        print "error list:", error_list
    return sucess_num


def spider_orginfo(org_name_list, mode):

    conn = mysql_connection()
    cursor = conn.cursor()
    all_sum = 0
    if mode == "ALL":
        org_name_list = []
        sql_text = "SELECT name FROM course_meta_organization"
        cursor.execute(sql_text)
        res = cursor.fetchall()
        for item in res:
            if item[0]:
                if item[0] not in org_name_list:
                    org_name_list.append(item[0])
                else:
                    pass
                    # print item[0].replace("\n", "").replace(" ", ""), "--"
    assert isinstance(org_name_list, list), "请使用list结构"
    sum = len(org_name_list)
    counter = 0
    orginfo_list = []
    for item_name in org_name_list:
        counter += 1
        process_msg = "{}%; sum={}; now={}".format((counter * 100 / sum), sum, item_name)
        print process_msg
        sql_text = """
            SELECT 
              id as orginfo_id,
              org as orginfo_org,
              name as orginfo_name,
              about as orginfo_about,
              cover_image,
              school_motto
            FROM   
              course_meta_organization 
            WHERE name="{org_name}" """.format(org_name=item_name)
        cursor.execute(sql_text)
        res = cursor.fetchall()
        desc = cursor.description
        if len(res) >= 1:
            chunks = [dict(zip([col[0] for col in desc], row)) for row in res]
            # 这里面只有一个元素
            orgs = set()
            for chunk in chunks:
                orgs.add(chunk["orginfo_org"])

            chunk = chunks[0]
            chunk["orginfo_org"] = u";".join(orgs)
            # url_ = "http://192.168.9.30:9999/search?process=0&group=&qt=2&cid=&serialized=&expiration=&course_type=0&st=1&hasTA=&course_id=&num=10&platform=0&version=2&org={org}&owner=xuetangX%3bedX&query=&pn=0&home=0&mode="
            url_ = "http://newsearch.xuetangx.info:9998/search?process=0&group=&qt=2&cid=&serialized=&expiration=&course_type=0&st=1&hasTA=&course_id=&num=10&platform=0&version=2&org={org}&owner=xuetangX%3bedX&query=&pn=0&home=0&mode="
            url = url_.format(org=query2urlcode(chunk["orginfo_org"]))
            # print url
            course_num = 0
            count_num = 3
            while count_num >= 1:
                try:
                    url_res = requests.get(url)
                    course_num = url_res.json()["total"]["course"]
                    count_num = 0
                except Exception, e:
                    print e
                    time.sleep(1)
                    count_num -= 1

            chunk["course_num"] = course_num
            print course_num, chunk["orginfo_org"], "------"
            chunk["ut"] = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())
            chunk["_index"] = IndexName
            chunk["_type"] = OrgInfo
            chunk["_id"] = str(chunk["orginfo_id"])

            orginfo_list.append(chunk)

        if len(orginfo_list) == 100:
            sucess_num, error_list = helpers.bulk(es_instance(), orginfo_list)
            all_sum += sucess_num
            del orginfo_list[:]
            if error_list:
                process_msg = "build error {ids}".format(ids=str(error_list))
                print process_msg
    sucess_num, error_list = helpers.bulk(es_instance(), orginfo_list)
    all_sum += sucess_num
    if error_list:
        process_msg = "build error {ids}".format(ids=str(error_list))
        print process_msg
    conn.close()
    print "一共有效更新%d" % (all_sum)
    return all_sum


def spider_staffinfo(row_name_list, mode):

    conn = mysql_connection()
    cursor = conn.cursor()
    all_sum = 0
    if mode == "ALL":
        row_name_list = []
        sql_text = "SELECT name FROM course_meta_staff"
        cursor.execute(sql_text)
        res = cursor.fetchall()
        for item in res:
            if not item[0] or u"#" in item[0] or len(item[0]) <= 1 or u"，" in item[0] \
                    or u"1" in item[0] or u"2" in item[0] or u"3" in item[0]:
                print item[0], "****"
                continue
            if item[0] not in row_name_list:  # 按照人名去重
                row_name_list.append(item[0])
    assert isinstance(row_name_list, list), "请使用list结构"
    sum = len(row_name_list)
    counter = 0
    staffinfo_list = []
    for item_name in row_name_list:
        counter += 1
        process_msg = "{}%; sum={}; now={}".format((counter * 100 / sum), sum, item_name)
        print process_msg
        sql_text = """
            SELECT 
              id as staffinfo_id,
              name as staffinfo_name,
              org_id_id as staffinfo_org,
              company as staffinfo_company,
              department as staffinfo_department,
              position as staffinfo_position,
              avartar as staffinfo_avartar,
              about as staffinfo_about      
            FROM
              course_meta_staff
            WHERE name="{name}" """.format(name=MySQLdb.escape_string(item_name))

        cursor.execute(sql_text)
        res = cursor.fetchall()
        desc = cursor.description
        chunks = [dict(zip([col[0] for col in desc], row)) for row in res]
        # 这里面只有一个元素

        if len(chunks) >= 1:
            for chunk in chunks:
                score_ = 0
                for key_ in chunk:
                    if not chunk[key_]:
                        score_ += 1
                chunk["score_"] = score_
            goal_chunk = sorted(chunks, key=lambda x: x["score_"], reverse=True)[-1]
            # url_ = "http://192.168.9.30:9999/search?query={name}&process=0&group=&qt=2&cid=&serialized=&expiration=&course_type=0&st=1&hasTA=&course_id=&num=15&platform=0&version=2&org=&owner=xuetangX%3BedX&pn=0&home=0&mode=&fields_v=1&persent=10"
            url_ = "http://newsearch.xuetangx.info:9998/search?process=0&group=&qt=2&cid=&serialized=&expiration=&course_type=0&st=1&hasTA=&course_id=&num=15&platform=0&version=2&org=&owner=xuetangX%3BedX&query={name}&pn=0&home=0&mode=&fields_v=1&persent=10"
            url = url_.format(name=query2urlcode(item_name))
            course_num = 0
            count_num = 3
            while count_num >= 1:
                try:
                    url_res = requests.get(url)
                    course_num = url_res.json()["total"]["course"]
                    count_num = 0
                except Exception,e:
                    print e
                    time.sleep(1)
                    count_num -= 1
            goal_chunk["staffinfo_course_num"] = course_num
            goal_chunk["ut"] = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())
            del goal_chunk["score_"]
            goal_chunk["staffinfo_company_md5"] = md5(str(goal_chunk["staffinfo_company"]))
            goal_chunk["_index"] = IndexName
            goal_chunk["_type"] = StaffInfo
            goal_chunk["_id"] = md5(str(goal_chunk["staffinfo_name"]))
            # print course_num, "----"
            staffinfo_list.append(goal_chunk)
            if len(staffinfo_list) == 100:
                sucess_num, error_list = helpers.bulk(es_instance(), staffinfo_list)
                all_sum += sucess_num
                del staffinfo_list[:]
                if error_list:
                    process_msg = "build error {ids}".format(ids=str(error_list))
                    print process_msg
    sucess_num, error_list = helpers.bulk(es_instance(), staffinfo_list)
    all_sum += sucess_num
    if error_list:
        process_msg = "build error {ids}".format(ids=str(error_list))
        print process_msg
    conn.close()
    print "一共有效更新%d" % (all_sum)
    return all_sum


def spider_xuetang_score(row_id_list, mode):

    def proc_stage(chunks_):
        chunk = dict()
        if len(chunks_) >= 1:
            for item_key in chunks_[0].keys():
                if item_key not in chunk:
                    if item_key == "creditcourse_id":
                        pass
                    else:
                        chunk[item_key] = set()
                for item_doc in chunks_:
                    if item_key == "creditcourse_id":
                        chunk[item_key]=item_doc[item_key]
                    else:
                        chunk[item_key].add(item_doc[item_key])
        for item_key in chunk:
            if isinstance(chunk[item_key], set):
                chunk[item_key] = list(chunk[item_key])
                if len(chunk[item_key]) == 1:
                    chunk[item_key] = chunk[item_key][0]
        return chunk

    sql_dict = {
        "school": [""" select * from
                        (SELECT 
                        a1.id as creditcourse_row_id, 
                        a1.id as l2_id,
                        a1.name as l2_name, 
                        a2.name as l1_name,
                        a2.id as l1_id 
                        FROM
                        newcloud_credit_organization as a1
                        left join
                        newcloud_credit_college as a2
                        on
                        a1.college_id = a2.id) a3
                        where a3.creditcourse_row_id ={param_key_}""",
                   proc_stage,
                   "creditcourse_row_id"
                   ],
        "stage": ["""
                    SELECT * from
                    (select
                    a1.creditcourse_id as creditcourse_id,
                    a2.id as stage_id,
                    a2.name as stage_name,
                    a2.weight as stage_weight
                    from
                    newcloud_credit_creditcourse_stages as a1
                    left join
                    newcloud_credit_coursestage as a2
                    on a1.coursestage_id = a2.id) a3
                    where
                    creditcourse_id = {param_key_}
                 """,
                  proc_stage,
                  "creditcourse_row_id"
                  ],
        "categorys": [
                    """select * from
                    (select
                    a1.creditcourse_id,
                    a2.id as category_id,
                    a2.name as category_name,
                    a2.weight,
                    a2.parent_id
                    from
                    newcloud_credit_creditcourse_categorys as a1
                    left join
                    newcloud_credit_coursecategory as a2
                    on
                    a1.coursecategory_id = a2.id) a3
                    where
                    a3.creditcourse_id = {param_key_}""",
                   proc_stage,
                   "creditcourse_row_id"
                ],
        "student_num": [
            """
            select count(user_id) as student_num from student_courseenrollment where course_id="{param_key_}" """,
            proc_stage, "course_id"
        ],
        "platform_num": [
            """
            select count(DISTINCT(plat_id)) as platform_num from newcloud_termcourse where coursekey="{param_key_}" """,
            proc_stage, "course_id"
        ]
    }
    conn = mysql_connection()
    cursor = conn.cursor()

    assert isinstance(row_id_list, list), "请使用list结构"

    if mode == "ALL":
        row_id_list = []
        sql_text = "SELECT id FROM newcloud_credit_creditcourse"
        cursor.execute(sql_text)
        res = cursor.fetchall()
        for item in res:
            row_id_list.append(item[0])

    sum = len(row_id_list)  # 总的数量
    all_sum = 0  # 成功数量
    creditcourse_bulk_list = []
    assert isinstance(row_id_list, list), "请使用list结构"
    counter = 0  # 游标
    for creditcourse_row__id in row_id_list:
        counter += 1
        process_msg = "{}%; sum={}; now={}".format((counter * 100 / sum), sum, creditcourse_row__id)
        print process_msg

        sql_text = """
            select 
            a.id, a.course_id, a.org, a.name, a.start, a.end,
            a.thumbnail,  a.owner, a.status, a.serialized, a.subtitle,
            b.id as creditcourse_row_id,
            b.created, b.modified,
            b.visible, b.is_enroll, b.is_apply, b.credit
            from 
            (select * from newcloud_credit_creditcourse where id={id} ) b
            left join 
            course_meta_course  as a
            on 
            a.id = b.course_id
            
            
            """.format(id=creditcourse_row__id)
        cursor.execute(sql_text)
        res = cursor.fetchall()
        desc = cursor.description
        chunks = [dict(zip([col[0] for col in desc], row)) for row in res]
        assert len(chunks) == 1, "抓取学堂学分课出现异常值，id={id}".format(id=creditcourse_row__id)
        chunk = chunks[0]

        for item_fields in sql_dict:
            param_key_ = sql_dict[item_fields][2]
            if chunk[param_key_]:
                sql_t = sql_dict[item_fields][0].format(param_key_=chunk[param_key_])
                # print sql_t
                cursor.execute(sql_t)
                res = cursor.fetchall()
                desc = cursor.description
                res = list(res)
                chunks_ = [dict(zip([col[0] for col in desc], row)) for row in res]
                res = sql_dict[item_fields][1](chunks_)
                chunk.update(res)

        sql_text = """select name from  newcloud_credit_coursecategory where id = {id}"""
        if "parent_id" in chunk:
            if not isinstance(chunk["parent_id"], list):
                chunk["parent_id"] = [chunk["parent_id"]]

            chunk["parent_name"] = []
            for item_parent in chunk["parent_id"]:
                try:
                    cursor.execute(sql_text.format(id=item_parent))
                    res = cursor.fetchall()
                    chunk["parent_name"].append(res[0][0])
                except Exception, e:
                    chunk["parent_name"].append(None)

        # print_time_json(chunk)
        add_key_prefix_4_dict(CreditCourse+"tp",chunk)
        chunk["ut"] = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())
        chunk["_index"] = IndexName
        chunk["_type"] = CreditCourse
        chunk["_id"] = md5(str(chunk[CreditCourse+"tp_"+"course_id"]))

        creditcourse_bulk_list.append(chunk)
        if len(creditcourse_bulk_list) == 100:
            sucess_num, error_list = helpers.bulk(es_instance(), creditcourse_bulk_list)
            all_sum += sucess_num
            del creditcourse_bulk_list[:]
            if error_list:
                process_msg = "build error {ids}".format(ids=str(error_list))
                print process_msg

    sucess_num, error_list = helpers.bulk(es_instance(), creditcourse_bulk_list)
    all_sum += sucess_num
    if error_list:
        process_msg = "build error {ids}".format(ids=str(error_list))
        print process_msg
    conn.close()
    print "一共有效更新%d" % (all_sum)
    return all_sum


if __name__ == "__main__":
    # spider_live([1], "livecast_livecastitem", "ON")
    # spider_live([1], "livecast_event", "ALL")
    # spider_microdegree([1], "ONE")
    # spider_orginfo([u"暨南大学"], "ALL")
    # spider_staffinfo([u"Adam Van Arsdale"], "ALL")
    spider_xuetang_score([25], "ALL")








