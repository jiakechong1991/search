# -*- coding: utf-8 -*-

import sys,json,re
reload(sys)
sys.setdefaultencoding('utf-8')

DEBUG_FLAG = False

IndexName = "course"
LiveTypeName = "live"
MicrodegreeTypeName = "microdegree"
KnowledgeTypeName = "knowledge"
OrgInfo = "orginfo"
StaffInfo = "staffinfo"
CreditCourse = "creditcourse"  # 学分课
CourseTypeName = "course"
StaffTypeName = "staff"
BaseIndexMappingFile = "es_setting.json"

extend_mapping_setting = {
    LiveTypeName: "es_setting_extend_live.json",
    MicrodegreeTypeName: "es_setting_extend_microdegree.json",
    OrgInfo: "es_setting_extend_orginfo.json",
    StaffInfo: "es_setting_extend_staffinfo.json",
    CreditCourse: "es_setting_extend_creditcourse.json"
}
extend_mapping_field_setting = {
    # CourseTypeName: "es_setting_extend_field_course.json",
}


MicrodegreeTypeName = "microdegree"
word_fre_hdfs = ""
save_dict_file = "/usr/share/ikhot/ikhot/a.txt"
# save_dict_file = "/Users/wangxiaoke/xuetang/es5_1/search/search_service/tool/a.txt"


ban_single_token = [u"学", u"机"]
single_word = [u"雾", u"霾"]
es_assist_word_index = "es_noun_dict"  # ES中的各种辅助词所在索引名
es_noun_word_type = "noun_dict"  # ES中的补充补充专有名词
es_core_word_type = "manual_core_dict"  # ES中的补充核心词汇

############################################################
# qt(query_type)
DEF_QUERY_TYPE_MIX_CK = 1  # 1：课程&知识点&live&微学位 混合搜索
DEF_QUERY_TYPE_COURSE = 2  # 2：只搜索课程
DEF_QUERY_TYPE_KNOWLE = 3  # 3：只搜索知识点
DEF_QUERY_TYPE_ENGINEER = 4  # 4: 只搜工程硕士
DEF_QUERY_TYPE_LIVE = 5  # 5: 只搜直播
DEF_QUERY_TYPE_MICRODEGREE = 6  # 6: 只搜微学位

DEF_QUERY_TYPE_ORG = 13
DEF_QUERY_TYPE_STAFF = 14
DEF_QUERY_TYPE_CID = 16
DEF_QUERY_TYPE_CREDICT = 17  # 学分课

############################################################
# sort 排序类型
DEF_SORT_TYPE_SCORE = 1  # 1：按score排序
DEF_SORT_TYPE_STATUS = 2  # 2：按课程状态排序
DEF_COURSE_ORG = [item.lower() for item in ["TsinghuaX", "NTHU", "NCTU", "MITx", "RiceX", "UC BerkeleyX", "WellesleyX"]]

get_data_conf_setting = {
    "1": {   # data_type = 1 :主站获取course/course type中的部分字段信息
        "type": "course",
        "index": "course"
    }
}

conf = {
    "mongo": {
        "host": ["10.0.2.21"],
        "port": 27017
    },
    # "mysql": {
    #     "host": "192.168.9.104",
    #     "user": "edxapp001",
    #     "password": "password"
    # },
    "mysql": {
        "host": "datamysql.xuetangx.info",
        "user": "mysql_ro",
        "password": "xuetangx.com168mysql"
    },
    "es": {
        # "host": ["192.168.9.30"]  # ES5.1 测试机
        # host列表中,第一个必须是带有web  ik词库
        "host": ["10.0.2.151", "10.0.2.152", "10.0.2.153", "10.0.2.154", "10.0.2.155"]
    },
    "tap_es": {
        "host": ["10.0.2.158", "10.0.2.159", "10.0.2.160", "10.0.2.161", "10.0.2.162"]
    },
    # luzhe 纠错  接口
    "correct": {
        "host": "correction.xuetangx.info",
        "port": 9080
    },
    # 晨康的课程选课人数接口
    "accumulate": {
        "host": "tapapi.xuetangx.info",
        "port": 9000
    }
}

web_dict_file = "http://{host}/ikhot/a.txt".format(host=conf["es"]["host"][0])

