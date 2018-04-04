# coding: utf-8
import json
import os
import sys
from tool.settings import conf, IndexName, BaseIndexMappingFile, extend_mapping_setting, extend_mapping_field_setting
"""
用于创建索引
"""


def opera_cmd(cmd):
    # 如果不能创建请使用sense删除旧索引
    print cmd
    f = os.popen(cmd)
    data = f.readlines()
    f.close()
    if len(data) >= 1:
        flag = json.loads(data[0]).setdefault("acknowledged", False)
        print data[0]
        print "命令执行成功" if flag else "命令执行失败"
    else:
        print "I am error"


def init_index():
    ip = conf["es"]["host"]
    cmd_index_base = """curl -XPUT http://{host}:9200/{Index} -d @spider/mapping_module/{index_file}""".format(
        host=ip[0], Index=IndexName, index_file=BaseIndexMappingFile)
    opera_cmd(cmd_index_base)
    # 新添加的mapping type
    for item_type in extend_mapping_setting:
        cmd_index_add = """curl -XPOST http://{host}:9200/{Index}/{Type}/_mappings \
        -d @spider/mapping_module/{index_add_file}""".format(
            host=ip[0], Index=IndexName, Type=item_type,
            index_add_file=extend_mapping_setting[item_type]
        )
        print "添加新的mapping  type"
        opera_cmd(cmd_index_add)
    # 扩充字段
    for item_type in extend_mapping_field_setting:
        cmd_index_add = """curl -XPOST http://{host}:9200/{Index}/{Type}/_mappings \
        -d @spider/mapping_module/{index_add_file}""".format(
            host=ip[0], Index=IndexName, Type=item_type,
            index_add_file=extend_mapping_field_setting[item_type]
        )
        print "添加字段"
        opera_cmd(cmd_index_add)



def delete_index():
    ip = conf["es"]["host"]
    cmd_list = []

    a = raw_input("你知道你现在是删除学堂云搜索的具体哪些索引吗? y/n: ")
    assert a == 'y'
    try:
        cmd_index_base = """curl -XDELETE http://{host}:9200/{Index} """.format(
            host=ip[0],
            Index=IndexName
        )
        cmd_list.append(cmd_index_base)
    except IndexError, e:
        pass

    for item_cmd in cmd_list:
        print item_cmd
        a = raw_input("执行这个删除吗:y/n ")
        try:
            assert a == 'y'
            opera_cmd(item_cmd)
        except Exception, e:
            pass


if __name__ == "__main__":

    args = sys.argv
    assert len(args) == 2, "请加参数init_index/delete_index"
    if args[1] == "init_index":
        init_index()
    elif args[1] == "delete_index":
        delete_index()
    else:
        print "输入参数有误,请加参数init_index/delete_index"

