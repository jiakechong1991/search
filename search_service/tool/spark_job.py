# -*- coding: utf-8 -*-

import jieba
from pyspark import SparkContext
import json
import os
import time
from hdfs.client import Client
client = Client("http://10.0.2.116:50070")
root_dir = "/user/wangxiaoke/wordcount"
APP_NAME = "my spark application"
es_node2 = "10.0.2.152"
hdfs_ip = "10.0.2.116:8020"

stop_word_list = ["(", ")", ",", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                  "。", " ", "》", "《", "&", "、", "？", "-", ".","；", "一",
                  "）", "（", "\t", ";", "\n", ":"
                  ]
cid_dict = {
    117: "计算机",
    118: "经管·会计",
    119: "创业",
    120: "电子",
    121: "工程",
    122: "环境·地球",
    123: "医学·健康",
    124: "生命科学",
    125: "数学",
    126: "物理",
    127: "化学",
    128: "社科·法律",
    129: "文学",
    130: "历史",
    131: "哲学",
    132: "艺术·设计",
    133: "外语",
    134: "教育",
    135: "其他",
    201: "大学先修课",
    2550: "公共管理",
    2783: "建筑",
    2952: "职场"
}
will_pro_type = {
    "about": ["about"],
    "staff-info": ["position", "company", "department", "name"],
    "structure": ["name", "structure"],
    "category": ["cid_name"],
    "title": ["title"]
}
text_type_list = ["about", "staff", "structure", "category", "title"]


def get_es_by_subject(subject_id):
    dsl = {
        "query": {
            "has_parent": {
                "parent_type": "course",
                "query": {
                    "term": {
                        "cid": {
                            "value": subject_id
                        }
                    }
                }
            }
        }
    }
    sum_rdd = None
    for item_type in text_type_list:
        es_conf = {
            "es.nodes": es_node2,
            "es.resource": "course/{type}".format(type=item_type),
            "es.scroll.keepalive": "10m",
            "es.scroll.size": "50",
            "es.query": json.dumps(dsl),
        }
        es_rdd = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_conf)
        if sum_rdd:
            sum_rdd = sum_rdd.union(es_rdd)
        else:
            sum_rdd = es_rdd
    return sum_rdd


def filter_doc(item_doc):
    doc_type = item_doc[1].get("item_type", "no")
    return True if (doc_type in will_pro_type) else False


def get_tokens_from_doc(item_doc):
    """
    从doc构建分词列表,
    """
    token_list = []
    doc_type = item_doc[1].get("item_type", "no")
    for text_field in will_pro_type[doc_type]:
        temp_line = item_doc[1].get(text_field, u"")
        # jieba.load_userdict('dict.txt')
        if not temp_line:
            continue
        one_field_text = u""
        if isinstance(temp_line, tuple):
            for item in temp_line:
                if item:
                    one_field_text += item
        else:
            one_field_text = temp_line
        seg_list = jieba.cut(one_field_text, cut_all=False)  # 精确模式
        for w in seg_list:
            if w:
                token_list.append(w)
    return token_list


def delete_r(your_dir):
    a = os.system('hdfs dfs  -rm -r {dir}'.format(dir=your_dir))
    return not a

def main(sc):
    start_time = time.time()
    for item_subject_id in cid_dict:
        print "收集:||" + cid_dict[item_subject_id] + "||文本素材"
        res_rdd = get_es_by_subject(item_subject_id)
        res_rdd = res_rdd.filter(filter_doc).flatMap(get_tokens_from_doc).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
        if str(item_subject_id) in client.list(root_dir):
            if delete_r("{root_dir}/{file_name}".format(root_dir=root_dir, file_name=item_subject_id)):
                print "删除:||" + cid_dict[item_subject_id] + "||上次的词频统计" + "成功"
            else:
                print "删除:||" + cid_dict[item_subject_id] + "||上次的词频统计" + "失败"
        res_rdd.saveAsTextFile("hdfs://{ip}/{root_dir}/{file_name}".format(
            ip=hdfs_ip, root_dir=root_dir, file_name=item_subject_id))
        print "本次对:||" + cid_dict[item_subject_id] + "||的词频统计完成"
    print "本轮全量学科的词频统计完成,耗时{t}秒".format(t=time.time()-start_time)


if __name__ == "__main__":
    sc = SparkContext(appName='search_click', pyFiles=None)
    main(sc)







    # text = sc.textFile("hdfs:///user/wangxiaoke/all.txt")
    # words = text.flatMap(tokenize_by_jieba).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    # words.saveAsTextFile("hdfs:///user/wangxiaoke/wordcount/all_index")  # 切好的词可以放到磁盘上
    #






