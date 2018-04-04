# -*- coding: utf-8 -*-

from hdfs import Client
import re
import json
import traceback
root_dir = "/user/wangxiaoke/wordcount"
hdfs_web_ip = "10.0.2.116:50070"
client = Client("http://{ip}".format(ip=hdfs_web_ip))

def crate_w_f_dict():
    # 提取HFDS文件,构建n个字典,每个学科一个,总的一个。
    w_f_dict = dict()
    w_f_dict["all_subject"] = dict()
    subject_list = client.list(root_dir)
    for item_subject in subject_list:
        w_f_dict[item_subject] = dict()
        subject_dir = root_dir + "/" + item_subject
        # 列出本科目的文件列表
        file_list = client.list(subject_dir)
        if "_SUCCESS" in file_list:
            file_list.remove("_SUCCESS")
        for item_file in file_list:
            file_dir = subject_dir + "/" + item_file
            print "读取文件:{file}".format(file=file_dir)
            with client.read(file_dir) as f:
                for temp_line in f.readlines():
                    temp_line = temp_line.rstrip('\n')
                    # print temp_line
                    if temp_line:
                        try:
                            match_object = re.match(r"""\(u['"](.*?)['"], (\d*?)\)""", temp_line)
                            key_word = match_object.group(1).decode('unicode-escape')
                            count = int(match_object.group(2))
                            # print key_word,"||",count,"++"
                            w_f_dict["all_subject"][key_word] = w_f_dict["all_subject"].get(key_word, 0) + count
                            w_f_dict[item_subject][key_word] = w_f_dict[item_subject].get(key_word, 0) + count
                        except Exception,e:
                            traceback.print_exc()
                            print "----"
                            print temp_line,
                            print len(temp_line)
                            print "-----"
                            print type(temp_line), "---"
                            raise Exception
                    else:
                        print temp_line
        print "本subject有:" + str(len(w_f_dict[item_subject])) + "词"

    return w_f_dict


# def main():
#     pass
#     # 周期性生成新的字典,并替换旧的字典
#     now_dict = crate_w_f_dict()
#
#     # 判断日期,3天更新一次字典。
#         # 如果文件夹都已经存在。
#             # 开始重建字典
#             new_dict = crate_w_f_dict()



def sum_word_subject(w_f_dict):
    word_sum_subject = dict()
    for item_subject in w_f_dict:
        temp_sum = 0
        for item_word in w_f_dict[item_subject]:
            temp_sum += w_f_dict[item_subject][item_word]
        word_sum_subject[item_subject] = temp_sum
    return word_sum_subject


def get_w(w_f_dict, word_sum_subject, word, subject):
    f1 = w_f_dict[subject][word]/word_sum_subject[subject]
    f2 = w_f_dict["all_subject"][word]/word_sum_subject["all_subject"]
    return f1*1.0/f2




if __name__ == "__main__":
    res = crate_w_f_dict()
    f = open("kkk.txt", "w")
    f.write(json.dumps(res))
    f.close()





