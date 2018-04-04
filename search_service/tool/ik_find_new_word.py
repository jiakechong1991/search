# -*- coding: utf-8 -*-


import sys
import time
import os
from logging import config, getLogger
from models import mysql_connection
from settings import save_dict_file
reload(sys)
sys.setdefaultencoding('utf-8')

DEF_CUR_FILE_PATH = os.path.split(os.path.realpath(__file__))[0]
config.fileConfig('{0}/logging.conf'.format(DEF_CUR_FILE_PATH))
logger = getLogger('search')


def get_staff_name():
    try:
        staff_name_set = set()
        temp_sql = """select name from course_meta_staff"""
        cursor = mysql_connection().cursor()
        cursor.execute(temp_sql)
        staff_name_list = cursor.fetchall()
        for item_staff_name in staff_name_list:
            if item_staff_name and item_staff_name[0]:
                # 如果有值,且不为空
                staff_name = item_staff_name[0]
                filter_list2 = [u"教师", u"副教授", u"教授", u"讲师", u"测试", u"副研究员", u"研究员"]
                for item_filter in filter_list2:
                    staff_name = staff_name.replace(item_filter, "")
                # 提取指定长度范围内的人名
                if 2 <= len(staff_name) <= 4:

                    staff_name = staff_name.replace(" ", "")
                    try:
                        int(staff_name)  # 防止出现数字
                    except Exception:
                        if u'\u4e00' <= staff_name[-1] <= u'\u9fff' and u'\u4e00' <= staff_name[0] <= u'\u9fff':
                            staff_name_set.add(staff_name)
                        # else:
                        #     print staff_name

        with open(save_dict_file, "w") as file_handle:
            file_handle.writelines([temp_staff + u"\n" for temp_staff in staff_name_set])
        cursor.close()
        return True
    except Exception, e:
        print e
        return False


if __name__ == "__main__":
    ISOTIMEFORMAT = '%H'
    has_done = False
    flag = get_staff_name()
    logger.info(u"启动先进行首次词库更新:{msg}".format(msg = u"成功" if flag else u"失败"))
    logger.info("IK 词库更新程序上线 starting")
    while True:

        now_hour = int(time.strftime(ISOTIMEFORMAT, time.localtime()))
        if now_hour == 2:
            if not has_done:
                time.sleep(1 * 60)
                flag = get_staff_name()
                logger.info(u"现在是{hour}点,本次词库更新:{msg}".format(hour=now_hour, msg = u"成功" if flag else u"失败"))
                if flag:
                    has_done = True

            else:
                pass
                time.sleep(10 * 60)
        else:
            time.sleep(10 * 60)
            logger.info("sleep 10 min")
            has_done = False

