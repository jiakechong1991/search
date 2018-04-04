# -*- coding=utf-8 -*-

import time
import MySQLdb
import os
from logging import config, getLogger
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
DEF_CUR_FILE_PATH = os.path.split(os.path.realpath(__file__))[0]
config.fileConfig('{0}/logging.conf'.format(DEF_CUR_FILE_PATH))
logger_ = getLogger('search')
sql_db = {
    "conn": None,
}


def mysql_connection():
    mysql_host = "datamysql.xuetangx.info"
    mysql_user = "mysql_ro"
    mysql_password = "xuetangx.com168mysql"
    if sql_db["conn"] == None:
        sql_db["conn"] = MySQLdb.connect(
            *[mysql_host, mysql_user, mysql_password, 'edxapp'], charset='utf8', use_unicode=True)
    else:
        try:
            logger_.info(sql_db["conn"].ping(True))
        except Exception, e:
            logger_.info("出现错误{e},要重建连接了".format(e=e))
            sql_db["conn"] = MySQLdb.connect(
                *[mysql_host, mysql_user, mysql_password, 'edxapp'], charset='utf8', use_unicode=True)

    return sql_db["conn"]


def func():
    logger_.info("----"*10)
    sql_label_text = "select id, status, course_id from course_meta_course  where id <=3"
    conn = mysql_connection()
    cursor = conn.cursor()
    cursor.execute(sql_label_text)
    res = cursor.fetchall()
    for i in res:
        logger_.info(i)


if __name__ == "__main__":
    while True:
        try:
            func()
            time.sleep(60*60*10)
        except Exception, e:
            logger_.info(e)



