# -*- coding: utf-8 -*-
"""改程序旨在课程管理系统的kafka写收是否一致"""

import time
import datetime
import email
import smtplib
import os
import MySQLdb
from logging import config, getLogger
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
DEF_CUR_FILE_PATH = os.path.split(os.path.realpath(__file__))[0]
sys.path.append(DEF_CUR_FILE_PATH)
config.fileConfig('{0}/logging.conf'.format(DEF_CUR_FILE_PATH))
logger_ = getLogger('search')
log_control = {
    "last_time": time.time(),
    "has_done": False
}


def log_spedd_control(second):
    """每10秒内，无论调用多少次，仅有一次返回True"""
    flag = False
    if time.time() - log_control["last_time"] >= second:
        if not log_control["has_done"]:
            flag = True
            log_control["has_done"] = True
            log_control["last_time"] = time.time()
    else:
        log_control["has_done"] = False
    return flag


class Mailer:
    def __init__(self, smtp_host, smtp_user, smtp_passwd, smtp_port = 25) :
        self.smtp_host = smtp_host
        self.smtp_user = smtp_user
        self.smtp_passwd = smtp_passwd
        self.smtp_port = smtp_port
        self.mail = email.MIMEMultipart.MIMEMultipart('related')
        self.alter = email.MIMEMultipart.MIMEMultipart('alternative')
        self.mail.attach(self.alter)
        self.attachments = []

    def mailfrom(self, mail_from) :
        self._from = mail_from
        self.mail['from'] = mail_from

    def mailto(self, mail_to) :
        """
        mail_to : comma separated emails
        """
        self._to = mail_to
        if type(mail_to) == list:
            self.mail['to'] = ','.join(mail_to)
        elif type(mail_to) == str :
            self.mail['to'] = mail_to
        else:
            raise Exception('invalid mail to')

    def mailsubject(self, mail_subject):
        self.mail['subject'] = mail_subject

    def text_body(self, body, encoding='utf-8'):
        self.alter.attach(email.MIMEText.MIMEText(body, 'plain', encoding))

    def html_body(self, body, encoding='utf-8'):
        self.alter.attach(email.MIMEText.MIMEText(body, 'html', encoding))

    def addattach(self, filepath, mime_type='octect-stream', rename=None):
        import os
        f = open(filepath, 'rb')
        filecontent = f.read()
        f.close()
        mb = email.MIMEBase.MIMEBase('application', mime_type)
        mb.set_payload(filecontent)
        email.Encoders.encode_base64(mb)
        fn = os.path.basename(filepath)
        mb.add_header('Content-Disposition', 'attachment', filename = rename or fn)
        self.mail.attach(mb)

    def send(self):
        self.mail['Date'] = email.Utils.formatdate( )
        smtp = False
        try:
            smtp = smtplib.SMTP()
            smtp.set_debuglevel(0)
            smtp.connect(self.smtp_host, self.smtp_port)
            smtp.login(self.smtp_user, self.smtp_passwd)
            smtp.sendmail(self._from, self._to, self.mail.as_string())
            return  True
        except Exception, e:
            import traceback
            print traceback.format_exc()
            return False
        smtp and smtp.quit()


def get_mailer():
    # 邮件服务器地址smtp.xuetangx.com
    mailer = Mailer('smtp.xuetangx.com', 'bigdata@xuetangx.com', 'bigdata123')
    mailer.mailfrom('bigdata@xuetangx.com')
    return mailer


def send_mail(content):

    if content:
        # 发送邮件
        mailer = get_mailer()
        mailto = [
            'zhuhaijun@xuetangx.com',
            'wangxiaoke@xuetangx.com',
            'wangchenkang@xuetangx.com'
        ]  #
        body = content
        title = unicode("教学系统kafka收发 检查:", "utf8")
        mailer.mailsubject(title)
        mailer.mailto(mailto)
        mailer.html_body(body)
        mailer.send()
        print("邮件已发送")
    else:
        print('文件为空不发邮件')


def read_mysql(this_day):

    def mysql_connection():
        mysql_host = 'tap-authority.xuetangx.info'
        mysql_user = "course"
        mysql_password = "course@xuetangx"
        conn_mysql = MySQLdb.connect(*[mysql_host, mysql_user, mysql_password, 'course_manage'], charset='utf8',
                                     use_unicode=True)
        return conn_mysql
    MYSQL_IN = mysql_connection()
    start_day = datetime.datetime.strptime(this_day, '%Y-%m-%d')
    end_day = (start_day+datetime.timedelta(1))
    sql_txt = """SELECT course_id 
                 FROM course_field_change_record 
                 WHERE 
                    name = 'parent_id' AND update_time >='{s}'
                    AND update_time <'{e}';""".format(s=start_day, e=end_day)
    try:
        MYSQL_IN.ping(True),
    except Exception, e:
        logger_.info(e)
        assert True == False, u"读取TAP mysql ping fail"

    cor_ = MYSQL_IN.cursor()
    cor_.execute(sql_txt)
    res = cor_.fetchall()
    res = [item[0] for item in res]
    cor_.close()
    MYSQL_IN.close()
    return res


def follow(file_):
    file_.seek(0, 2)
    while True:
        line = file_.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line


def process_msg(msg):
    timestamp = "{y}-{m}-{d}".format(
        y=datetime.datetime.now().strftime("%Y"), m=msg[0:2], d=msg[3:5])
    try:
        start_day = datetime.datetime.strptime(timestamp, '%Y-%m-%d')
    except Exception,e:
        timestamp = "error timestamp"
    if "tap" in msg and "parent_cur" in msg:
        course = msg.split(" ")[-3].replace("course:", "")
    else:
        course = "not tap vaild msg"
    return timestamp, course


if __name__ == '__main__':
    try:
        file_name = "/var/log/supervisor/search/consumer_courseid_new.log"
        logfile = open(file_name, "r")
        loglines = follow(logfile)
        day_change = False
        tap_msg_list = []
        last_msg_date = None
        first_day = True
        for line in loglines:
            timestamp_, course_ = process_msg(line)
            if timestamp_ == "error timestamp":
                logger_.info("不能解析的行:{l}".format(l=line))
                continue
            if not last_msg_date:
                last_msg_date = timestamp_
                logger_.info("初始化日期{d}".format(d=last_msg_date))
            if log_spedd_control(60*30):
                logger_.info("t={t}  course={c} ".format(t=timestamp_, c=course_))

            if timestamp_ != last_msg_date:
                day_change = True
            if day_change:
                today_receive_sum = len(tap_msg_list)
                mysql_msg_list = read_mysql(last_msg_date)
                today_send_sum = len(mysql_msg_list)
                if today_receive_sum != today_send_sum:
                    msg_ = "{today} kafka收发数量不一致,send{s}条，receive{r}条，明细见日志".format(
                        today=last_msg_date, s=today_send_sum, r=today_receive_sum)
                    logger_.info(
                        "今天mysql中的course_list:{a},日志中的course_list:{b}".format(a=tap_msg_list, b=mysql_msg_list))

                else:
                    msg_ = "{today} kafka收发数量一致,一共{a}条".format(today=last_msg_date, a=today_receive_sum)
                if first_day:
                    msg_ = "监控从{d}半天中途开始，本天不计入监控，从下一天开始".format(d=last_msg_date)
                    first_day = False
                del tap_msg_list[:]
                day_change = False
                logger_.info(msg_)
                send_mail(msg_)

            last_msg_date = timestamp_
            if course_ != "not tap vaild msg":
                tap_msg_list.append(line)
    except Exception, e:
        logfile.close()
        logger_.info("====")
        logger_.info(e)
        logger_.info("---")
        msg_ = "tap_kafka 读写监控崩溃,请查看日志"
        logger_.info(msg_)
        send_mail(msg_)
        assert True == False, msg_


