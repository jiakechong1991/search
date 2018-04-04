#!/usr/bin/python
#coding:utf8
import sys,json,re
reload(sys)
sys.setdefaultencoding('utf-8')
import datetime
import re

pattern_new = re.compile(r'course-v1:(.+)\+(.+)\+(.+)')
pattern_old = re.compile(r'(.+)\/(.+)\/(.+)')
pattern_part = re.compile(r'.+_(\d)X.*')

############################################################
def get_err_ret(dic_param, err_code, err_msg):
    ret_data = {
            'param': dic_param,
            'total': {'all':0,'course':0,'knowledge':0},
            'data': [],
            'error_code': int(err_code),
            'error_msg': str(err_msg)
        }
    return ret_data

############################################################


def get_ok_ret(dic_param, lst_data, n_all, n_course, n_knowledge):
    # 填充最后的结构体
    ret_data = {
            'param': dic_param,
            'total': {
                    'all':n_all,
                    'course':n_course,
                    'knowledge':n_knowledge
                },
            'data': lst_data,  # 核心的数据字段
            'error_code': 0,
            'error_msg': ''
        }
    return ret_data


def get_original_course_num(course_id):
    # INPUT: course_id
    # OUTPUT: original course num, part # of series courses(0 if not a series course)
    match = pattern_new.match(course_id)
    if not match:
        match = pattern_old.match(course_id)
    if not match:
        return None, None
    org = match.group(1).lower()
    course_num = match.group(2).upper()
    run = match.group(3)
#    print org, course_num, run
#    print match.group(1), match.group(2), match.group(3)
    p = -1
    part = '0'
    if org == 'tsinghuax':
        px = course_num.find('X')
        px_ = course_num.find('X_')
        p_ = course_num.find('_')
        match = pattern_part.match(course_num)
        if match:
            part = match.group(1)
        if px > 0 and (px == len(course_num) - 1 or px == px_):
            p = px
            if p_ > 0 and p_ < px:
                p = p_
        elif p_ > 0:
            p = p_
        if p > 0:
            rt_course_num = course_num[0:p]
            return rt_course_num, part
        else:
            return course_num, part
    elif org == 'mitx' or org == 'mit' or org == 'uc_berkeleyx' or org == 'nthu':
        course_num = course_num.replace('_', '.')
        p = course_num.find('X')
        if p > 0:
            rt_course_num = course_num[0:p]
            return rt_course_num, part
    else:
        px = course_num.find('X')
        px_ = course_num.find('X_')
        p_ = course_num.find('_')
        match = pattern_part.match(course_num)
        if match:
            part = match.group(1)
        if px > 0 and (px == len(course_num) - 1 or px == px_):
            p = px
            if p_ > 0 and p_ < px:
                p = p_
        elif p_ > 0:
            p = p_
        if p > 0:
            rt_course_num = course_num[0:p]
            return rt_course_num, part
        else:
            return course_num, part
    return course_num, part

