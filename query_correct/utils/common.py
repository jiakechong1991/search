# coding: utf8
from lang_conf import *

def in_range(c, ranges):
    for cur_range in ranges:
        if c >= cur_range[0] and c <= cur_range[1]:
            return True
    return False

def not_in_range(c, ranges):
    return not in_range(c, ranges)

def all_in_range(chars, ranges):
    """ chars里所有的字符都在ranges里 """
    for c in chars:
        if c != ' ' and not_in_range(c, ranges):
            return False
    return True

def has_in_range(chars, ranges):
    """ chars里有字符在ranges里"""
    for c in chars:
        if in_range(c, ranges):
            return True
    return False

def all_in_list(chars, lst):
    for c in chars:
        if c not in lst:
            return False
    return True


def replace_pos(chars, pos, replace_c):
    """ 把chars在pos处的字符换成replace_c[长度可以不为1] """
    result = chars[:pos] + replace_c + chars[pos+1:]
    return result

def replace_poses(chars, poses, replace_c):
    """ 对于poses里的每一个pos，把word该位置的字符换成replace_c """
    new_chars = ''
    poses.insert(0, -1)
    poses.sort()
    for i in range(len(poses) - 1):
        beg, end = poses[i], poses[i+1]
        new_chars += chars[beg+1:end] + replace_c
    new_chars += chars[poses[-1]+1:]
    return new_chars

def discard_all_digits(chars):
    digit_poses = []
    for i, c in enumerate(chars):
        if in_range(c, digit_range):
            digit_poses.append(i)
    new_chars = replace_poses(chars, digit_poses, '')
    return new_chars


def get_lcs(stra, strb):
    """ 
        获取stra, strb的最长公共子序列[不一定连续]
        如：str1 = u'我爱北京天安门', str2 = u'我在公司北门'
            lcs = u'我北门'
    """
    la = len(stra)
    lb = len(strb)
    c = [[0]*(lb+1) for i in range(la+1)]
    for i in range(la-1,-1,-1):
        for j in range(lb-1,-1,-1):
            if stra[i] == strb[j]:
                c[i][j] = c[i+1][j+1] + 1 
            else:
                c[i][j] = max({c[i+1][j],c[i][j+1]})
    i,j = 0,0 
    ret = ''
    while i < la and j < lb: 
        if stra[i] == strb[j]:
            ret += stra[i]
            i += 1
            j += 1
        elif c[i+1][j] >= c[i][j+1] :
            i += 1
        else:
            j += 1
    return ret 

def get_remain_sequence(str1, str2, included=False):
    """ 
        str1 和 str2 除去最长公共子序列之后剩下的序列 
        如：str1 = u'我爱北京天安门', str2 = u'我在公司北门'
        remain_sequence = u'爱京天安在公司'
        其中：included表示，str1是否是str2的一个子序列
    """
    result = ''
    if not included:
        lcs_result = get_lcs(str1, str2)
        result += get_remain_sequence(lcs_result, str1, True)
        result += get_remain_sequence(lcs_result, str2, True)
        return result
    else:
        if not str1:
            return str2
        lcs_result = str1
        lcs_index = 0
        for i, s in enumerate(str2):
            if s != lcs_result[lcs_index]:
                result += s
            else:
                lcs_index += 1
                if lcs_index == len(lcs_result):
                    result += str2[i+1:]
                    break
        return result
    

def get_chinese_sequence(word):
    result = ''
    for c in word:
        if in_range(c, chinese_range):
            result += c
    return result

def get_row_num(filepath):
    count = -1
    for count, line in enumerate(open(filepath, 'rU')):
        pass
    count += 1
    return count

