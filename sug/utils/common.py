# coding: utf8

def get_row_num(filepath):
    """ 获取文件行数 """
    count = 0
    for count, line in enumerate(open(filepath, 'rU')):
        pass
    return count + 1


def strQ2B(data):
    """ 全角转半角 """
    rstring = ""
    for uchar in data:
        inside_code = ord(uchar)
        # 全角空格直接转换
        if inside_code == 12288:
            inside_code = 32
        # 全角字符（除空格）根据关系转化
        elif (inside_code >= 65281) and (inside_code <= 65374):
            inside_code -= 65248

        rstring += unichr(inside_code)

    data = rstring
    return data


def cut_long_word(word, max_len=50):
    """ 和cut_long_title差不多，就是换个名字，便于理解 """
    n_len = len(word)
    if n_len <= max_len:
        return (0, word)
    n_cur_len = n_len
    arr_word = word.split(' ')
    if len(arr_word) < 5: #chinese characters
        word = word[0:max_len-1]
    else: #english words
        for i in range(len(arr_word)-1, 0, -1):
            n_cur_len -= (len(arr_word[i]) + 1)
            if n_cur_len <= max_len:
                break
        arr_word_rst = arr_word[0:i]
        word = ' '.join(arr_word_rst)
    return (1, word)

def is_start_with_ordinal_num(data):
    """ 是否为序数词开头 """
    import re
    pat_ordinal = re.compile(u'第[一二三四五六七八九十零1234567890]')
    if None == pat_ordinal.match(data):
        return False
    return True

