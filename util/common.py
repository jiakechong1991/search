#coding:utf8
import sys, os, json
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))


def cut_long_title(course_name, max_len=50):
    course_name = course_name.decode('utf8')
    n_len = len(course_name)
    if n_len <= max_len:
        return (0, course_name)
    n_cur_len = n_len
    arr_word = course_name.split(' ')
    if len(arr_word) < 5: #chinese characters
        course_name = course_name[0:max_len-1]
    else: #english words
        for i in range(len(arr_word)-1, 0, -1):
            n_cur_len -= (len(arr_word[i]) + 1)
            if n_cur_len <= max_len:
                break
        arr_word_rst = arr_word[0:i]
        course_name = ' '.join(arr_word_rst)
    return (1, course_name)

def cut_long_word(word, max_len=50):
    """ 和cut_long_title差不多，就是换个名字，便于理解 """
    word = word.decode('utf8')
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

def get_row_num(filepath):
    count = -1
    for count, line in enumerate(open(filepath, 'rU')):
        pass
    count += 1
    return count

def get_step(all_num, digit_num=2):
    step = 1 
    for i in range(len(str(all_num)) - digit_num): 
        step *= 10
    return step

