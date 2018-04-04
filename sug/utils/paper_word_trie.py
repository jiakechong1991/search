#!/usr/bin/python
#coding:utf8
import sys, os
import datetime
reload(sys)
sys.setdefaultencoding("utf8")
import datetime, time
############################################################
class TrieNode(object):
    def __init__(self):
        self.tf = 0
        self.children = {}
        self.len = 0
############################################################
class Trie(object):
    def __init__(self):
        self.root = TrieNode()

    def add(self, key, tf=1):
        # print key, freq
        key=key.rstrip()
        if 0==len(key):
            return
        node = self.root
        # key = key.decode("utf8","ignore")
        for char in key:
            if char not in node.children:
                child = TrieNode()
                node.children[char] = child
                node = child
            else:
                node = node.children[char]
        node.tf = tf
        node.len = len(key)

    def build_dict_by_file(self, file_in):
        if not os.path.exists(file_in):
            print 'file[%s] not exist.'%file_in
            return
        print 'load file:%s'%file_in
        t_beg = datetime.datetime.now()
        f_in = open(file_in, 'r')
        num = 0
        for line in f_in:
            line = line.strip()
            if 0 == len(line):
                continue
            fields = line.split('\t')
            tf = 1
            if len(fields) == 1:
                str_word = line
            else:
                str_word = fields[0]
            str_word = str_word.decode("utf8","ignore")
            if len(str_word) < 2:
                continue
            self.add(str_word, tf)
            # print line
            num += 1
        f_in.close()
        t_end = datetime.datetime.now()
        n_elapse=(t_end - t_beg).seconds
        print 'phrase num:{0}, elapse:{1}'.format(num, n_elapse)

    # key = key.decode("utf8","ignore")
    def search(self, key):
        n_len = 0
        node = self.root
        for char in key:
            if char not in node.children:
                break
            node = node.children[char]
            n_len = node.len
        return [n_len, node.tf]

    def search_all_sub(self, key):
        n_len = 0
        node = self.root
        dic_ret = {}
        for char in key:
            if char not in node.children:
                break
            node = node.children[char]
            n_len = node.len
            if n_len > 0:
                dic_ret[n_len] = node.tf
        return dic_ret

    def get_all_match(self, str_in):
        u_in = str_in.decode("utf8","ignore")
        n_len = len(u_in)
        # print 'get_all_match:', u_in, n_len
        dic_ret = {}
        for i in range(n_len):
            u_tmp = u_in[i:n_len]
            dic_tmp = self.search_all_sub(u_tmp)
            for (n_search,tf) in dic_tmp.iteritems():
                word = u_tmp[0:n_search]
                dic_ret[word] = tf
        return dic_ret
############################################################
if __name__ == '__main__':
    trie_phrase = Trie()
    trie_phrase.add(u'测试')
    trie_phrase.add(u'试用')
    trie_phrase.add(u'用例')
    trie_phrase.add(u'测试用例')
    # dic_ret = trie_phrase.search_all_sub(u'测试用例')
    dic_ret = trie_phrase.get_all_match(u'我们的测试用例。')
    for (k,v) in dic_ret.iteritems():
        print k,v
