# coding:utf8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class TrieCommonNode(object):
    def __init__(self):
        self.len = None
        self.children = {}


class TrieCommon(object):
    def __init__(self):
        self.root = TrieCommonNode()

    def add(self, key):
        key = key.rstrip()  # 删除空格
        if 0 == len(key):
            return
        node = self.root
        key=key.decode("utf8","ignore")
        for char in key:
            if char not in node.children:
                child = TrieCommonNode()
                node.children[char] = child
                node = child
            else:
                node = node.children[char]
        node.len = len(key)  # 在叶子节点上记录这个单词的长度信息

    def search(self, key):
        # 获取这个key在trie树中的长度
        key = key.decode('utf8', 'ignore')
        node = self.root
        nLen = 0
        for char in key:
            if char not in node.children:
                break
            node = node.children[char]
            if node.len:
                nLen=node.len
        return nLen
############################################################

class Highlighting(object):
    def __init__(self,
            w=56, #20150810 changed by zhuhaijun
            tag1='<span class="keyword">',
            tag2='</span>'):
        self.__dict__.update(locals())
        del self.__dict__['self']
        self.lst_seg_query = []  #

    # 设置分词token列表
    def set_seg_query(self, lst_query):
        self.lst_seg_query = lst_query  # 要高亮的短语

    def _get_max_window(self, lst_len):
        # '''n_cur_max累加时用词长的立方，提高长词的权重'''
        n_len = 0
        n_max = 0  # 权重
        max_i = 0
        max_j = 0
        for i in range(len(lst_len)):
            # print 'i=',i
            if 0 == lst_len[i]:
                n_len += 1
            else:
                n_len += lst_len[i]
            if lst_len[i] > 0:
                n_max += lst_len[i]*lst_len[i]*lst_len[i]
            if n_len >= self.w:
                break
        max_j = i+1
        i = 0
        n_cur_len = n_len
        n_cur_max = n_max
        for j in range(max_j, len(lst_len)):
            n_tmp = lst_len[i]
            if 0 == lst_len[i]:
                n_tmp = 1
            n_cur_len -= n_tmp
            n_tmp = lst_len[j]
            if 0 == lst_len[j]:
                n_tmp = 1
            n_cur_len += n_tmp
            if lst_len[i] > 0:
                n_cur_max -= lst_len[i]*lst_len[i]*lst_len[i]
            if lst_len[j] > 0:
                # n_cur_max += 1
                n_cur_max += lst_len[j]*lst_len[j]*lst_len[j]
            if n_cur_len >= self.w and n_cur_max > n_max:
                max_i = i + 1
                max_j = j
                n_max = n_cur_max
            i += 1

        return (max_i, max_j)

    '''
    20150805 add lower case converter
    eg:Jeff Jorge
    '''
    def abstract(self, str_text):
        # 真正的高亮字符串匹配算法
        if str_text == None:
            str_text = u""
        lst_word = self.lst_seg_query
        if 0 == len(lst_word) or len(str_text) == 0:
            return str_text

        objTrie = TrieCommon()
        for word in lst_word:
            objTrie.add(word.decode('utf8'))
        lst_text = []
        # 编解码，转小写
        u_text = str_text.decode('utf8')
        u_lower = u_text.lower()
        i = 0
        n_len = len(u_text)  # 输入字符串的长度
        lst_len = []
        while i < n_len:
            # print u_lower[i:], "++++"
            n = objTrie.search(u_lower[i:])
            if 0 == n:
                #n = 1
                word = u_text[i:i+1]
                i += 1
            else:
                word = self.tag1 + u_text[i:i+n] + self.tag2
                # print 'word:',word
                i += n
            # print word, "--"
            lst_text.append(word)  # 这里面是所有的单字(或者是被高亮标签表中的字)
            lst_len.append(n)
        # print 'len(lst_len)=',len(lst_len)
        print lst_len
        if n_len <= self.w:
            str_abst = ''.join(lst_text)
        else:
            i,j = self._get_max_window(lst_len)
            str_abst = ''.join(lst_text[i:j+1])
            if 0 == i:
                str_abst = str_abst + '...'
            elif n_len == j+1:
                str_abst = '...' + str_abst
            else:
                str_abst = '...' + str_abst + '...'
        return str_abst

if __name__ == '__main__':
    obj = Highlighting(w=56)
    lst_query = ['创', "乐山大佛"]
    obj.set_seg_query(lst_query)
    str_abst = obj.abstract("""收到了法拉是打飞机啊；乐山大佛；按理说地方；阿拉山口多发了；束带结发；
    埃里克森的分解卡萨丁法律上看到房价啦稍等盛看见对方拉克丝大姐夫拉开
    束带结发老师看到交付艺术创造美,创业的现象,创和业的结。""")
    print str_abst
