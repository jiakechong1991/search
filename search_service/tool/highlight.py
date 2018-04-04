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
        key = key.rstrip()
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
        node.len = len(key)

    def search(self, key):
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
        self.lst_seg_query = []

    # 设置分词token列表
    def set_seg_query(self, lst_query):
        self.lst_seg_query = lst_query

    def _get_max_window(self, lst_len):
        '''
        n_cur_max累加时用词长的立方，提高长词的权重。
        ...考量放到科技<span class="keyword">创</span>新中去的方式讨论负责任的<span class="keyword">创</span>新这一概念的含义和意义。在本课中，我们将：探讨负责任的<span class="keyword">创</span>新的概念，有关<span class="keyword">创</span>...
        ...业，和对学业相关的关键问题感兴趣的遵循传统科技课程的学生。特许：本课的材料来自于带尔夫工业大学版权并得到了荷兰<span class="keyword">创造</span>...
        '''

        n_len = 0
        n_max = 0
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
        u_text = str_text.decode('utf8')
        u_lower = u_text.lower()
        i = 0
        n_len = len(u_text)
        lst_len = []
        while i < n_len:
            n = objTrie.search(u_lower[i:])
            if 0 == n:
                #n = 1
                word = u_text[i:i+1]
                i += 1
            else:
                word = self.tag1 + u_text[i:i+n] + self.tag2
                # print 'word:',word
                i += n
            lst_text.append(word)
            lst_len.append(n)
        # print 'len(lst_len)=',len(lst_len)
        # print lst_len
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
    lst_query = ['创业']
    obj.set_seg_query(lst_query)
    str_abst = obj.abstract('艺术创造美,创业的现象,创和业的结。')
    print str_abst
