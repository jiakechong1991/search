#-*- coding: utf-8 -*-
import time
import math
import copy
import requests
import urllib2

COURSE_NAME = 10
COURSE_STAFF = 3
STRUCTURE = 2
WORD = 1
no_result_query = set()
result_query = set()

def is_chinese(uchar):
    if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
        return True
    else:
        return False

class Tree(object):
    def __init__(self):
        self.nodes = {}
        self.relation = {}
        self.doc_total = 0
        self.term_total = 0  # node 总数
        self.search_total = 0  # 可检索的节点数

    def add_node(self, word, node_type=WORD):
        self.term_total += 1
        if word in self.nodes:
            # 更新这个节点的 tem_num,只升不降的更新node_type
            self.update_node_type(self.nodes[word], node_type)
        else:
            # 添加一个新的节点
            self.nodes[word] = Node(word, node_type)
        return self.nodes.get(word, None)
    
    def add_log_node(self, word, node_type=WORD):
        self.search_total += 1
        if not word in self.nodes:
            self.nodes[word] = Node(word, node_type)
        else:
            self.nodes[word].search_num += 1

    def update_node_type(self, node, node_type):
        return node.update_node_type(node_type)

    def delete_node(self, word):
        if word in self.nodes:
            self.nodes.pop(word)
        if word in self.relation:
            self.relation.pop(word)
        for (src, dst_rel) in self.relation.items():
            if word in dst_rel:
                dst_rel.pop(word)
            if not dst_rel:
                self.relation.pop(src)

    def get_node(self, word):
        return self.nodes.get(word, None)

    def update_score(self, node, category):
        if not node:
            return
        else:
            node.update_score(category)

    def add_relation(self, src, dst, score = None):
        # input: src_node   dst_node
        if src and dst:
            if score == None:
                # 如果没有指定得分,就自行利用词语间的文本关系计算得分
                score = self.get_score(src, dst)
            if not src.word in self.relation:
                self.relation[src.word] = {dst.word: score}
            else:
                dst_relation = self.relation[src.word]
                if not dst.word in dst_relation:
                    # 本if和上面的if not 重复
                    dst_relation[dst.word] = score
                else:
                    # 有效代码
                    if score == 0:
                        # 初始化
                        dst_relation[dst.word] = score
                    else:
                        # 累计得分
                        dst_relation[dst.word] += score

    def traverse_relation(self):
        self.doc_total += 1
        # 本树的节点数
        if self.term_total < 20:
            # 如果太小,加大一点,平滑吗?
            self.term_total *= 100
        for (word, node) in self.nodes.items():
            # 本node 在本课程的出现概率
            node.tf = node.term_num / float(self.term_total)
        for (src_word, src_node) in self.nodes.items():
            for (dst_word, dst_node) in self.nodes.items():
                if src_word != dst_word:
                    # C-N-2 的两两两赋值两个词的相关性
                    self.add_relation(src_node, dst_node, 0)

    def get_score(self, src_node, dst_node):
        def edit_distance(src, dst):
            length = len(src) + 1
            width = len(dst) + 1
            array = []
            for i in range(length):
                array.append([])
                for j in range(width):
                    array[i].append(0)
            # 每行第一列 赋值
            for i in range(length):
                array[i][0] = i
            # 第一行每列 赋值
            for i in range(width):
                array[0][i] = i

            for i in range(1, length):  # 行
                for j in range(1, width):  # 列
                    t = min(array[i-1][j] + 1, array[i][j-1] + 1)
                    if src[i-1] == dst[j-1]:
                        d = 0
                    else:
                        d = 1
                    array[i][j] = min(t, array[i-1][j-1] + d)
            
            return array[length-1][width-1]
        src = src_node.word.replace(" ", "").decode("utf-8")
        dst = dst_node.word.replace(" ", "").decode("utf-8")
        ed = edit_distance(src, dst)  # 计算编辑距离
        length = max(len(src), len(dst))
        ed_sim = (length - ed) / float(length)  # 量化一个编辑距离的得分
        src_contain = 0
        dst_contain = 0
        if src in dst:
            src_contain = 1
        if dst in src:
            dst_contain = 1
        # 计算一个 基于:"编辑距离","包含关系"的文本相关的分
        sim_score = ed_sim * 0.4 + src_contain * 0.3 - dst_contain * 100

        # 计算该词 在本课程tree中的 tf-idf,以此做重要性得分
        src_score = src_node.get_score(self.doc_total) * 100
        dst_score = dst_node.get_score(self.doc_total) * 100
        # 混合这个词对的重要性得分
        rel_score = src_score * 0.2 + dst_score * 0.5
        if src_score == 0 or dst_score == 0:
            return 0
        # 混合 "词对重要性得分" &  "词对的文本相近性得分"
        return (rel_score + sim_score) / 10

    def combine(self, tree):
        self.doc_total += tree.doc_total
        doc = set()
        for (word, node) in tree.nodes.items():
            # 合并两个树的node字典
            if word in self.nodes:
                # update tf
                self.nodes[word].tf += node.tf  # 该词全部的出现次数
                self.nodes[word].doc_num += node.doc_num  # 该词在多门课中的出现次数
            else:
                # create new node
                self.nodes[word] = Node(node.word, node.node_type)
                self.nodes[word].tf = node.tf
                self.nodes[word].term_num = node.term_num
                self.nodes[word].doc_num = node.doc_num
                doc.add(word)
        # 混合两棵树的relation网络,并累加得分
        for (src, dst_rel) in tree.relation.items():
            for (dst, score) in dst_rel.items():
                if not src in self.relation:
                    self.relation[src] = {dst: score}
                else:
                    if not dst in self.relation[src]:
                        self.relation[src][dst] = score
                    else:
                        self.relation[src][dst] += score
    
    def get_related_result(self):
        for (word, node) in self.nodes.items():
            node.idf = math.log10(float(self.doc_total) / (1 + node.doc_num))
        for (src_word, src_node) in self.nodes.items():
            result = {}
            src_node.children = []
            if src_word in self.relation:
                for (dst_word, score) in self.relation[src_word].items():
                    try:
                        w = len(dst_word.decode("utf-8"))
                        if w <= 1:
                            w = 0
                        if w >= 5:
                            w = 5
                        w = math.sqrt(w)
                    except Exception, e:
                        w = 0
                        print dst_word
                    w = 1
                    score = score * self.nodes[dst_word].idf * (1 + math.log10(1 + self.nodes[dst_word].search_num)) * w  
                    try:
                        dst_word = dst_word.decode("utf-8")
                        # 判断这个词是不是中文
                        if is_chinese(dst_word[0]):
                            dst_word = dst_word.replace(" ", '').encode("utf-8")
                        else:
                            dst_word = dst_word.lower()
                    except:
                        pass
                    if dst_word in result:
                        result[dst_word] += score
                    else:
                        result[dst_word] = score
                    # 到这里,这个Word的所有相关词便利一遍,并且加入排序得分表result
                # 取出result中前10个
                for word, score in sorted(result.items(), key=lambda x: -x[1])[:10]:
                    if word in result_query:
                        src_node.children.append({"word": word, "score": score})
                    elif word in no_result_query:
                        continue
                    else:
                        if get_search_result(word):
                            src_node.children.append({"word": word, "score": score})
                            result_query.add(word)
                        else:
                            no_result_query.add(word)
            else:
                pass

    def build_relation(self):
        for (src, dst_rel) in self.relation.items():
            for (dst, score) in dst_rel.items():
                self.relation[src][dst] = self.get_score(self.nodes[src], self.nodes[dst])

class Node(object):
    def __init__(self, word, node_type=WORD):
        self.word = word
        self.children = []
        self.node_type = node_type
        self.search_num = 0
        self.term_num = 0
        self.doc_num = 0
        if node_type != WORD:
            self.term_num = 1
            self.doc_num = 1
        else:
            self.search_num = 1
        self.tf = 0
        self.idf = 0

    def get_score(self, doc_total):
        return self.node_type * self.tf / (self.doc_num + 1) / float(doc_total)
    
    def update_node_type(self, node_type):

        self.term_num += 1  # 这个NOde在本课程中的出现次数
        # node_type   只升不降
        if self.node_type < node_type:
            self.node_type = node_type
            return True
        return False

    def update_score(self, category):
        self.score += self.get_score(category)
        return self.score

    def __str__(self):
        return self.word


def get_search_result(query):
    try:
        url = "http://10.0.2.152:9999/search?query=" + urllib2.quote(query.encode("utf-8")) + "&qt=2&st=1&owner=" + urllib2.quote('xuetangX;edX')
        r = requests.get(url)
        result = r.json()
        a = get_search_score(result)
        if a:
            print query
    except:
        a = False
    finally:
        return a


def get_search_score(result):
    data = result['data']
    score = sum(map(lambda x: x.get("score", 0), data))
    if score > 300:
        return True
    else:
        return False

