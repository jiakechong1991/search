# -*- coding: utf-8 -*-
import sys
import jieba
import jieba.posseg
import jieba.analyse
import os
from copy import deepcopy
reload(sys)
sys.setdefaultencoding('utf-8')

ss = u"网络安全"
seg_list = jieba.cut(ss, cut_all=True)

print("全模式--: " + "/ ".join(seg_list))  # 全模式

seg_list = jieba.cut(ss, cut_all=False)
print("精确模式--: " + "/ ".join(seg_list))  # 精确模式

seg_list = jieba.cut_for_search(ss)  # 搜索引擎模式
print("搜索引擎模式--:" + "/ ".join(seg_list))
print "query--: {a}".format(a=ss)
