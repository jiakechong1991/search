# coding: utf8
chinese_range = [(u'\u4e00', u'\u9fa5')]
foreign_range = [('a', 'z'), ('A', 'Z'), (u'\u00c0', u'\u024f')]    # add some latin 
digit_range = [('0', '9')]
chinese_digits = [u'一', u'二', u'三', u'四', u'五', u'六', u'七', u'八', u'九', u'十']

arabic_digits = [str(x) for x in range(0, 10)]

#拼音声母和韵母的模糊音处理
FUZZY_INITIALS=[[u"c",u"ch"], [u"s",u"sh"], [u"z",u"zh"]]
FUZZY_FINALS=[["an","ang"], ["en","eng"], ["in","ing"], ["ian","iang"], ["uan","uang"]]


