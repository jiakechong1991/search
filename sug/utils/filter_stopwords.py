#!/usr/bin/python
#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class FilterWords():
    set_stopwords = set([])
    def init(self, file_in):
        try:
            fin = open(file_in)
        except:
            print "Fail to open file:%s"%file_in
            return
        line = fin.readline()
        while line:
            line = line.strip()
            line = line.decode('utf8')
            if line in self.set_stopwords:
                #print "exist:%s"%line
                pass
            else:
                self.set_stopwords.add(line)
            line = fin.readline()
        fin.close()
        print "num_stopwords:%d"%len(self.set_stopwords)

    def filter_stopwords(self, line):
        list_words = line.split(" ")
        list_rst = []
        for word in list_words:
            #print word
            word = word.decode('utf8')
            #filter single character
            if len(word) < 2:
                continue
            if word in self.set_stopwords:
                continue
            list_rst.append(word)
        return " ".join(list_rst)

    def filter_stopwords_lst(self, lst_fields):
        list_rst = []
        for word in lst_fields:
            #print word
            word_temp = word.decode('utf8')
            #print word_temp, len(word_temp)
            #filter single character
            if len(word_temp) < 2:
                continue
            if word_temp in self.set_stopwords:
                continue
            list_rst.append(word)
        return list_rst

    def ProcessFile(self, file_in, file_ot, filterstopwords=True):
        fin = open(file_in)
        fot = open(file_ot, "w")
        n = 0
        for line in fin:
            line = line.strip('\r\n')
            if 0 == len(line):
                continue
            fields = line.split('\t')
            field_wordseg = fields[-1]
            n += 1
            lst_out = fields[:-1]
            if filterstopwords:
                fields[-1] = self.filter_stopwords(field_wordseg)
            lst_out.append(fields[-1])
            fot.write('\t'.join(lst_out)+"\n")
        fot.close()
        fin.close()

def print_usage(program=""):
    print "Usage:"
    print "    python %s file_stopwords file_in file_out"%sys.argv[0]

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print_usage()
        sys.exit(1)
    obj = FilterWords()
    obj.init(sys.argv[1])
    obj.ProcessFile(sys.argv[2], sys.argv[3])
