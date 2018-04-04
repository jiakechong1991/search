#-*- coding=utf-8 -*-
'''
pip install tornado
pip install elasticsearch
python sug_server.py
'''
import json
import logging
import time
import urllib
logging.getLogger('elasticsearch.trace').setLevel(logging.INFO)
logging.getLogger('elasticsearch').setLevel(logging.INFO)

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.autoreload
import tornado.web
from tornado.options import define, options

from get_suggest import *
from config.conf import DEBUG

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

define("port", default=9090, type=int)
define("debug", default=DEBUG, help="enable debug mode")
class SugHandler(tornado.web.RequestHandler):
    def to_int(self, num, default):
        if num == "":
            return default
        else:
            return int(num)

    def get(self):
        ret_data = {}
        ret_data['total'] = 0
        ret_data['data'] = []
        ret_data['error_code'] = 0
        ret_data['error_msg'] = ''
        try:
            qt = self.get_argument("qt", "")
            query = self.get_argument("query", "")
            num = self.get_argument("num", "")
            debug = self.get_argument("debug", "")

            # init args
            if query == "":
                pass
            else:
                query = urllib.unquote(query).encode('utf-8')

            num = self.to_int(num, 5)
            qt = self.to_int(qt, 1)
            debug = self.to_int(debug, 0)
            # get result
            t_beg = time.time()
            ret_data['param'] = {
                'qt': qt,
                'query': query,
                'num': num,
                'debug': debug
            }
            if not QT_TYPE_MAP.has_key(qt):
                ret_data['error_code'] = -1
                ret_data['error_msg'] = 'qt value is invalid'
            else:
                lst_ret,lst_debug = get_sug(qt=qt, query=query, num=num, debug=debug)
                t_elapse = time.time() - t_beg
                ret_data['total'] = len(lst_ret)
                ret_data['data'] = lst_ret
                ret_data['debug'] = lst_debug
                ret_data['time'] = "%.0f"%(float(t_elapse) * 1000)
        except Exception, e:
            ret_data['error_code'] = -1
            ret_data['error_msg'] = str(e)
        self.write(json.dumps(ret_data))

if __name__ == "__main__":
    tornado.options.parse_command_line()
    settings = dict(gzip=True, debug=options.debug)
    app = tornado.web.Application(
        handlers=[
            (r"/sug", SugHandler),
        ],**settings
    )
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

############################################################
#http://localhost:9090/sug?qt=1&query=a
#http://192.168.9.243:9090/sug?qt=1&query=data
