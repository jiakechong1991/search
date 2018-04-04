#-*- coding=utf-8 -*-
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.autoreload
import tornado.web
from tornado.options import define, options
from searching import get_result
import urllib
import json
import traceback

define("port", default=9996, help="run on the given port", type=int)


class RelatedSearchHandler(tornado.web.RequestHandler):

    def to_int(self, num, default):
        if num == "":
            return default
        else:
            return int(num)

    def to_list(self, s):
        if s == "":
            return None
        else:
            return s.split(";")

    def to_str(self, s):
        if s == "":
            return None
        else:
            return str(s)

    def get(self):
        try:
            query = self.get_argument("query", "")
            # init args
            if query == "":
                pass
            else:
                query = urllib.unquote(query).encode('utf-8')
            num = self.get_argument("num", 8)
            num = self.to_int(num, 8)
            assert num <= 8, "请 num<=8"
            result = get_result(query, num)
            self.write({'data': result, 'error_code': 0, 'error_msg': ''})
        except Exception, e:
            print traceback.print_exc()
            result = {'data': [], 'error_code': -1, 'error_msg': str(e)}
            self.write(json.dumps(result))


class Home(tornado.web.RequestHandler):
    def get(self):
        self.write("this is related_search")

if __name__ == "__main__":
    print "tornado启动"
    tornado.options.parse_command_line()
    settings = dict(gzip=True)
    app = tornado.web.Application(
        handlers=[
            (r"/related_search", RelatedSearchHandler),
            (r"/home", Home)
        ], **settings
    )
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
