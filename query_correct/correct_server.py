# coding: utf8
import json
import logging
import os
import re
import sys
import time
import urllib
from tornado import gen
import tornado.web
import tornado.ioloop
from tornado.options import options, define
from corrector import Corrector

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))
from config.conf import LOGGING_FORMAT, DEBUG

define('debug', default=DEBUG, help='enable debug mode')
define('port', default=9080, help='run on this port', type=int)

class CorrectHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, **kwargs):
        super(CorrectHandler, self).__init__(application, request, **kwargs)
        self.corrector = Corrector()

    def get(self):
        ret_data = {'correct_result': []}
        try:
            query = self.get_argument('query', '')
            logging.info('Starting query: %s, port: %s', query, options.port)
            if not query:
                pass
            else:
                query = urllib.unquote(query)
                if (re.match(r'^[a-zA-Z0-9]+$', query) and len(query) <= 20) or len(query) <= 8:
                    correct_result = self.corrector.get_correct_words(query) 
                    candidate = correct_result['candidate']
                    logging.info(u'query: {query}, status: {status}, msg: {msg}, candidate: {candidate}'\
                        .format(query = query,
                            status = correct_result['status'],
                            msg = correct_result['msg'],
                            candidate = u','.join(candidate)))
                    ret_data['correct_result'] = candidate
                else:
                    ret_data['correct_result'] = []
        except Exception, e:
            ret_data['error_code'] = -1
            ret_data['error_msg'] = str(e)
            logging.error(e)
        finally:
            [h_weak_ref().flush() for h_weak_ref in logging._handlerList]
            self.write(json.dumps(ret_data)) 

def make_app():
    settings = {
        'debug': options.debug,
        'gzip': True
    }
    return tornado.web.Application([
        (r'/query_correct', CorrectHandler),
    ], **settings)


if __name__ == '__main__':
    logging_level = logging.DEBUG if options.debug else logging.INFO

    root = logging.getLogger()
    root.setLevel(logging_level)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging_level)
    ch.setFormatter(logging.Formatter(LOGGING_FORMAT))

    eh = logging.StreamHandler(sys.stderr)
    eh.setLevel(logging.WARNING)
    eh.setFormatter(logging.Formatter(LOGGING_FORMAT))

    root.addHandler(ch)
    root.addHandler(eh)

    tornado.options.parse_command_line()
    app = make_app()
    app.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

