# coding: utf8

DEBUG = False

INPUT_MIN_LEN = 2
INPUT_MAX_LEN = 50

## ES CONFIG
ES_HOSTS_MAP = {
    'debug': ['192.168.9.118'],
    'production': ['10.0.2.151', '10.0.2.152', '10.0.2.153', '10.0.2.154', '10.0.2.155']
}
ES_HOSTS = ES_HOSTS_MAP['debug'] if DEBUG else ES_HOSTS_MAP['production']

ES_INDEX = 'qc-index'

## LOGGING CONFIG
LOGGING_FORMAT = '%(asctime)s %(name)s %(module)s %(lineno)d %(levelname)s %(message)s'

