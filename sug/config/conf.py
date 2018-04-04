DEBUG=False

MIN_WORD_LEN = 2
MAX_WORD_LEN = 50
MAX_MIX_NUM = 5
MAX_USER_WEIGHT = 10000
MAX_WEIGHT = 10000

QT_TYPE_MAP = {
    1: 'platform',
    2: 'forum_user',
    3: 'course_name'
}

## ES CONFIG
ES_HOSTS_MAP = { 
    'debug': ['192.168.9.243'],
    'production': ['10.0.0.162', '10.0.0.163', '10.0.0.164']
}
ES_HOSTS = ES_HOSTS_MAP['debug'] if DEBUG else ES_HOSTS_MAP['production']

ES_INDEX = 'sug-index2'
ONLINE_ES_INDEX = 'sug-index-online'

## LOGGING CONFIG
LOGGING_FORMAT = '%(asctime)s %(name)s %(module)s %(lineno)d %(levelname)s %(message)s'
