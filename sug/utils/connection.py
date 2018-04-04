import contextlib
import functools
import MySQLdb
import MySQLdb.cursors

from config.db_conf import MYSQL_DB

def mysql_connection(db):
    mysql = MYSQL_DB[db]
    conn = MySQLdb.connect(
        host=mysql['host'],
        user=mysql['user'],
        passwd=mysql['passwd'],
        db=db,
        charset='utf8',
        use_unicode=True,
        cursorclass=MySQLdb.cursors.DictCursor,
        connect_timeout=60)
    return conn


def with_mysql_cursor(db):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            with contextlib.closing(mysql_connection(db)) as connection:
                with contextlib.closing(connection.cursor()) as cursor:
                    return func(self, cursor, *args, **kwargs)
        return wrapper
    return decorator
