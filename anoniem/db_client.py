import MySQLdb


class DbClient:
    def __init__(self, host, port, user, password, db):
        self._db = MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=db)
        self._db.autocommit(True)

    def execute_query(self, query, values):
        cursor = self._db.cursor()
        cursor.execute(query, values)
        return cursor
