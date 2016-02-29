class Producer:
    def __init__(self, queue, db, data_provider):
        self._provider = data_provider
        self._queue = queue
        self._db = db

    def create_job(self, table, primary_key, column, action):
        cursor = self._db.execute_query('SELECT {} from {}'.format(primary_key, table), ())
        random_data = getattr(self._provider, action)
        for row in cursor:
            self._queue.put((table, column, primary_key, random_data(), row[0]))
        cursor.close()
