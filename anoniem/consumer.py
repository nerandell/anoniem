class Consumer:
    def __init__(self, queue, db):
        self._queue = queue
        self._db = db

    def consume(self):
        while True:
            job = self._queue.get()
            self._run(job)
            self._queue.task_done()

    def _run(self, job):
        table, column, primary_key, random_value, primary_key_id = job
        cursor = self._db.execute_query('UPDATE {} SET {} = %s WHERE {} = %s'.format(table, column, primary_key),
                                        (random_value, primary_key_id))
        cursor.close()
