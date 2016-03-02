class Consumer:
    bulk_update_query = 'update {} set {} = case {} end where {} in %s'

    def __init__(self, queue, db):
        self._queue = queue
        self._db = db

    def consume(self):
        while True:
            job = self._queue.get()
            self._run(job)
            self._queue.task_done()

    def _run(self, job):
        when_query = ''
        values = ()
        p_keys = []
        mtable = None
        mcolumn = None
        mprimary = None
        if len(job):
            for ele in job:
                table, column, primary_key, random_value, primary_key_id = ele
                p_keys.append(primary_key_id)
                mtable = table
                mcolumn = column
                mprimary = primary_key
                when_query += ' when {} = %s then %s'.format(primary_key)
                values = values + (primary_key_id, random_value)
            values = values + (p_keys,)
            print('On Job', mtable, mcolumn)
            try:
                cursor = self._db.execute_query(self.bulk_update_query.format(mtable, mcolumn, when_query, mprimary), values)
                cursor.close()
                print('Finished Job', mtable, mcolumn)
            except Exception as e:
                print(e, mtable, mcolumn)