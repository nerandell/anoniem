class Consumer:
    bulk_update_query = 'update {} set {} = case {} end where {} in {}'

    def __init__(self, queue, db, file):
        self._queue = queue
        self._db = db
        self._file = file
        self.target = open(self._file, 'a')

    def consume(self):
        while True:
            job = self._queue.get()
            self._run(job)
            self._queue.task_done()

    def _run(self, job):
        when_query = []
        values = ()
        p_keys = []
        mtable = None
        mcolumn = None
        mprimary = None
        if len(job):
            p_keys = []
            print ('Got job')
            for ele in job:
                table, column, primary_key, random_value, primary_key_id = ele
                mtable = table
                mcolumn = column
                mprimary = primary_key
                p_keys.append(str(primary_key_id))
                when_query.append('when {} = {} then \'{}\''.format(primary_key, primary_key_id, random_value))
                # values = values + (primary_key_id, random_value)
            # values = values + (p_keys,)
            print('On Job', mtable, mcolumn)
            try:
                query = self.bulk_update_query.format(mtable, mcolumn, ' '.join(when_query), mprimary,
                                                      '(' + ','.join(p_keys) + ')')
                print ('Writing query to file')
                self.target.write(query + ';\n')
                # cursor = self._db.execute_query(self.bulk_update_query.format(mtable, mcolumn, when_query, mprimary), values)
                # cursor.close()
                print('Finished Job', mtable, mcolumn)
            except Exception as e:
                raise e
