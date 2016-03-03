class Consumer:
    bulk_update_query = 'update {} set {} = case {} end where {} in %s'

    def __init__(self, queue, db):
        self._counter = 0
        self._queue = queue
        self._db = db

    def consume(self):
        while True:
            job = self._queue.get()
            self._run(job)
            self._queue.task_done()

    def _run(self, job):
        when_query = []
        values = []
        p_keys = []
        mtable = None
        mcolumn = None
        mprimary = None
        if len(job):
            print ('Got job')
            for ele in job:
                table, column, primary_key, random_value, action, primary_key_id = ele
                if action != 'random_int':
                    random_value = str(random_value)
                if isinstance(random_value, str) and (action == 'email' or column == 'screenname'):
                    random_value = str(self._counter) + random_value
                self._counter += 1
                p_keys.append(str(primary_key_id))
                mtable = table
                mcolumn = column
                mprimary = primary_key
                when_query.append('when {} = %s then %s'.format(primary_key))
                values.append(primary_key_id)
                values.append(random_value)
            values.append(p_keys)
            print('On Job ' + mtable + ' ' + mcolumn)
            try:
                cursor = self._db.execute_query(
                    self.bulk_update_query.format(mtable, mcolumn, ' '.join(when_query), mprimary),
                    values)
                cursor.commit()
                cursor.close()
                print('Finished Job ' + mtable + ' ' + mcolumn)
                with open('./output.txt', 'a') as target:
                    target.write(mtable + ':' + mcolumn + '\n')

            except Exception as e:
                raise e
                print("Exception {} for table {} and column {}".format(e, mtable, mcolumn))
