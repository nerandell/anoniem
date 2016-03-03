class Consumer:
    bulk_update_query = 'update {} set {} = case {} end where {} in %s'

    def __init__(self, queue, db, data_provider):
        self._counter = 0
        self._queue = queue
        self._db = db
        self._provider = data_provider

    def consume(self):
        while True:
            job = self._queue.get()
            self._run(job)
            self._queue.task_done()

    def _run(self, job):
        table, primary_key, ids, updates = job
        update_values = []
        factories = []
        for action, column in updates:
            factory = getattr(self._provider, action)
            update_values.append('{0} = %s'.format(column))
            append = (action == 'email' or column == 'screenname')
            factories.append((factory, action, append))
        for primary_key_id in ids:
            values = []
            for factory, action, append in factories:
                random_value = factory()
                if action != 'random_int':
                    random_value = str(random_value)
                if isinstance(random_value, str) and append:
                    random_value = str(self._counter) + random_value
                self._counter += 1
                values.append(random_value)
            update_query = 'update {0} set {1} where {2} = %s'.format(table, ', '.join(update_values), primary_key)
            try:
                values.append(primary_key_id)
                cursor = self._db.execute_query(update_query, values)
                cursor.close()
            except Exception as e:
                print "Exception {0} for table {1}".format(e, table)
                break
        else:
            print 'Finished Job ' + table
