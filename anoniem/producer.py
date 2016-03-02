class Producer:
    def __init__(self, queue, db, data_provider):
        self._cache = {}
        self._provider = data_provider
        self._queue = queue
        self._db = db

    def build_cache(self, table, primary_key):
        cursor = self._db.execute_query('SELECT {0} from {1}'.format(primary_key, table), ())
        primary_keys = [row[0] for row in cursor]
        self._cache[table] = primary_keys
        cursor.close()

    def create_job(self, table, primary_key, column, action):
        primary_keys = self._cache[table]
        random_data = getattr(self._provider, action)
        print random_data().__class__.__name__
        job = [(table, column, primary_key, random_data(), row) for row in primary_keys]
        return job
