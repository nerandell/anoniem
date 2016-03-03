from threading import Thread
from Queue import Queue
from collections import defaultdict

import yaml
import click
from faker import Faker

from .consumer import Consumer
from .producer import Producer
from .db_client import DbClient


class Anoniem:
    def __init__(self, host, port, user, password, db, num_of_workers):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db = db
        self._config = None
        self._queue = Queue()
        self._num_of_workers = num_of_workers
        self._producer = Producer(self._queue, self._get_db_client(), Faker())
        self._done = defaultdict(list)

    def _create_workers(self):
        for i in range(self._num_of_workers):
            t = Thread(target=self._worker)
            t.daemon = True
            t.start()

    def read(self, file_name):
        cfg = yaml.load(open(file_name))
        if cfg:
            self._config = cfg
            self._anonymize()

    def _anonymize(self):
        tables = self._config.get('tables', {})

        for table, actions in tables.items():
            self._producer.build_cache(table, actions['primary_key'])

        print 'Cache built'

        queues = []
        for table, actions in tables.items():
            queue = Queue()
            queues.append(queue)
            self._worker(table, queue)
            print 'On Table {0}'.format(table)
            primary_key = actions.pop('primary_key')
            job = self._producer.create_row_job(table, primary_key, actions)
            queue.put(job)

        for queue in queues:
            queue.join()

    def _consume(self, queue):
        consumer = Consumer(queue, self._get_db_client(), Faker())
        consumer.consume()

    def _worker(self, table, queue):
        print 'Creating thread for table', table
        t = Thread(target=self._consume, name=table, args=(queue, ))
        t.daemon = True
        t.start()
        return t

    def _get_db_client(self):
        return DbClient(self._host, self._port, self._user, self._password, self._db)


@click.command()
@click.option('--filename', help='Name of the yml file')
@click.option('--host', default='127.0.0.1', help='db host')
@click.option('--port', default=3306, help='db port')
@click.option('--user', help='db user')
@click.option('--password', default='', help='db password')
@click.option('--db', help='db name')
@click.option('--number_of_workers', default=4, help='Number of threads')
def anonymize(filename, host, port, user, password, db, number_of_workers):
    anoniem = Anoniem(host, port, user, password, db, number_of_workers)
    anoniem.read(filename)


if __name__ == '__main__':
    anonymize()
