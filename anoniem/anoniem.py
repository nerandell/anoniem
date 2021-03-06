from threading import Thread
from queue import Queue

import yaml
import click
from faker import Faker

from anoniem.consumer import Consumer
from anoniem.producer import Producer
from anoniem.db_client import DbClient


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
        self._create_workers()

    def _create_workers(self):
        for i in range(self._num_of_workers):
            t = Thread(target=self._worker)
            t.start()

    def read(self, file_name):
        cfg = yaml.load(open(file_name))
        if cfg:
            self._config = cfg
            self._anonymize()
        self._queue.join()

    def _anonymize(self):
        tables = self._config.get('tables', {})
        for table, actions in tables.items():
            primary_key = actions.pop('primary_key')
            for action, columns in actions.items():
                self._randomize(table, columns, primary_key, action)

    def _randomize(self, table, columns, primary_key, action):
        for column in columns:
            self._producer.create_job(table, primary_key, column, action[10:])

    def _worker(self):
        consumer = Consumer(self._queue, self._get_db_client())
        consumer.consume()

    def _get_db_client(self):
        return DbClient(self._host, self._port, self._user, self._password, self._db)


@click.command()
@click.option('--filename', help='Name of the yml file')
@click.option('--host', default='127.0.0.1', help='db host')
@click.option('--port', default=3306, help='db port')
@click.option('--user', help='db user')
@click.option('--password', default='', help='db password')
@click.option('--db', help='db name')
@click.option('--number_of_workers', default=5, help='Number of threads')
def anonymize(filename, host, port, user, password, db, number_of_workers):
    anoniem = Anoniem(host, port, user, password, db, number_of_workers)
    anoniem.read(filename)


if __name__ == '__main__':
    anonymize()
