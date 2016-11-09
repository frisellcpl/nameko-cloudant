from weakref import WeakKeyDictionary

from nameko.extensions import DependencyProvider

from cloudant.client import Cloudant

DB_USERNAME = 'DB_USERNAME'
DB_PASSWORD = 'DB_PASSWORD'
DB_ACCOUNT = 'DB_ACCOUNT'


class DatabaseClient(DependencyProvider):
    def __init__(self, database):
        self.database = database
        self.clients = WeakKeyDictionary()

    def setup(self):
        self.username = self.container.config[DB_USERNAME]
        self.password = self.container.config[DB_PASSWORD]
        self.account = self.container.config[DB_ACCOUNT]

    def stop(self):
        client = self.clients[worker_ctx]
        client.disconnect()
        del client

    def get_dependency(self, worker_ctx):
        client = Cloudant(
            self.username,
            self.password,
            account=self.account,
        )
        self.clients[worker_ctx] = client
        client.connect()
        return client[self.database]

    def worker_teardown(self, worker_ctx):
        client = self.clients.pop(worker_ctx)
        client.disconnect()
