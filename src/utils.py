from riak import RiakClient
from riak.riak_object import RiakObject


RIAK_HOST = '127.0.0.1'
RIAK_PORT = 8098


def connect(host=RIAK_HOST, port=RIAK_PORT):
    client = RiakClient(protocol='http', host=host, http_port=port)
    assert client.ping(), "Failed to connect to client"
    return client


class Bucket(object):

    def __init__(self, client, bucket):
        self.client = client
        self.name = bucket
        self.bucket = self.client.bucket(bucket)

    def set_index(self, schemafile):
        with open(schemafile, 'r') as schema:
            self.client.create_search_schema(self.name, schema.read())
            self.client.create_search_index(self.name, schema=self.name)
            self.bucket.set_property('search_index', self.name)

    def put(self, key, data):
        obj = RiakObject(self.client, self.bucket, key=key)
        obj.data = data
        obj.store()
        
    def get(self, key):
        return self.bucket.get(key)

    def search(self, search):
        return self.bucket.search(search)
