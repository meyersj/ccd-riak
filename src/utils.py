import socket
import fcntl
import struct

from riak import RiakClient
from riak.riak_object import RiakObject


def ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])


try:
    RIAK_HOST = ip_address('eth0')
except IOError:
    RIAK_HOST = '127.0.0.1'
RIAK_HTTP_PORT = 8098
RIAK_PBC_PORT = 8087


def connect():
    #nodes = []
    #for ip in ["45.55.0.44", "107.170.224.168", "45.55.1.236"]:
    #    nodes.append(dict(host=ip, http_port=RIAK_HTTP_PORT, pb_port=RIAK_PBC_PORT))
    nodes = [dict(host=RIAK_HOST, http_port=RIAK_HTTP_PORT, pb_port=RIAK_PBC_PORT)]
    client = RiakClient(nodes=nodes)
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

    def delete(self, key):
        self.bucket.delete(key)

    def get(self, key):
        return self.bucket.get(key)

    def search(self, search):
        return self.bucket.search(search)
