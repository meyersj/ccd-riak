import csv 
import json
from os.path import join, dirname, abspath
import time
import os

from riak import RiakClient, RiakNode
from riak.riak_object import RiakObject


RIAK_HOST = '127.0.0.1'
RIAK_PORT = 8098
HIGHWAYS = 'highways.csv'
STATIONS = 'freeway_stations.csv'
DETECTORS = 'freeway_detectors.csv'
LOOPDATA = 'freeway_loopdata_OneHour.csv'


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


def connect(host=RIAK_HOST, port=RIAK_PORT):
    client = RiakClient(protocol='http', host=host, http_port=port)
    assert client.ping(), "Failed to connect to client"
    return client


def loader(filepath, insert, bucket):
    with open(filepath, 'rb') as f:
        for row in csv.DictReader(f):
            insert(bucket, row)


def insert_highway(bucket, row):
    bucket.put(row["highwayid"], row)


def insert_station(bucket, row):
    bucket.put(row["stationid"], row)


def insert_detector(bucket, row):
    bucket.put(row["detectorid"], row)


def insert_loopdata(bucket, row):
    key = "{0}-{1}".format(row["detectorid"], row["starttime"])
    bucket.put(key, row)


def build(client):
    """
        - create bucket for each data type
        - load solr index from xml resource
        - load each record from csv data into bucket
    """

    parent = dirname(dirname(abspath(__file__)))
    data = join(parent, "data")
    solr = join(parent, "conf/solr")
    
    highways_bucket = Bucket(client, 'highways')
    highways_bucket.set_index(join(solr, 'highways.xml'))
    loader(join(data, HIGHWAYS), insert_highway, highways_bucket)
    
    stations_bucket = Bucket(client, 'stations')
    stations_bucket.set_index(join(solr, 'stations.xml'))
    loader(join(data, STATIONS), insert_station, stations_bucket)
    
    detectors_bucket = Bucket(client, 'detectors')
    detectors_bucket.set_index(join(solr, 'detectors.xml'))
    loader(join(data, DETECTORS), insert_detector, detectors_bucket)

    loopdata_bucket = Bucket(client, 'loopdata')
    loopdata_bucket.set_index(join(solr, 'loopdata.xml'))
    loader(join(data, LOOPDATA), insert_loopdata, loopdata_bucket)


def test(client):
    highways_bucket = Bucket(client, 'highways')
    stations_bucket = Bucket(client, 'stations')
    detectors_bucket = Bucket(client, 'detectors')
    loopdata_bucket = Bucket(client, 'loopdata')

    # send some test queries
    print highways_bucket.get('3').data, '\n'
    print highways_bucket.search('highwayid:3'), '\n'
    print stations_bucket.get('1045').data, '\n'
    print stations_bucket.search('stationid:1045'), '\n'
    print detectors_bucket.get('1345').data, '\n'
    print detectors_bucket.search('detectorid:1345'), '\n'
    print loopdata_bucket.search('detectorid:1345'), '\n'


def main():
    client = connect()
    build(client)
    test(client)


if __name__ == '__main__':
    main()
