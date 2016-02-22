import csv 
import json
from os.path import join, dirname, abspath
import time
import os

from datetime import datetime


from riak import RiakClient, RiakNode
from riak.riak_object import RiakObject


RIAK_HOST = '127.0.0.1'
RIAK_PORT = 8098
HIGHWAYS = 'highways.csv'
STATIONS = 'freeway_stations.csv'
DETECTORS = 'freeway_detectors.csv'
LOOPDATA = 'freeway_loopdata_OneHour.csv'
FOSTER_NB_STATIONID = '1047'


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
    d, t = row['starttime'].split(' ')
    if len(t) == 7:
        t = '0' + t
    timestamp = datetime.strptime(d + ' ' + t, '%m/%d/%Y %H:%M:%S')
    row['starttime'] = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
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
    
    stations_bucket = Bucket(client, 'stations')
    stations_bucket.set_index(join(solr, 'stations.xml'))
    
    detectors_bucket = Bucket(client, 'detectors')
    detectors_bucket.set_index(join(solr, 'detectors.xml'))
    
    loopdata_bucket = Bucket(client, 'loopdata')
    loopdata_bucket.set_index(join(solr, 'loopdata.xml'))

    time.sleep(60)
    
    loader(join(data, HIGHWAYS), insert_highway, highways_bucket)
    loader(join(data, STATIONS), insert_station, stations_bucket)
    loader(join(data, DETECTORS), insert_detector, detectors_bucket)
    loader(join(data, LOOPDATA), insert_loopdata, loopdata_bucket)


def test_indexes(client):
    print Bucket(client, 'highways').search('highwayid:3')
    print Bucket(client, 'detectors').search('detectorid:1345')
    print Bucket(client, 'stations').search('stationid:1045')
    print Bucket(client, 'loopdata').search('detectorid:1345')


def query1(client):
    loopdata_bucket = Bucket(client, 'loopdata')
    results = loopdata_bucket.search('speed:[100 TO *]')
    return results['num_found']


def query2(client):
    """ find detector ids for Foster NB Station
    """
    detectors_bucket = Bucket(client, 'detectors')
    results = detectors_bucket.search('stationid:{0}'.format(FOSTER_NB_STATIONID))
    detectorids = " OR ".join(
        [ "detectorid:" + str(row['detectorid']) for row in results['docs'] ]
    )
    
    """ using the found detector ids construct a query to retrieve the records
        on the given date
    """
    loopdata_bucket = Bucket(client, 'loopdata')
    daterange = 'starttime:[2011-09-15T00:00:00Z TO 2011-09-15T23:59:59Z]' 
    query = '{0} AND ({1})'.format(daterange, detectorids)
    results = loopdata_bucket.search(query)
    
    """ from the results compute volume
    """
    volume = 0
    for row in results['docs']:
        volume += row['volume']
    return volume


def query(client):
    print query1(client)
    print query2(client)
      

def main():
    client = connect()
    #build(client)
    #test_indexes(client)
    query(client)


if __name__ == '__main__':
    main()
