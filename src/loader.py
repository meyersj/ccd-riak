import csv
from os.path import join, dirname, abspath
import time
import os
from datetime import datetime

from utils import Bucket


HIGHWAYS = 'highways.csv'
STATIONS = 'freeway_stations.csv'
DETECTORS = 'freeway_detectors.csv'
LOOPDATA = 'loopdata1000.csv'


def loader(filepath, insert, bucket):
    """ 
    loop over each row in the csv as a dictionary (json)
    and call insert function with
    """
    line = 2 # first row are headers
    lines = 0
    start = time.time()

    """ get total line count
    """
    with open(filepath, 'rb') as f:
        # after iterating all lines i will have count -1
        for i, l in enumerate(f): pass
        lines = i + 1
    

    """ if there is only one or no rows there is no work to do
    """
    if lines < line:
        return False

    """ continue processing even if errors are encountered
    """
    with open(filepath, 'rb') as f:
        for i, row in enumerate(csv.DictReader(f)):
            try:
                insert(bucket, row)
                if i % 5000 == 0:
	            print datetime.now(), filepath, "line", line
	    except Exception as e:
	        print "error", os.path.basename(filepath), "line", line
                print str(e)
            line += 1
    end = time.time()
    print os.path.basename(filepath), "elapsed", (end - start) / 60.0, "minutes"


def insert_highway(bucket, row):
    bucket.put(row["highwayid"], row)


def insert_station(bucket, row):
    bucket.put(row["stationid"], row)


def insert_detector(bucket, row):
    bucket.put(row["detectorid"], row)


def insert_loopdata(bucket, row):
    datepart, timepart = row['starttime'].split(' ')
    timepart = timepart.split('-')[0]
    key = "{0}-{1}".format(row["detectorid"], row["starttime"])
    row['starttime'] = datepart + "T" + timepart + "Z"
    # if empty string change to zero
    # solr will not index if integer field is empty string
    for field in row:
        if row[field] == "":
            row[field] = 0
    bucket.put(key, row)


def delete_loopdata(bucket, row):
    key = "{0}-{1}".format(row["detectorid"], row["starttime"])
    bucket.delete(key)
   

def load_schema(client, solr):
    Bucket(client, 'highways').set_index(join(solr, 'highways.xml'))
    Bucket(client, 'stations').set_index(join(solr, 'stations.xml'))
    Bucket(client, 'detectors').set_index(join(solr, 'detectors.xml'))
    Bucket(client, 'loopdata').set_index(join(solr, 'loopdata.xml'))


def load_data(client, data):
    loader(join(data, HIGHWAYS), insert_highway, Bucket(client, 'highways'))
    loader(join(data, STATIONS), insert_station, Bucket(client, 'stations'))
    loader(join(data, DETECTORS), insert_detector, Bucket(client, 'detectors'))


def load_loopdata(client):
    data = join(dirname(dirname(abspath(__file__))), "data")
    loader(join(data, LOOPDATA), insert_loopdata, Bucket(client, 'loopdata')) 


def clear(client):
    data = join(dirname(dirname(abspath(__file__))), "data")
    loader(join(data, LOOPDATA), delete_loopdata, Bucket(client, 'loopdata')) 


def load(client):
    """
        - create bucket for each data type
        - load solr index from xml resource
        - load each record from csv data into bucket
    """

    parent = dirname(dirname(abspath(__file__)))
    data = join(parent, "data")
    solr = join(parent, "conf/solr")
    
    load_schema(client, solr)    
    time.sleep(60)
    load_data(client, data)
