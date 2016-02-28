import csv
from os.path import join, dirname, abspath
import time
import os
from datetime import datetime

from utils import Bucket


HIGHWAYS = 'highways.csv'
STATIONS = 'freeway_stations.csv'
DETECTORS = 'freeway_detectors.csv'
LOOPDATA = ''


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
                if i + 1 % 10000 == 0:
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
    timestamp = datetime.strptime(datepart + ' ' + timepart, '%Y-%m-%d %H:%M:%S')
    epoch = int(time.mktime(timestamp.timetuple()))
    key = "{0}-{1}".format(row["detectorid"], epoch)
    row['starttime'] = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
    bucket.put(key, row)
  

def load(client):
    """
        - create bucket for each data type
        - load solr index from xml resource
        - load each record from csv data into bucket
    """

    parent = dirname(dirname(abspath(__file__)))
    data = join(parent, "data")
    solr = join(parent, "conf/solr")
    
    highways_bucket = Bucket(client, 'highways')
    stations_bucket = Bucket(client, 'stations')
    detectors_bucket = Bucket(client, 'detectors')
    loopdata_bucket = Bucket(client, 'loopdata')
    
    highways_bucket.set_index(join(solr, 'highways.xml'))
    stations_bucket.set_index(join(solr, 'stations.xml'))
    detectors_bucket.set_index(join(solr, 'detectors.xml'))
    loopdata_bucket.set_index(join(solr, 'loopdata.xml'))
    
    time.sleep(60)
    
    loader(join(data, HIGHWAYS), insert_highway, highways_bucket)
    loader(join(data, STATIONS), insert_station, stations_bucket)
    loader(join(data, DETECTORS), insert_detector, detectors_bucket)
    for batch in [1, 2, 3]:
    	LOOPDATA = 'loopdata{0}'.format(batch)
    	print LOOPDATA
        loader(join(data, LOOPDATA), insert_loopdata, loopdata_bucket)
