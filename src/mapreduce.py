from utils import connect, Bucket, timer

from riak import RiakMapReduce, RiakKeyFilter

from datetime import datetime
import time


FOSTER_NB_STATIONID = '1047'
MAX_ROWS = 18000000

HOUR = 60 * 60
HOURS2 = HOUR * 2
HOURS24 = HOUR * 24


def timerange_convert(start, duration):
    # compute start and end epoch based on start timestamp, duration in seconds
    t = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    start = int(time.mktime(t.timetuple()))
    end = start + (duration - 1)
    return start, end


def lookup_detectors(stationid):
    detectors_bucket = Bucket(client, 'detectors')
    results = detectors_bucket.search(
        'stationid:{0}'.format(stationid), params=dict(rows=MAX_ROWS)
    )
    return [ str(row['detectorid']) for row in results['docs'] ]


@timer
def query1(client):
    """
        Name: Speed Over 100
        Desc: Count number of loopdata records with speed over 100
    """
    mr = RiakMapReduce(client)
    mr.add_bucket('loopdata')
    
    # key: <detector id>-<epoch>-<speed>-<volume>
    # tokenize key and grab speed, filter greater than 100
    mr.add_key_filter("tokenize", "-", 3)
    mr.add_key_filter("string_to_int")
    mr.add_key_filter("greater_than", 100)
   
    # return 1 for each record with speed greater than 100
    # and reduce into total count
    mr.map("function(record) { return [1]; }")
    mr.reduce_sum()
    
    response = mr.run(timeout=HOURS24 * 1000)
    if response:
        return response[0]
    return None


@timer
def query2(client, start, duration=HOURS24):
    """
        Count number of loopdata records with speed over 100
    """

    # lookup detector ids for Foster NB station
    detectorids = lookup_detectors(FOSTER_NB_STATIONID)
 
    # build list of key filters for any key starting with detector id
    # for Foster NB station
    # starts_with 1361 OR starts_with 1362 OR starts_with 1363
    foster = None
    for detector in detectorids:
        f = RiakKeyFilter().starts_with(detector)
        if not foster:
            foster = f
        else:
            foster = foster | f
   
    # filter records where volume is zero
    volume = RiakKeyFilter().tokenize("-", 4)
    volume += RiakKeyFilter().neq("0")
    
    # key: <detector id>-<epoch>-<speed>-<volume>
    # build key filters for epoch being between start and end times
    start_epoch, end_epoch = timerange_convert(start, duration)
    timerange = RiakKeyFilter().tokenize("-", 2)
    timerange += RiakKeyFilter().string_to_int()
    timerange += RiakKeyFilter().between(start_epoch, end_epoch)

    mr = RiakMapReduce(client)
    mr.add_bucket('loopdata')
    mr.add_key_filters(volume & timerange & foster) 
   
    # return 1 for each record with speed greater than 100
    # and reduce into total count
    mr.map("""
      function(record) {
        var data = Riak.mapValuesJson(record)[0];
        return [parseInt(data.volume)];
      }
    """)
    mr.reduce_sum()
    
    response = mr.run(timeout=HOURS24 * 1000)
    if response:
        return response[0]
    return None


@timer
def query3(client, start, duration=HOURS2):
    """
        Count number of loopdata records with speed over 100
    """
    # get length of Foster NB station
    stations_bucket = Bucket(client, 'stations')
    results = stations_bucket.get(FOSTER_NB_STATIONID)
    length = results.data['length']

    # lookup detector ids for Foster NB station
    detectorids = lookup_detectors(FOSTER_NB_STATIONID)
   
    # build list of key filters for any key starting with detector id
    # for Foster NB station
    # starts_with 1361 OR starts_with 1362 OR starts_with 1363
    foster = None
    for detector in detectorids:
        f = RiakKeyFilter().starts_with(detector)
        if not foster:
            foster = f
        else:
            foster = foster | f
   
    # filter records where volume is zero
    volume = RiakKeyFilter().tokenize("-", 4)
    volume += RiakKeyFilter().neq("0")
    
    # key: <detector id>-<epoch>-<speed>-<volume>
    # build key filters for epoch being between start and end times
    start_epoch, end_epoch = timerange_convert(start, duration)
    timerange = RiakKeyFilter().tokenize("-", 2)
    timerange += RiakKeyFilter().string_to_int()
    timerange += RiakKeyFilter().between(start_epoch, end_epoch)

    mr = RiakMapReduce(client)
    mr.add_bucket('loopdata')
    mr.add_key_filters(volume & timerange & foster) 
   
    # return speed for each loopdata record matching key filters above
    mr.map("""
      function(record) {
        var data = Riak.mapValuesJson(record)[0];
        return [parseInt(data.speed)];
      }
    """)
    mr.reduce("""
      function(values) {
        return values;
      }
    """)
    
    # compute peak travel time from all mapped records
    response = mr.run(timeout=HOURS24 * 1000)
    if not response:
        return None
    avg_speed = sum(response) / float(len(response))
    peak_travel_time = (float(length) / avg_speed) * 3600
    return peak_travel_time


def run(client):
    q1 = query1(client)
    print 'Query 1: Records with Speed > 100\n\t', q1['ret']
    print '\n', q1['elapsed'] * 1000, 'ms\n'
    
    start = "2011-09-21 00:00:00"
    q2 = query2(client, start)
    print 'Query 2: Foster NB Volume on Sept 21 2011\n\t', q2['ret']
    print '\n', q2['elapsed'] * 1000, 'ms\n'
    
    ampeak = "2011-09-22 07:00:00"
    pmpeak = "2011-09-22 16:00:00"
    q3am = query3(client, ampeak)
    print 'Query 3: Foster NB AM Peak Travel Time\n\t', q3am['ret']
    print '\n', q3am['elapsed'] * 1000, 'ms\n'
    q3pm = query3(client, pmpeak)
    print 'Query 3: Foster NB PM Peak Travel Time\n\t', q3pm['ret']
    print '\n', q3pm['elapsed'] * 1000, 'ms'


if __name__ == "__main__":
    client = connect()
    run(client)
