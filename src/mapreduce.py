from utils import connect, Bucket

from riak import RiakMapReduce, RiakKeyFilter

from datetime import datetime
import time


FOSTER_NB_STATIONID = '1047'
MAX_ROWS = 18000000


TZ_OFFSET = -7
HOUR = 60 * 60
HOURS2 = HOUR * 2
HOURS24 = HOUR * 24


def timerange_convert(start, duration, tzoffset):
    # compute start and end epoch based on start timestamp, duration in seconds
    # and timezone offset from UTC in hours
    t = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    start = int(time.mktime(t.timetuple())) - (tzoffset * HOUR)
    end = start + (duration - 1)
    return start, end


def lookup_detectors(stationid):
    detectors_bucket = Bucket(client, 'detectors')
    results = detectors_bucket.search(
        'stationid:{0}'.format(stationid), params=dict(rows=MAX_ROWS)
    )
    return [ str(row['detectorid']) for row in results['docs'] ]


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
    
    response = mr.run()
    if response:
        return response[0]
    return None


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
    start, end = timerange_convert(start, duration, TZ_OFFSET)
    timerange = RiakKeyFilter().tokenize("-", 2)
    timerange += RiakKeyFilter().string_to_int()
    timerange += RiakKeyFilter().between(start, end)

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
    
    response = mr.run()
    if response:
        return response[0]
    return None


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
    start, end = timerange_convert(start, duration, TZ_OFFSET)
    timerange = RiakKeyFilter().tokenize("-", 2)
    timerange += RiakKeyFilter().string_to_int()
    timerange += RiakKeyFilter().between(start, end)

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
    response = mr.run()
    if not response:
        return None
    avg_speed = sum(response) / float(len(response))
    peak_travel_time = (float(length) / avg_speed) * 3600
    return peak_travel_time


if __name__ == "__main__":
    client = connect()

    print "Query 1 - Count over 100:\n\tRecords:", query1(client)

    date = "2011-09-15"
    msg = "Query 2 - Volume on {0} at Foster NB Station\n\tVolume:".format(date)
    timestamp = "{0} 00:00:00".format(date)
    print msg, query2(client, timestamp)
    
    ampeak = "2011-09-15 00:07:00"
    pmpeak = "2011-09-15 00:16:00"
    print "Query 3 - Foster NB - Avg Travel Time"
    print "\tAM Peak:", query3(client, ampeak)
    print "\tPM Peak:", query3(client, pmpeak)
