from utils import Bucket, timer

FOSTER_NB_STATIONID = '1047'
MAX_ROWS = 18000000


@timer
def query1(client):
    loopdata_bucket = Bucket(client, 'loopdata')
    results = loopdata_bucket.search('speed:[101 TO *]', params=dict(rows=MAX_ROWS))
    return results['num_found']


@timer
def query2(client, daterange):
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
    daterange = 'starttime:[{0}]'.format(daterange)
    query = '{0} AND ({1}) AND (*:* NOT volume:0)'.format(daterange, detectorids)
    results = loopdata_bucket.search(query, params=dict(rows=MAX_ROWS))

    """ from the results compute volume
    """
    volume = 0
    for row in results['docs']:
        volume += int(row['volume'])
    return volume


@timer
def query3(client, daterange):
    """ get Foster NB length
    """
    stations_bucket = Bucket(client, 'stations')
    results = stations_bucket.get(FOSTER_NB_STATIONID)
    length = results.data['length']
    
    """ find detector ids for Foster NB Station
    """
    detectors_bucket = Bucket(client, 'detectors')
    results = detectors_bucket.search(
        'stationid:{0}'.format(FOSTER_NB_STATIONID),
        params=dict(rows=MAX_ROWS)
    )
    detectorids = " OR ".join(
        [ "detectorid:" + str(row['detectorid']) for row in results['docs'] ]
    )
    
    """ using the found detector ids construct a query to retrieve the records
        for the given timerange
    """
    loopdata_bucket = Bucket(client, 'loopdata')
    timerange = 'starttime:[{0}]'.format(daterange)
    query = '{0} AND ({1}) AND (*:* NOT volume:0)'.format(timerange, detectorids)
    results = loopdata_bucket.search(query, params=dict(rows=MAX_ROWS))
    if len(results['docs']) == 0:
        return None 

    """ from the results compute volume
    """
    speed = 0
    count = 0
    for row in results['docs']:
        if int(row['volume']) > 0 and int(row['speed']) > 0:
            speed += int(row['speed'])
            count += 1
    avg_speed = float(speed) / float(count)
    if avg_speed == 0:
        return None
    peak_travel_time = (float(length) / avg_speed) * 3600
    return peak_travel_time


@timer
def query4(client):
    JOHNSON_CREEK = '1046'
    COLUMBIA = '1140'
    stations_bucket = Bucket(client, 'stations')
    station = stations_bucket.get(JOHNSON_CREEK)
    stations = [station.data]
    while station.data['stationid'] != COLUMBIA:
        station = stations_bucket.get(station.data['downstream'])
        stations.append(station.data)
    return [ station['locationtext'] for station in stations ]


@timer
def query5(client, length):
    STATION_ID = '1140'
    stations_bucket = Bucket(client, 'stations')
    result = stations_bucket.get(STATION_ID)
    before = result.data['length']
    result.data['length'] = length
    stations_bucket.put(STATION_ID, result.data)
    result = stations_bucket.get(STATION_ID)
    after = result.data['length']
    return before, after


def test_indexes(client):
    print Bucket(client, 'highways').search('highwayid:3')
    print Bucket(client, 'detectors').search('detectorid:1345')
    print Bucket(client, 'stations').search('stationid:1045')
    print Bucket(client, 'loopdata').search('detectorid:1345')


def test_keys(client):
    print Bucket(client, 'highways').get('3').data
    print Bucket(client, 'detectors').get('1345').data
    print Bucket(client, 'stations').get('1045').data


def all_queries(client):
    q1 = query1(client)
    print 'Query 1: Records with Speed > 100\n\t', q1['ret']
    print '\n', q1['elapsed'] * 1000, 'ms\n'
    # 2011-09-21 00:00:00 TO 2011-9-21 23:59:59 converted to UTC (-07:00)
    daterange = '2011-09-21T07:00:00Z TO 2011-09-22T06:59:59Z'
    q2 = query2(client, daterange)
    print 'Query 2: Foster NB Volume on Sept 21 2011\n\t', q2['ret']
    print '\n', q2['elapsed'] * 1000, 'ms\n'
    # ampeak 2011-09-22 7:00:00 TO 8:59:59 converted to UTC (-07:00)
    # pmpeak 2011-09-22 16:00:00 TO 17:59:59 converted to UTC (-07:00)
    ampeak = '2011-09-22T14:00:00Z TO 2011-09-22T15:59:59Z'
    pmpeak = '2011-09-22T23:00:00Z TO 2011-09-23T00:59:59Z'
    q3am = query3(client, ampeak)
    print 'Query 3: Foster NB AM Peak Travel Time\n\t', q3am['ret']
    print '\n', q3am['elapsed'] * 1000, 'ms\n'
    q3pm = query3(client, pmpeak)
    print 'Query 3: Foster NB PM Peak Travel Time\n\t', q3pm['ret']
    print '\n', q3pm['elapsed'] * 1000, 'ms\n'
    q4 = query4(client)
    print 'Query 4: Route Finding'
    for stop in q4['ret']:
        print '\t' + stop
    print '\n', q4['elapsed'], 'ms\n'
    q5 = query5(client, 2.3)
    print 'Query 5: Update station 1140 length\n\t', q5['ret'][1]
    print '\n', q5['elapsed']


def run(client):
    #test_indexes(client)
    #test_keys(client)
    all_queries(client)
