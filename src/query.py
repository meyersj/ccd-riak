from utils import Bucket

FOSTER_NB_STATIONID = '1047'


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


def query3(client):
    """ get Foster NB length
    """
    stations_bucket = Bucket(client, 'stations')
    results = stations_bucket.get(FOSTER_NB_STATIONID)
    length = results.data['length']

    
    """ find detector ids for Foster NB Station
    """
    detectors_bucket = Bucket(client, 'detectors')
    results = detectors_bucket.search('stationid:{0}'.format(FOSTER_NB_STATIONID))
    detectorids = " OR ".join(
        [ "detectorid:" + str(row['detectorid']) for row in results['docs'] ]
    )
    
    """ using the found detector ids construct a query to retrieve the records
        for the given timerange
    """
    loopdata_bucket = Bucket(client, 'loopdata')
    timerange = 'starttime:[2011-09-15T00:00:00Z TO 2011-09-15T00:59:59Z]' 
    query = '{0} AND ({1})'.format(timerange, detectorids)
    results = loopdata_bucket.search(query)
    
    """ from the results compute volume
    """
    speed = 0
    for row in results['docs']:
        speed += row['speed']
    avg_speed = float(speed) / len(results['docs'])
    peak_travel_time = (float(length) / avg_speed) * 3600
    return peak_travel_time


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


def query5(client):
    STATION_ID = '1140'
    stations_bucket = Bucket(client, 'stations')
    result = stations_bucket.get(STATION_ID)
    result.data['length'] = 2.3
    stations_bucket.put(STATION_ID, result.data)


def test_indexes(client):
    print Bucket(client, 'highways').search('highwayid:3')
    print Bucket(client, 'detectors').search('detectorid:1345')
    print Bucket(client, 'stations').search('stationid:1045')
    print Bucket(client, 'loopdata').search('detectorid:1345')


def run(client):
    #test_indexes(client)
    print 'Query 1: Records with Speed >= 100:', query1(client)
    print 'Query 2: Volume at Foster NB on Sept 21 2011:', query2(client)
    print 'Query 3: Peak Travel Times:', query3(client)
    print 'Query 4: Route Finding:', query4(client)
    print 'Query 5: Update station 1140 lengh'
    query5(client)
