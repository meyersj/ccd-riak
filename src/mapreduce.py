from utils import connect, Bucket

from riak import RiakMapReduce


def speed_over_100(client):
    """
        Example Map Reduce Job with Riak
        count the number of occurences for each highway id in all the detectors
    """
    mr = RiakMapReduce(client)
    mr.add_bucket('loopdata')
    mr.map("""
      function(v) {
        var data = Riak.mapValuesJson(v)[0];
        //var highwayid = data.highwayid;
        ejsLog('/tmp/map_reduce.log', JSON.stringify(data))
        return [{d:data}];
        //return [{d:[highwayid, 1]}];
      }
    """)
    mr.reduce("""
      function(values) {
        var counts = {};
        var i = 0;
        if (values[0].c) {
          counts = values[0].c;
          i = 1;
        }
        /*
        for (; i < values.length; i++) {
          var data = values[i].d;
          if (data[0] in counts) {
            counts[data[0]] += data[1]
          }
          else {
            counts[data[0]] = data[1]
          }
        }
        */
        return [{c:counts}];
      }
    """)
    response = mr.run()
    print response[0]["c"]


def highway_detector_count(client):
    """
        Example Map Reduce Job with Riak
        count the number of occurences for each highway id in all the detectors
    """
    mr = RiakMapReduce(client)
    mr.add_bucket('detectors')
    mr.map("""
      function(v) {
        var data = Riak.mapValuesJson(v)[0];
        var highwayid = data.highwayid;
        return [{d:[highwayid, 1]}];
      }
    """)
    mr.reduce("""
      function(values) {
        var counts = {};
        var i = 0;
        if (values[0].c) {
          counts = values[0].c;
          i = 1;
        }
        for (; i < values.length; i++) {
          var data = values[i].d;
          if (data[0] in counts) {
            counts[data[0]] += data[1]
          }
          else {
            counts[data[0]] = data[1]
          }
        }
        return [{c:counts}];
      }
    """)
    response = mr.run()
    print response[0]["c"]


if __name__ == "__main__":
    client = connect()
    speed_over_100(client)
    highway_detector_count(client)
