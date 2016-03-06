#!/bin/bash

tmp=/tmp/status1204k31mlkdm1

# active protocol buffer connections
riak-admin stat show pbc_active > $tmp

# 1 minute puts
riak-admin stat show node_puts >> $tmp

# average jobs in
riak-admin stat show node_put_fsm_in_rate >> $tmp

# average jobs out
riak-admin stat show node_put_fsm_out_rate >> $tmp

cat $tmp
