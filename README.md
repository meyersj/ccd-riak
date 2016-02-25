build vm
```bash
vagrant up
curl http://127.0.0.1:8098/ping     # should return 'OK' if everything worked
```

connect to vm and run scripts
```bash
vagrant ssh

# load data
python /vagrant/src/main.py load

# run queries
python /vagrant/src/main.py query
```


Building Cluster
================

Create three nodes and make sure `nodename` and ip address is configured in `riak.conf`. The First node can be started as normal (`riak start`). For additional nodes you have to join them to any existing node.

Assume first nodes `nodename` is `riak@198.192.96.216`

1. Connect to each additional node and run: `riak-admin cluster join riak@198.192.96.216`
2. Check cluster plan: `riak-admin cluster plan`
3. Commit changes to build cluster: `riak-admin cluster commit`

