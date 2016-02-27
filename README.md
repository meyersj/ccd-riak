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


Building Cluster (manually)
===========================

Create three nodes and make sure `nodename` and ip address is configured in `riak.conf`. The First node can be started as normal (`riak start`). For additional nodes you have to join them to any existing node.

Assume first nodes `nodename` is `riak@198.192.96.216`

1. Connect to each additional node and run: `riak-admin cluster join riak@198.192.96.216`
2. Check cluster plan: `riak-admin cluster plan`
3. Commit changes to build cluster: `riak-admin cluster commit`

Building Cluster (automatically)
================================

Create a main project directory
```bash
mkdir ~/ccd-riak
cd ~/ccd-riak
```

Clone this repo for each node (you must name the folder using the format `nodeX`)
```bash
git clone https://github.com/meyersj/ccd-riak.git node1
git clone https://github.com/meyersj/ccd-riak.git node2
git clone https://github.com/meyersj/ccd-riak.git node3
```

Clone again for the manager
```bash
git clone https://github.com/meyersj/ccd-riak.git manager
```

Edit `manager/bin/manager.sh`
  + `export DO_API_TOKEN=`  must be set to valid digital ocean API token
  + `export DO_SSH_KEY=` must be set to private ssh key that will be used for accessing the nodes

Build nodes
  + Execute: `manager/bin/manager.sh build`
  + This will run a `vagrant up --provider=digital_ocean` in each of repositories created above
  + In each `nodeX` you will find a `build.out` file.
  + When `FINISHED` is printed at the end of `build.out` that node has been built succesfully
  + For each node you want to get the IP address
  + In `manager/bin/manager.sh` set `MAIN_NODE` variable to the IP address of any node and
      set `ALL_NODES` to each additional nodes IP address 

After setting `MAIN_NODE` and `ALL_NODES` create the cluster
  + Execute: `manager/bin/manager.sh cluster`
  + This will ssh onto each node in `ALL_NODES` and join it to `MAIN_NODE`
  + Then changes are committed to create a cluster

Finally load the data
  + Execute: `manager/bin/manager.sh load`
  + The `loopdata` csv has been split into three files and loaded into S3
  + Each node will download its section of data and begin looping over it inserting the records using put requests 

