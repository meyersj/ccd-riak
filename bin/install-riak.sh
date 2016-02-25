#!/bin/bash

# http://docs.basho.com/riak/latest/ops/building/installing/debian-ubuntu


curl https://packagecloud.io/gpg.key | apt-key add -
cp /vagrant/conf/basho.list /etc/apt/sources.list.d
apt-get update
apt-get install -y \
    apt-transport-https default-jdk \
    riak python-dev python-pip libffi-dev libssl-dev 
pip install riak
