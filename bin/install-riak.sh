#!/bin/bash

# http://docs.basho.com/riak/latest/ops/building/installing/debian-ubuntu


curl https://packagecloud.io/gpg.key | apt-key add -
cp /vagrant/conf/basho.list /etc/apt/sources.list.d
apt-get update
apt-get install -y \
    apt-transport-https default-jdk \
    riak python-dev python-pip libffi-dev libssl-dev 
pip install riak

# install go
curl -O https://storage.googleapis.com/golang/go1.5.3.linux-amd64.tar.gz
tar -xvf go1.5.3.linux-amd64.tar.gz
mv go /usr/local

echo >> ~/.profile
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~/.profile
source ~/.profile
go get github.com/basho/riak-go-client

echo FINISHED
reboot
