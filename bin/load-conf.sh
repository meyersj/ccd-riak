#!/bin/bash

# for live server
template=/vagrant/conf/riak.conf.template
out=/etc/riak/riak.conf
ip=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`
sed -e "s/{{ip}}/${ip}/g" ${template} > ${out}

# for local vm
#cp /vagrant/conf/riak.conf /etc/riak/riak.conf
