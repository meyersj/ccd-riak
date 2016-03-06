#!/bin/bash


riakdata=/var/lib/riak
riaklogs=/var/log/riak
sudo riak stop
sudo rm -rf ${riakdata}/*
sudo rm -rf ${riaklogs}/*
