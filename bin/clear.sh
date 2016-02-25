#!/bin/bash


riakdata=/var/lib/riak
sudo riak stop
sudo rm -rf ${riakdata}/*
sudo riak start
