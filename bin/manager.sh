#!/bin/bash

# Usage:
#   ./manager.sh build      # deploy riak instances
#   ./manager.sh cluster    # create cluster
#   ./manager.sh load       # load data
#   ./manager.sh destroy    # destroy riak instances


CMD=$1

START=$PWD
SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`
ROOT=`readlink -f $SCRIPTPATH/../..`
MAIN_NODE=159.203.243.11
ALL_NODES=( ${MAIN_NODE} ${RMS_NODE} 159.203.243.9 159.203.243.15 )
DATA=https://s3-us-west-2.amazonaws.com/jeffrey.alan.meyers.bucket/loopdata/loopdata


export DEPLOY_MODE="digitalocean"
export DO_API_TOKEN=${_DO_API_TOKEN}
export DO_SSH_KEY=${_DO_SSH_KEY}


cd ${ROOT}


if [ "${CMD}" = "build" ]; then
    for i in 1 2 3
    do
        if [ -d "node${i}" ]; then
            SERVER_NAME=riak-node${i}
            export DO_SERVER_NAME=${SERVER_NAME}
            echo "Building node: ${DO_SERVER_NAME}"
            cd node${i}
            nohup sh -c "vagrant up --provider=digital_ocean" > build.out 2>&1 &
            cd ..
        else
            echo "Error: directory ./node${i} does not exists"
            exit 1
        fi
    done
fi


if [ "${CMD}" = "destroy" ]; then
    for i in 1 2 3
    do
        if [ -d "node${i}" ]; then
            cd node${i}
            vagrant destroy -f
            cd ..
        else
            echo "Error: directory ./node${i} does not exists"
            exit 1
        fi
    done
fi


if [ "${CMD}" = "cluster" ]; then
    for i in "${ALL_NODES[@]}"
    do
        echo "Clear Node" $i
        user=root
        if [ "${i}" == "${RMS_NODE}" ]; then
            user=riak
        fi
        ssh ${user}@${i} "riak stop; rm -rf /var/lib/riak/*; riak start"
    done
    for i in "${ALL_NODES[@]}"
    do
        echo Node $i
        user=root
        if [ "${i}" == "${RMS_NODE}" ]; then
            user=riak
        fi
        if [ "${i}" != "${MAIN_NODE}" ]; then
            ssh ${user}@${i} "riak-admin cluster join riak@${MAIN_NODE}"
        fi
    done
    ssh root@${MAIN_NODE} "riak-admin cluster plan"
    ssh root@${MAIN_NODE} "riak-admin cluster commit"
    ssh root@${MAIN_NODE} "riak-admin cluster status"
    sleep 15
    ssh root@${MAIN_NODE} "riak-admin cluster status"
    sleep 15
    ssh root@${MAIN_NODE} "riak-admin cluster status"
    sleep 15
    ssh root@${MAIN_NODE} "riak-admin cluster status"
fi


if [ "${CMD}" = "load" ]; then
    for i in 1 2 3
    do
        ip=${ALL_NODES[$(expr ${i} - 1)]}
        echo $ip
        file=${DATA}${i}.csv
        ssh root@${ip} "cd /vagrant/data; wget ${file}"
        load="nohup sh -c \"./bin/load-data.sh\" > build.out 2>&1 &"
        ssh root@${ip} "cd /vagrant; export NODE_ID=${i}; ${load}"
    done
fi

cd $START
