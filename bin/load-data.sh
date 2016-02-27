#!/bin/bash


SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`

cd $SCRIPTPATH/..

python src/main.py load
python src/main.py query
