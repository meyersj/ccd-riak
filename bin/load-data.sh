#!/bin/bash


SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`

cd $SCRIPTPATH/..

python src/main.py load
echo "FINISHED LOADING"
python src/main.py query
