#!/bin/bash

file=$1

function check {
    # find last succesfully loaded record
    last_load=`cat $1 | awk '/ /{print $3}' | grep -o '[0-9]*' | tail -n 1`
    
    # check for errors in output logs
    errors=`cat $1 | grep -ic 'error\|failure'`
    if [ ${errors} -gt 0 ]
    then
	echo "ERRORS"
    else
	echo "Success!"
    fi
    
    echo "Records Loaded: ${last_load}"
}

check ${file}
