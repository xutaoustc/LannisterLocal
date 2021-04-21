#!/bin/bash

cd `dirname $0`
cd ..
HOME=`pwd`

export SERVER_PID=$HOME/bin/linkis.pid

if [[ ! -f "${SERVER_PID}" ]]; then
    echo "No Lannister to stop"
else
    pid=$(cat ${SERVER_PID})
    if [[ -z "${pid}" ]]; then
      echo "No Lannister to stop"
    else
      echo "Stopping Lannister"
      kill "$pid" && rm -f "$SERVER_PID"
    fi
fi