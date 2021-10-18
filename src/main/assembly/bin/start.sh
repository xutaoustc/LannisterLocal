#!/bin/bash

cd `dirname $0`
cd ..
HOME=`pwd`

# load lannister-env
. $HOME/conf/lannister-env.sh

if [ ! -d $HOME/logs  ];then
  mkdir $HOME/logs
fi

export SERVER_PID=$HOME/bin/lannister.pid
export SERVER_LOG_PATH=$HOME/logs

if test -z "$SERVER_HEAP_SIZE"
then
  export SERVER_HEAP_SIZE="512M"
fi

if test -z "$SERVER_JAVA_OPTS"
then
  export SERVER_JAVA_OPTS="-Xmx$SERVER_HEAP_SIZE -XX:+UseG1GC -Xloggc:$HOME/logs/lannister-gc.log -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=39899 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
fi

if [[ -f "${SERVER_PID}" ]]; then
    pid=$(cat ${SERVER_PID})
    if kill -0 ${pid} >/dev/null 2>&1; then
      echo "Server is already running."
      exit 1
    fi
fi

nohup java $SERVER_JAVA_OPTS -jar $HOME/boot/Lannister-1.0-SNAPSHOT.jar $SERVER_CLASS 2>&1 > $SERVER_LOG_PATH/lannister.out &
pid=$!
if [[ -z "${pid}" ]]; then
    echo "server $SERVER_NAME start failed!"
    exit 1
else
    echo "server $SERVER_NAME start succeeded! full log in $SERVER_LOG_PATH/lannister.out"
    echo $pid > $SERVER_PID
fi