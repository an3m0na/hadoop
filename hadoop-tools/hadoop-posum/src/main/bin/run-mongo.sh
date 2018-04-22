#!/bin/bash

###############################################################################
printUsage() {
  echo "Usage: run-mongo.sh <OPTIONS>"
  echo "                 --stop"
  echo "                 --no-clear"
  echo "                 --db-path=<path_to_mongo_data>"
  echo
}
###############################################################################
parseArgs() {
  dbPath="/data/db"
  noClear=false
  for i in $*
  do
    case $i in
      --stop)
      doStop=true
      ;;
      --no-clear)
      noClear=true
      ;;
      --db-path=*)
      dbPath=${i#*=}
      ;;
    *)
      echo "Invalid option"
      echo
      printUsage
      exit 1
      ;;
    esac
  done
}
###############################################################################
runMaster() {

  HADOOP_BIN=${HADOOP_HOME}/bin
  if [[ -z ${HADOOP_LOG_DIR} ]]; then
    HADOOP_LOG_DIR=${HADOOP_HOME}/logs
  fi
  HADOOP_BIN=${HADOOP_HOME}/bin
  POSUM_CLASSPATH=`${HADOOP_BIN}/hadoop classpath`:${HADOOP_HOME}/share/hadoop/tools/lib/*

  echo ">>> Checking mongod..."
  pid=`pgrep mongod`

  set -e

  if [[ ${doStop} == true ]]; then
    echo ">>> Stopping mongod..."
    if [[ ! -z ${pid} ]]; then
      kill -kill ${pid}
	fi
  else
    if [[ -z ${pid} ]]; then
      echo " >> Starting mongod"
      mkdir -p ${HADOOP_HOME}/logs
      if [[ ${noClear} == false ]]; then
        rm -rf ${dbPath}
      fi
      mkdir -p ${dbPath}
      mkdir -p ${HADOOP_LOG_DIR}
      mongod --fork --logpath ${HADOOP_LOG_DIR}/mongodb.log --dbpath ${dbPath} --bind_ip 127.0.0.1
	fi
  fi
}
###############################################################################

parseArgs "$@"
runMaster

exit 0