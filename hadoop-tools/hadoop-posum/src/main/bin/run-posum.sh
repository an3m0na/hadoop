#!/bin/bash

###############################################################################
printUsage() {
  echo "Usage: run-posum.sh <OPTIONS>"
  echo "                 --stop"
  echo "                 --restart"
  echo "                 --input-something=<something>"
  echo "                 [--print-simulation]"
  echo                  
}
###############################################################################
parseArgs() {
  for i in $*
  do
    case $i in
    --input-something=*)
      input=${i#*=}
      ;;
      --stop)
      doStop=true
      ;;
      --restart)
      doRestart=true
      ;;
    --print-simulation)
      printsimulation="true"
      ;;
    *)
      echo "Invalid option"
      echo
      printUsage
      exit 1
      ;;
    esac
  done

#  if [[ "${input}" == "" ]] ; then
#    echo "--input-something must be specified"
#    echo
#    printUsage
#    exit 1
#  fi
}
###############################################################################
killProcesses() {
    PID=`jps -l | grep $1 | cut -d " " -f1`
    if [[ $PID != "" ]]; then
        kill -kill $PID
    fi
}
###############################################################################
runMaster() {
    set -e

    PROCESSES=(
        "org.apache.hadoop.tools.posum.core.master.POSUMMaster"
        "org.apache.hadoop.tools.posum.database.master.DataMaster"
        "org.apache.hadoop.tools.posum.simulator.master.SimulationMaster"
        )

    HADOOP_BASE=`which hadoop`
    HADOOP_BASE=`dirname $HADOOP_BASE`/..
    POSUM_CLASSPATH=`hadoop classpath`:${HADOOP_BASE}/share/hadoop/tools/lib/*

    if [[ ${doStop} == true ]] || [[ ${doRestart} == true ]]; then

        echo ">>> Killing POSUM processes"
        for (( i=${#PROCESSES[@]}-1; i>=0; i-- )); do
          echo ${PROCESSES[${i}]}
          killProcesses ${PROCESSES[${i}]}
        done

        if [[ ${doStop} == true ]]; then
            return
        fi
    fi

    echo ">>> Checking mongod"
    #start mongod if not running
    CMD=`pgrep mongod > /dev/null; echo $?`
    if [[ $CMD != 0 ]]; then
      echo " >> Starting mongod"
      mongod --fork --logpath ${HADOOP_BASE}/logs/mongodb.log --bind_ip 127.0.0.1
    fi

    args="-inputsomething ${input}"

    if [[ "${printsimulation}" == "true" ]] ; then
        args="${args} -printsimulation"
    fi

    echo ">>> Starting POSUM processes"
    for (( i=0; i<${#PROCESSES[@]}; i++ )); do
      echo ${PROCESSES[${i}]}
      java -cp ${POSUM_CLASSPATH} -Dhadoop.log.dir="${HADOOP_BASE}/logs" ${PROCESSES[${i}]} & #${args}
      sleep 3
    done

}
###############################################################################

parseArgs "$@"
runMaster

exit 0
