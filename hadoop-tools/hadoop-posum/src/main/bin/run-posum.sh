#!/bin/bash

###############################################################################
printUsage() {
  echo "Usage: run-posum.sh <OPTIONS>"
  echo "                 --stop"
  echo "                 --restart"
  echo "                 --maxmem=<maximum JVM memory for each process in MB>"
  echo                  
}
###############################################################################
parseArgs() {
  maxmem=2048
  for i in $*
  do
    case $i in
      --maxmem=*)
      maxmem=${i#*=}
      ;;
      --stop)
      doStop=true
      ;;
      --restart)
      doRestart=true
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
        "org.apache.hadoop.tools.posum.orchestration.master.OrchestrationMaster"
        "org.apache.hadoop.tools.posum.data.master.DataMaster"
        "org.apache.hadoop.tools.posum.simulation.master.SimulationMaster"
        )

    HADOOP_BIN=${HADOOP_HOME}/bin
    POSUM_CLASSPATH=`${HADOOP_BIN}/hadoop classpath`:${HADOOP_HOME}/share/hadoop/tools/lib/*

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

    echo ">>> Starting POSUM processes" > run-posum.out
    for (( i=0; i<${#PROCESSES[@]}; i++ )); do
      echo ">> Starting ${PROCESSES[${i}]}" >> run-posum.out
      java -cp ${POSUM_CLASSPATH} -Xmx${maxmem}M -Dhadoop.log.dir="${HADOOP_HOME}/logs" ${PROCESSES[${i}]} >> run-posum.out 2>&1 &
      sleep 3
    done

}
###############################################################################

parseArgs "$@"
runMaster

exit 0
