#!/bin/bash

###############################################################################
printUsage() {
  echo "Usage: posum-master.sh <OPTIONS>"
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

  if [[ "${input}" == "" ]] ; then
    echo "--input-something must be specified"
    echo
    printUsage
    exit 1
  fi
}

###############################################################################
calculateClasspath() {
  HADOOP_BASE=`which hadoop`
  HADOOP_BASE=`dirname $HADOOP_BASE`
  DEFAULT_LIBEXEC_DIR=${HADOOP_BASE}/../libexec
  HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
  . $HADOOP_LIBEXEC_DIR/hadoop-config.sh
  export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${TOOL_PATH}:html"
}
###############################################################################
runMaster() {

  args="-inputsomething ${input}"

  if [[ "${printsimulation}" == "true" ]] ; then
    args="${args} -printsimulation"
  fi

  hadoop org.apache.hadoop.tools.posum.POSUMMaster ${args}
}
###############################################################################

calculateClasspath
parseArgs "$@"
runMaster

exit 0
