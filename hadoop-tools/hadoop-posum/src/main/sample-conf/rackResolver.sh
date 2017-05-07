  #!/bin/bash

  regex="node3([1-4]0|[0-3][1-9]|4[1-8])"

  while [ $# -gt 0 ] ; do
    nodeArg=$1
    # node301-node324 -> rack C03,  node325-node348 -> rack C04
    if [[ ${nodeArg} =~ ${regex} ]]; then
      name="${BASH_REMATCH[1]}"
      if [[ "${name}" -lt "25" ]]; then
        rack="C03"
      else
        rack="C04"
      fi
      /bin/echo -n "/${rack} "
    else
      /bin/echo -n "/rack-unknown "
    fi
    shift 
  done