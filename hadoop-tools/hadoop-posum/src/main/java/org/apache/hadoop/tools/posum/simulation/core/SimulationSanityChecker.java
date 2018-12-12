package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.core.appmaster.MRAMDaemon;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;

public class SimulationSanityChecker {
  private Map<String, Integer> containers = new HashMap<>();
  private int maxContainers = 1;

  public SimulationSanityChecker(Configuration conf) {
    int maxMemory = conf.getInt(YarnConfiguration.NM_PMEM_MB, YarnConfiguration.DEFAULT_NM_PMEM_MB);
    int minAllocation = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    maxContainers = maxMemory / minAllocation;
  }

  public static class InconsistentStateException extends PosumException {
    public InconsistentStateException(String s) {
      super(s);
    }
  }

  public void countContainers(MRAMDaemon amSim) {
    if (wouldExceedMaxContainers(amSim))
      throw new InconsistentStateException("Too many containers would be scheduled on cluster resources. DB state must be inconsistent");
  }

  private boolean wouldExceedMaxContainers(MRAMDaemon am) {
    if (wouldExceedMaxContainers(am.getHostName()))
      return true;
    for (SimulatedContainer simulatedContainer : am.getContainers()) {
      if (wouldExceedMaxContainers(simulatedContainer.getHostName()))
        return true;
    }
    return false;
  }

  private boolean wouldExceedMaxContainers(String hostName) {
    if (hostName == null)
      return false;
    int newCount = orZero(containers.get(hostName)) + 1;
    if (newCount > maxContainers)
      return true;
    containers.put(hostName, newCount);
    return false;
  }


}
