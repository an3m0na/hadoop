package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;

public class SimulationConfiguration {
  public static final String PREFIX = PosumConfiguration.SIMULATOR_PREFIX;
  // runner
  public static final String RUNNER_PREFIX = PREFIX + "runner.";
  public static final String RUNNER_POOL_SIZE = RUNNER_PREFIX + "pool.size";
  public static final int RUNNER_POOL_SIZE_DEFAULT = 10;
  // scheduler
  public static final String SCHEDULER_PREFIX = PREFIX + "scheduler.";
  public static final String RM_SCHEDULER = SCHEDULER_PREFIX + "class";
  // nm
  public static final String NM_PREFIX = PREFIX + "nm.";
  public static final String NM_MEMORY_MB = NM_PREFIX + "memory.mb";
  public static final int NM_MEMORY_MB_DEFAULT = 10240;
  public static final String NM_VCORES = NM_PREFIX + "vcores";
  public static final int NM_VCORES_DEFAULT = 10;
  public static final String NM_HEARTBEAT_INTERVAL_MS = NM_PREFIX
    + "heartbeat.interval.ms";
  public static final int NM_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
  // am
  public static final String AM_PREFIX = PREFIX + "am.";
  public static final String AM_HEARTBEAT_INTERVAL_MS = AM_PREFIX
    + "heartbeat.interval.ms";
  public static final int AM_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
  // container
  public static final String CONTAINER_PREFIX = PREFIX + "container.";
  public static final String CONTAINER_MEMORY_MB = CONTAINER_PREFIX
    + "memory.mb";
  public static final int CONTAINER_MEMORY_MB_DEFAULT = 1024;
  public static final String CONTAINER_VCORES = CONTAINER_PREFIX + "vcores";
  public static final int CONTAINER_VCORES_DEFAULT = 1;
}
