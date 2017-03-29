package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class PosumConfiguration {

  public static Configuration newInstance() {
    Configuration conf = new Configuration();
    conf.addResource("mapred-site.xml");
    conf.addResource("yarn-site.xml");
    conf.addResource("posum-core.xml");
    return conf;
  }

  public static int DEFAULT_BUFFER_SIZE = 1024;

  public static final String PREFIX = "tools.posum.";
  public static final String ORCHESTRATOR_PREFIX = PREFIX + "orchestrator.";
  public static final String MONITOR_PREFIX = PREFIX + "monitor.";
  public static final String PREDICTOR_PREFIX = PREFIX + "predictor.";
  public static final String SIMULATOR_PREFIX = PREFIX + "simulator.";
  public static final String DATABASE_PREFIX = PREFIX + "database.";
  public static final String SCHEDULER_PREFIX = PREFIX + "scheduler.";
  public static final String SIMULTATION_PREFIX = PREFIX + "simulation.";


  public static final String MASTER_HEARTBEAT_MS = ORCHESTRATOR_PREFIX + "heartbeat.ms";
  public static final int MASTER_HEARTBEAT_MS_DEFAULT = 1000;

  public static final String CLUSTER_MONITOR_HEARTBEAT_MS = MONITOR_PREFIX + "cluster.heartbeat.ms";
  public static final int CLUSTER_MONITOR_HEARTBEAT_MS_DEFAULT = 1000;

  public static final String POSUM_MONITOR_HEARTBEAT_MS = MONITOR_PREFIX + "posum.heartbeat.ms";
  public static final int POSUM_MONITOR_HEARTBEAT_MS_DEFAULT = 2000;

  public static final String PREDICTION_BUFFER = PREDICTOR_PREFIX + "buffer";
  public static final int PREDICTION_BUFFER_DEFAULT = 5;

  public static final String AVERAGE_JOB_DURATION = PREDICTOR_PREFIX + "avgJobDuration";
  public static final long AVERAGE_JOB_DURATION_DEFAULT = 360000;

  public static final String AVERAGE_TASK_DURATION = PREDICTOR_PREFIX + "avgTaskDuration";
  public static final int AVERAGE_TASK_DURATION_DEFAULT = 6000;

  public static final String PREDICTOR_CLASS = PREDICTOR_PREFIX + "class";
  public static final String PREDICTOR_TIMEOUT = PREDICTOR_PREFIX + "timeout";
  public static final long PREDICTOR_TIMEOUT_DEFAULT = 10000;


  public static final String DATABASE_URL = DATABASE_PREFIX + "url";
  public static final String DATABASE_URL_DEFAULT = "127.0.0.1:27017";

  public static final String DM_BIND_ADDRESS = DATABASE_PREFIX + "bind-host";
  public static final String DM_ADDRESS = DATABASE_PREFIX + "address";
  public static final int DM_PORT_DEFAULT = 17000;
  public static final String DM_ADDRESS_DEFAULT = "0.0.0.0:" + DM_PORT_DEFAULT;
  public static final String DM_SERVICE_THREAD_COUNT = DATABASE_PREFIX + "conn.thread-count";
  public static final int DM_SERVICE_THREAD_COUNT_DEFAULT = 50;

  public static final String PM_BIND_ADDRESS = ORCHESTRATOR_PREFIX + "bind-host";
  public static final String PM_ADDRESS = ORCHESTRATOR_PREFIX + "address";
  public static final int PM_PORT_DEFAULT = 17010;
  public static final String PM_ADDRESS_DEFAULT = "0.0.0.0:" + PM_PORT_DEFAULT;
  public static final String PM_SERVICE_THREAD_COUNT = ORCHESTRATOR_PREFIX + "conn.thread-count";
  public static final int PM_SERVICE_THREAD_COUNT_DEFAULT = 50;

  public static final String POSUM_CONNECT_MAX_WAIT_MS = PREFIX + "conn.max-wait.ms";
  public static final long POSUM_CONNECT_MAX_WAIT_MS_DEFAULT = 15 * 60 * 1000;
  public static final String POSUM_CONNECT_RETRY_INTERVAL_MS = PREFIX + "conn.retry-interval.ms";
  public static final long POSUM_CONNECT_RETRY_INTERVAL_MS_DEFAULT = 30 * 1000;
  public static String CLIENT_FAILOVER_SLEEPTIME_BASE_MS = PREFIX + "failover.sleeptime.base.ms";
  public static String CLIENT_FAILOVER_SLEEPTIME_MAX_MS = PREFIX + "failover.sleeptime.max.ms";
  public static String CLIENT_FAILOVER_MAX_ATTEMPTS = PREFIX + "failover.max.attempts";

  public static final String MONITOR_KEEP_HISTORY = MONITOR_PREFIX + "history.on";
  public static final boolean MONITOR_KEEP_HISTORY_DEFAULT = true;

  public static final String SCHEDULER_ADDRESS = SCHEDULER_PREFIX + "address";
  public static final int SCHEDULER_PORT_DEFAULT = 17020;
  public static final String SCHEDULER_ADDRESS_DEFAULT = "0.0.0.0:" + SCHEDULER_PORT_DEFAULT;
  public static final String SCHEDULER_SERVICE_THREAD_COUNT = SCHEDULER_PREFIX + "conn.thread-count";
  public static final int SCHEDULER_SERVICE_THREAD_COUNT_DEFAULT = 10;

  //should be a comma separated list of NAME=CLASS groups
  public static final String SCHEDULER_POLICY_MAP = SCHEDULER_PREFIX + "policies";
  public static final String DEFAULT_POLICY = SCHEDULER_PREFIX + "default";
  public static final String DEFAULT_POLICY_DEFAULT = "FIFO";
  public static final String POLICY_SWITCH_ENABLED = SCHEDULER_PREFIX + "policy-switch-enabled";
  public static final boolean POLICY_SWITCH_ENABLED_DEFAULT = true;

  public static final String SIMULATION_INTERVAL = SIMULATOR_PREFIX + "interval";
  public static final long SIMULATION_INTERVAL_DEFAULT = 120000;

  public static final String SIMULATOR_BIND_ADDRESS = SIMULATOR_PREFIX + "bind-host";
  public static final String SIMULATOR_ADDRESS = SIMULATOR_PREFIX + "address";
  public static final int SIMULATOR_PORT_DEFAULT = 17030;
  public static final String SIMULATOR_ADDRESS_DEFAULT = "0.0.0.0:" + SIMULATOR_PORT_DEFAULT;
  public static final String SIMULATOR_SERVICE_THREAD_COUNT = SIMULATOR_PREFIX + "conn.thread-count";
  public static final int SIMULATOR_SERVICE_THREAD_COUNT_DEFAULT = 10;

  public static final String MASTER_WEBAPP_PORT = ORCHESTRATOR_PREFIX + "webapp.port";
  public static final int MASTER_WEBAPP_PORT_DEFAULT = 18000;

  public static final String SCHEDULER_WEBAPP_PORT = SCHEDULER_PREFIX + "webapp.port";
  public static final int SCHEDULER_WEBAPP_PORT_DEFAULT = 18010;

  public static final String DM_WEBAPP_PORT = DATABASE_PREFIX + "webapp.port";
  public static final int DM_WEBAPP_PORT_DEFAULT = 18020;

  public static final String SIMULATOR_WEBAPP_PORT = SIMULATOR_PREFIX + "webapp.port";
  public static final int SIMULATOR_WEBAPP_PORT_DEFAULT = 18030;

  public static final String SCHEDULER_METRICS_ON = SCHEDULER_PREFIX + "metrics.on";
  public static final boolean SCHEDULER_METRICS_ON_DEFAULT = true;

  public static final String FINE_GRAINED_MONITOR = MONITOR_PREFIX + "fine-grained";
  public static final boolean FINE_GRAINED_MONITOR_DEFAULT = true;

  public static final String APP_DEADLINE = YarnConfiguration.YARN_PREFIX + "application.deadline";
  public static final long APP_DEADLINE_DEFAULT = 120000;

  public static final String DC_PRIORITY = SCHEDULER_PREFIX + "dc.priority";
  public static final float DC_PRIORITY_DEFAULT = 0.8f;

  public static final String MIN_EXEC_TIME = SCHEDULER_PREFIX + "min-exec-time";
  public static final long MIN_EXEC_TIME_DEFAULT = 10000;

  public static final String REPRIORITIZE_INTERVAL = SCHEDULER_PREFIX + "reprioritize.ms";
  public static final long REPRIORITIZE_INTERVAL_DEFAULT = 10000;

  public static final String SIMULATION_RUNNER_POOL_SIZE = SIMULTATION_PREFIX + "concurrent.daemons";
  public static final int SIMULATION_RUNNER_POOL_SIZE_DEFAULT = 10;
  public static final String SIMULATION_SCHEDULER = SCHEDULER_PREFIX + "scheduler.class";
  public static final String NM_DAEMON_PREFIX = SIMULTATION_PREFIX + "nm.";
  public static final String NM_DAEMON_MEMORY_MB = NM_DAEMON_PREFIX + "memory.mb";
  public static final int NM_DAEMON_MEMORY_MB_DEFAULT = 10240;
  public static final String NM_DAEMON_VCORES = NM_DAEMON_PREFIX + "vcores";
  public static final int NM_DAEMON_VCORES_DEFAULT = 10;
  public static final String NM_DAEMON_HEARTBEAT_INTERVAL_MS = NM_DAEMON_PREFIX + "heartbeat.interval.ms";
  public static final int NM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
  public static final String AM_DAEMON_PREFIX = SIMULTATION_PREFIX + "am.";
  public static final String AM_DAEMON_HEARTBEAT_INTERVAL_MS = AM_DAEMON_PREFIX + "heartbeat.interval.ms";
  public static final int AM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
  public static final String SIMULATION_CONTAINER_PREFIX = SIMULTATION_PREFIX + "container.";
  public static final String SIMULATION_CONTAINER_MEMORY_MB = SIMULATION_CONTAINER_PREFIX + "memory.mb";
  public static final int SIMULATION_CONTAINER_MEMORY_MB_DEFAULT = 1024;
  public static final String SIMULATION_CONTAINER_VCORES = SIMULATION_CONTAINER_PREFIX + "vcores";
  public static final int SIMULATION_CONTAINER_VCORES_DEFAULT = 1;
}
