package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Created by ane on 2/9/16.
 */
public class POSUMConfiguration {


    public static Configuration newInstance() {
        Configuration conf = new Configuration();
        conf.addResource("posum-core.xml");
        return conf;
    }

    public static int DEFAULT_BUFFER_SIZE = 1024;

    public static final String PREFIX = "tools.posum.";
    public static final String CORE_PREFIX = PREFIX + "core.";
    public static final String MONITOR_PREFIX = PREFIX + "monitor.";
    public static final String PREDICTOR_PREFIX = PREFIX + "predictor.";
    public static final String SIMULTATOR_PREFIX = PREFIX + "simulator.";
    public static final String DATABASE_PREFIX = PREFIX + "database.";
    public static final String SCHEDULER_PREFIX = CORE_PREFIX + "scheduler.";

    public static final String MASTER_HEARTBEAT_MS = CORE_PREFIX + "heartbeat.ms";
    public static final int MASTER_HEARTBEAT_MS_DEFAULT = 1000;

    public static final String MONITOR_HEARTBEAT_MS = MONITOR_PREFIX + "heartbeat.ms";
    public static final int MONITOR_HEARTBEAT_MS_DEFAULT = 1000;

    public static final String BUFFER = PREDICTOR_PREFIX + "buffer";
    public static final int BUFFER_DEFAULT = 2;

    public static final String AVERAGE_JOB_DURATION = PREDICTOR_PREFIX + "avgJobDuration";
    public static final int AVERAGE_JOB_DURATION_DEFAULT = 300000;

    public static final String AVERAGE_TASK_DURATION = PREDICTOR_PREFIX + "avgTaskDuration";
    public static final int AVERAGE_TASK_DURATION_DEFAULT = 20000;

    public static final String PREDICTOR_CLASS = PREDICTOR_PREFIX + "class";

    public static final String DATABASE_URL = DATABASE_PREFIX + "url";
    public static final String DATABASE_URL_DEFAULT = "127.0.0.1:27017";

    public static final String DATABASE_NAME = DATABASE_PREFIX + "name";
    public static final String DATABASE_NAME_DEFAULT = "posum";

    public static final String DM_BIND_ADDRESS = DATABASE_PREFIX + "bind-host";
    public static final String DM_ADDRESS_DEFAULT = "127.0.0.1";
    public static final int DM_PORT_DEFAULT = 17000;
    public static final String DM_SERVICE_THREAD_COUNT = DATABASE_PREFIX + "conn.thread-count";
    public static final int DM_SERVICE_THREAD_COUNT_DEFAULT = 50;

    public static final String PM_BIND_ADDRESS = CORE_PREFIX + "bind-host";
    public static final String PM_ADDRESS = CORE_PREFIX + "address";
    public static final String PM_ADDRESS_DEFAULT = "127.0.0.1";
    public static final int PM_PORT_DEFAULT = 17010;
    public static final String PM_SERVICE_THREAD_COUNT = CORE_PREFIX + "conn.thread-count";
    public static final int PM_SERVICE_THREAD_COUNT_DEFAULT = 50;

    public static final String POSUM_CONNECT_MAX_WAIT_MS = PREFIX + "conn.max-wait.ms";
    public static final long POSUM_CONNECT_MAX_WAIT_MS_DEFAULT = 15 * 60 * 1000;
    public static final String POSUM_CONNECT_RETRY_INTERVAL_MS = PREFIX + "conn.retry-interval.ms";
    public static final long POSUM_CONNECT_RETRY_INTERVAL_MS_DEFAULT = 30 * 1000;
    public static String CLIENT_FAILOVER_SLEEPTIME_BASE_MS = PREFIX + "failover.sleeptime.base.ms";
    public static String CLIENT_FAILOVER_SLEEPTIME_MAX_MS = PREFIX + "failover.sleeptime.max.ms";
    public static String CLIENT_FAILOVER_MAX_ATTEMPTS = PREFIX + "failover.max.attempts";

    public static final String MONITOR_KEEP_HISTORY = MONITOR_PREFIX + "keep-history";
    public static final boolean MONITOR_KEEP_HISTORY_DEFAULT = true;

    public static final String SCHEDULER_BIND_ADDRESS = SCHEDULER_PREFIX + "bind-host";
    public static final String SCHEDULER_ADDRESS_DEFAULT = "127.0.0.1";
    public static final int SCHEDULER_PORT_DEFAULT = 17020;
    public static final String SCHEDULER_SERVICE_THREAD_COUNT = SCHEDULER_PREFIX + "conn.thread-count";
    public static final int SCHEDULER_SERVICE_THREAD_COUNT_DEFAULT = 10;

    //should be a comma separated list of NAME=CLASS groups
    public static final String SCHEDULER_POLICY_MAP = SCHEDULER_PREFIX + "policies";
    public static final String DEFAULT_POLICY = SCHEDULER_PREFIX + "default";
    public static final String DEFAULT_POLICY_DEFAULT = "FIFO";

    public static final String SIMULATION_INTERVAL = SIMULTATOR_PREFIX + "interval";
    public static final long SIMULATION_INTERVAL_DEFAULT = 10000;

    public static final String SIMULATOR_BIND_ADDRESS = SIMULTATOR_PREFIX + "bind-host";
    public static final String SIMULATOR_ADDRESS_DEFAULT = "127.0.0.1";
    public static final int SIMULATOR_PORT_DEFAULT = 17030;
    public static final String SIMULATOR_SERVICE_THREAD_COUNT = SIMULTATOR_PREFIX + "conn.thread-count";
    public static final int SIMULATOR_SERVICE_THREAD_COUNT_DEFAULT = 10;

    public static final String MASTER_WEBAPP_PORT = CORE_PREFIX + "webapp.port";
    public static final int MASTER_WEBAPP_PORT_DEFAULT = 18000;
    public static final String SCHEDULER_WEBAPP_PORT = SCHEDULER_PREFIX + "webapp.port";
    public static final int SCHEDULER_WEBAPP_PORT_DEFAULT = 18010;

    public static final String SCHEDULER_METRICS_DIR = SCHEDULER_PREFIX + "metrics.dir";
    public static final String SCHEDULER_METRICS_DIR_DEFAULT = "logs";

    public static final String MAX_SCHEDULER_CHOICE_BUFFER = SCHEDULER_PREFIX + "choices.buffer";
    public static final int MAX_SCHEDULER_CHOICE_BUFFER_DEFAULT = 50;


}
