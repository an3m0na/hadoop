package org.apache.hadoop.tools.posum.common;

/**
 * Created by ane on 2/9/16.
 */
public class POSUMConfiguration {

    public static int DEFAULT_BUFFER_SIZE = 1024;

    public static final String PREFIX = "tools.posum.";
    public static final String CORE_PREFIX = PREFIX + "core.";
    public static final String MONITOR_PREFIX = PREFIX + "monitor.";
    public static final String PREDICTOR_PREFIX = PREFIX + "predictor.";
    public static final String DATABASE_PREFIX = PREFIX + "database.";

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
    public static final String DM_ADDRESS = DATABASE_PREFIX + "address";
    public static final String DEFAULT_DM_ADDRESS = "0.0.0.0";
    public static final int DEFAULT_DM_PORT = 7000;
    public static final String DM_SERVICE_THREAD_COUNT = DATABASE_PREFIX + "conn.thread-count";
    public static final int DEFAULT_DM_SERVICE_THREAD_COUNT = 50;

    public static final String PM_BIND_ADDRESS = CORE_PREFIX + "bind-host";
    public static final String PM_ADDRESS = CORE_PREFIX + "address";
    public static final String DEFAULT_PM_ADDRESS = "0.0.0.0";
    public static final int DEFAULT_PM_PORT = 7010;
    public static final String PM_SERVICE_THREAD_COUNT = CORE_PREFIX + "conn.thread-count";
    public static final int DEFAULT_PM_SERVICE_THREAD_COUNT = 50;

    public static final String POSUM_CONNECT_MAX_WAIT_MS = PREFIX + "conn.max-wait.ms";
    public static final long DEFAULT_POSUM_CONNECT_MAX_WAIT_MS = 15 * 60 * 1000;
    public static final String POSUM_CONNECT_RETRY_INTERVAL_MS = PREFIX + "conn.retry-interval.ms";
    public static final long DEFAULT_POSUM_CONNECT_RETRY_INTERVAL_MS = 30 * 1000;
    public static String CLIENT_FAILOVER_SLEEPTIME_BASE_MS = PREFIX + "failover.sleeptime.base.ms";
    public static String CLIENT_FAILOVER_SLEEPTIME_MAX_MS = PREFIX + "failover.sleeptime.max.ms";
    public static String CLIENT_FAILOVER_MAX_ATTEMPTS = PREFIX + "failover.max.attempts";

    public static final String MONITOR_KEEP_HISTORY = MONITOR_PREFIX + "keep-history";
    public static final boolean MONITOR_KEEP_HISTORY_DEFAULT = false;

}
