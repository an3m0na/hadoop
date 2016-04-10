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
    public static final String DATABASE_PREFIX = PREFIX + "database.";
    public static final String META_PREFIX = CORE_PREFIX + "meta.";

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

    public static final String RELEVANT_SCHEDULER_CONFIGS = CORE_PREFIX + "conf.relevant";
    public static final String DEFAULT_RELEVANT_SCHEDULER_CONFIGS =
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB + "," +
                    YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + "," +
                    YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB + "," +
                    YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES + "," +
                    YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES + "," +
                    YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME + "," +
                    YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES + "," +
                    YarnConfiguration.RM_SCHEDULER_ADDRESS + "," +
                    YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT + "," +
                    YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS + ",";

    public static final String META_BIND_ADDRESS = META_PREFIX + "bind-host";
    public static final String META_ADDRESS = META_PREFIX + "address";
    public static final String DEFAULT_META_ADDRESS = "0.0.0.0";
    public static final int DEFAULT_META_PORT = 7020;
    public static final String META_SERVICE_THREAD_COUNT = META_PREFIX + "conn.thread-count";
    public static final int DEFAULT_META_SERVICE_THREAD_COUNT = 10;
}
