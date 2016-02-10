package org.apache.hadoop.tools.posum;

/**
 * Created by ane on 2/9/16.
 */
public class POSUMConfiguration {

    public static final String PREFIX = "tools.posum.";
    public static final String MASTER_PREFIX = PREFIX + "master.";
    public static final String MONITOR_PREFIX = PREFIX + "monitor.";
    public static final String PREDICTOR_PREFIX = PREFIX + "predictor.";

    public static final String MASTER_HEARTBEAT_MS = MASTER_PREFIX + "heartbeat.ms";
    public static final int MASTER_HEARTBEAT_MS_DEFAULT = 1000;

    public static final String MONITOR_HEARTBEAT_MS = MONITOR_PREFIX  + "heartbeat.ms";
    public static final int MONITOR_HEARTBEAT_MS_DEFAULT = 1000;

    public static final String BUFFER = PREDICTOR_PREFIX + "buffer";
    public static final int BUFFER_DEFAULT = 2;

    public static final String AVERAGE_JOB_DURATION = PREDICTOR_PREFIX + "avgJobDuration";
    public static final int AVERAGE_JOB_DURATION_DEFAULT = 300000;

    public static final String AVERAGE_TASK_DURATION = PREDICTOR_PREFIX + "avgTaskDuration";
    public static final int AVERAGE_TASK_DURATION_DEFAULT = 20000;

    public static final String PREDICTOR_CLASS = PREDICTOR_PREFIX + "class";

}
