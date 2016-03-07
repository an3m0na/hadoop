package org.apache.hadoop.tools.posum.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/3/16.
 */
public class Utils {

    private static Log logger = LogFactory.getLog(Utils.class);

    public static TaskType getTaskTypeFromId(String id) {
        try {
            String[] parts = id.split("_");
            return "m".equals(parts[parts.length - 2]) ? TaskType.MAP : TaskType.REDUCE;
        } catch (Exception e) {
            logger.error("[Utils] Could not get task type from id " + id, e);
        }
        return null;
    }

    public static ApplicationId parseApplicationId(String id) {
        try {
            String[] parts = id.split("_");
            return ApplicationId.newInstance(Long.parseLong(parts[1]),
                    Integer.parseInt(parts[2]));
        } catch (Exception e) {
            logger.error("[Utils] Could not get task type from id " + id, e);
        }
        return null;
    }

    public static JobId parseJobId(String appId, String id) {
        try {
            String[] parts = id.split("_");
            JobId jobId = Records.newRecord(JobId.class);
            jobId.setAppId(parseApplicationId(appId));
            jobId.setId(Integer.parseInt(parts[parts.length - 1]));
            return jobId;
        } catch (Exception e) {
            logger.error("[Utils] Could not get task type from id " + id, e);
        }
        return null;
    }

    public static TaskId parseTaskId(String appId, String jobId, String id) {
        try {
            String[] parts = id.split("_");
            TaskId taskId = Records.newRecord(TaskId.class);
            taskId.setJobId(parseJobId(appId, jobId));
            taskId.setTaskType("m".equals(parts[parts.length - 2]) ? TaskType.MAP : TaskType.REDUCE);
            taskId.setId(Integer.parseInt(parts[parts.length - 1]));
            return taskId;
        } catch (Exception e) {
            logger.error("[Utils] Could not get task type from id " + id, e);
        }
        return null;
    }
}
