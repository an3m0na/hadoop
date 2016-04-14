package org.apache.hadoop.tools.posum.common.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
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
            throw new POSUMException("Id parse exception for " + id, e);
        }
    }

    public static ApplicationId parseApplicationId(String id) {
        try {
            String[] parts = id.split("_");
            return ApplicationId.newInstance(Long.parseLong(parts[1]),
                    Integer.parseInt(parts[2]));
        } catch (Exception e) {
            throw new POSUMException("Id parse exception for " + id, e);
        }
    }

    public static JobId parseJobId(String appId, String id) {
        try {
            String[] parts = id.split("_");
            JobId jobId = Records.newRecord(JobId.class);
            jobId.setAppId(parseApplicationId(appId));
            jobId.setId(Integer.parseInt(parts[parts.length - 1]));
            return jobId;
        } catch (Exception e) {
            throw new POSUMException("Id parse exception for " + id, e);
        }
    }

    public static TaskId parseTaskId(String appId, String id) {
        try {
            String[] parts = id.split("_");
            TaskId taskId = Records.newRecord(TaskId.class);
            taskId.setJobId(parseJobId(appId, parts[0] + "_" + parts[1] + "_" + parts[2]));
            taskId.setTaskType("m".equals(parts[3]) ? TaskType.MAP : TaskType.REDUCE);
            taskId.setId(Integer.parseInt(parts[4]));
            return taskId;
        } catch (Exception e) {
            throw new POSUMException("Id parse exception for " + id, e);
        }
    }

    public static TaskId parseTaskId(String id) {
        try {
            String[] parts = id.split("_");
            TaskId taskId = Records.newRecord(TaskId.class);
            taskId.setJobId(parseJobId("application_" + parts[1] + "0000", parts[0] + "_" + parts[1] + "_" + parts[2]));
            taskId.setTaskType("m".equals(parts[3]) ? TaskType.MAP : TaskType.REDUCE);
            taskId.setId(Integer.parseInt(parts[4]));
            return taskId;
        } catch (Exception e) {
            throw new POSUMException("Id parse exception for " + id, e);
        }
    }

    public static <T> SimpleResponse<T> handleError(String type, SimpleResponse<T> response) {
        if (!response.getSuccessful()) {
            throw new POSUMException("Request type " + type + " returned with error: " + "\n" + response.getText(),
                    response.getException());
        }
        return response;
    }
}
