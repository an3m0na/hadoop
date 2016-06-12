package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 4/20/16.
 */
public abstract class TaskPrediction {

    public static TaskPrediction newInstance(String taskId, Long duration) {
        TaskPrediction result = Records.newRecord(TaskPrediction.class);
        result.setId(taskId);
        result.setDuration(duration);
        return result;
    }

    public abstract void setId(String id);

    public abstract String getId();

    public abstract void setDuration(Long duration);

    public abstract Long getDuration();

    @Override
    public String toString() {
        return "TaskPrediction{" + getId() + "=" + getDuration() + "}";
    }
}
