package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 4/20/16.
 */
public abstract class TaskPredictionPayload implements Payload{

    public static TaskPredictionPayload newInstance(String predictor, String taskId, Long duration) {
        TaskPredictionPayload result = Records.newRecord(TaskPredictionPayload.class);
        result.setPredictor(predictor);
        result.setId(taskId);
        result.setDuration(duration);
        return result;
    }

    public abstract void setPredictor(String predictor);

    public abstract String getPredictor();

    public abstract void setId(String id);

    public abstract String getId();

    public abstract void setDuration(Long duration);

    public abstract Long getDuration();

    @Override
    public String toString() {
        return "TaskPrediction{" + getPredictor() + ": " + getId() + "=" + getDuration() + "}";
    }
}
