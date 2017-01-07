package org.apache.hadoop.tools.posum.simulation.predictor;

public class TaskPredictionOutput {
    private Long duration;


    public TaskPredictionOutput(Long duration) {
        this.duration = duration;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "TaskPredictionOutput{" +
                "duration=" + duration +
                '}';
    }
}
