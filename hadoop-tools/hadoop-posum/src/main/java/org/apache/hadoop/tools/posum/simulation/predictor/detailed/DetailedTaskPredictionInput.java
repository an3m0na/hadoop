package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;

public class DetailedTaskPredictionInput extends TaskPredictionInput {
  private Boolean local;

  public DetailedTaskPredictionInput(String taskId) {
    super(taskId);
  }

  public DetailedTaskPredictionInput(String taskId, boolean local) {
    super(taskId);
    this.local = local;
  }

  public Boolean getLocal() {
    return local;
  }

  public void setLocal(Boolean local) {
    this.local = local;
  }
}
