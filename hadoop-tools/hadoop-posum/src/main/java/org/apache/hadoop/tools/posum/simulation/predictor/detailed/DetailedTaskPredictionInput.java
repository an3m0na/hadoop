package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;

public class DetailedTaskPredictionInput extends TaskPredictionInput {
  private Boolean local;

  public DetailedTaskPredictionInput(JobProfile job, TaskType type, Boolean local) {
    super(job, type);
    this.local = local;
  }

  public DetailedTaskPredictionInput(String taskId, Boolean local) {
    super(taskId);
    this.local = local;
  }

  public DetailedTaskPredictionInput(TaskProfile task, Boolean local) {
    super(task);
    this.local = local;
  }

  public Boolean getLocal() {
    return local;
  }

  public void setLocal(Boolean local) {
    this.local = local;
  }
}
