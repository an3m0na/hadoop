package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;

public abstract class SimpleMapPredictionStats extends PredictionStats {

  public SimpleMapPredictionStats(int relevance, Enum... statKeys) {
    super(relevance, statKeys);
  }

  public void addSamples(JobProfile job, List<TaskProfile> tasks) {
    for (TaskProfile task : tasks) {
      if (getDuration(task) <= 0 || !task.getType().equals(TaskType.MAP))
        continue;
      // this is a finished map task; add as sample
      addSample(job, task);
    }
  }

  public abstract void addSample(JobProfile job, TaskProfile task);
}
