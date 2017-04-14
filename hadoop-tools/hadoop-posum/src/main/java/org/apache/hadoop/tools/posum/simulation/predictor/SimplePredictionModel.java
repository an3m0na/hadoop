package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class SimplePredictionModel implements PredictionModel{

  protected int historyBuffer;
  protected Set<String> sourceJobs = new HashSet<>();

  public SimplePredictionModel(int historyBuffer) {
    this.historyBuffer = historyBuffer;
  }

  public Set<String> getSourceJobs() {
    return sourceJobs;
  }

  public void updateModel(List<JobProfile> sources) {
    if (sources == null || sources.size() < 1)
      return;
    for (JobProfile job : sources) {
      updateModel(job);
    }
  }

  public abstract void updateModel(JobProfile source);
}
