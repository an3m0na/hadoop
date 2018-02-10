package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.Map;

public abstract class PredictionProfile {
  private JobProfile job;

  public PredictionProfile(JobProfile job) {
    this.job = job;
  }

  public JobProfile getJob() {
    return job;
  }

  public abstract Map<String, String> serialize();

  public abstract void deserialize();
}
