package org.apache.hadoop.tools.posum.simulation.predictor;

public class JobPredictionInput {
  private String jobId;

  public JobPredictionInput(String jobId) {
    this.jobId = jobId;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }
}
