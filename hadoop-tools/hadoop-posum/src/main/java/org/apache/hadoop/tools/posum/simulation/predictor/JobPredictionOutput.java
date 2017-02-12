package org.apache.hadoop.tools.posum.simulation.predictor;

public class JobPredictionOutput {
  private Long runtime;

  public JobPredictionOutput(Long runtime) {
    this.runtime = runtime;
  }

  public Long getRuntime() {
    return runtime;
  }

  public void setRuntime(Long runtime) {
    this.runtime = runtime;
  }
}
