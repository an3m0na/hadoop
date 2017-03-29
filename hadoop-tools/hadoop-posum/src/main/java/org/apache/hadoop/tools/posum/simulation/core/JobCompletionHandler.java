package org.apache.hadoop.tools.posum.simulation.core;

public interface JobCompletionHandler {
  void handle(String jobId);
}
