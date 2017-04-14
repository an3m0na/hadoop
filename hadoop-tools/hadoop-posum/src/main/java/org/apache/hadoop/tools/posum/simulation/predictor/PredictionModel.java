package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.List;
import java.util.Set;

public interface PredictionModel {

  Set<String> getSourceJobs();

  void updateModel(List<JobProfile> sources);

  void updateModel(JobProfile source);
}
