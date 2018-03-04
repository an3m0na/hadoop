package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.PredictionStats;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.RegressionWithFallbackStatEntry;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getSplitSize;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_LOCAL_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_REMOTE_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;

class DetailedMapPredictionStats extends PredictionStats<RegressionWithFallbackStatEntry> {

  DetailedMapPredictionStats(int relevance) {
    super(relevance, MAP_DURATION, MAP_LOCAL_DURATION, MAP_REMOTE_DURATION, MAP_SELECTIVITY);
  }

  public void addSamples(JobProfile job, List<TaskProfile> tasks) {

    int sampleNo = job.getCompletedMaps();
    Long avgDuration = job.getAvgMapDuration();

    if (sampleNo > 0 && avgDuration != null) {
      RegressionWithFallbackStatEntry mapDurationEntry = new RegressionWithFallbackStatEntry();
      RegressionWithFallbackStatEntry localDurationEntry = new RegressionWithFallbackStatEntry();
      RegressionWithFallbackStatEntry remoteDurationEntry = new RegressionWithFallbackStatEntry();

      if (job.getTotalSplitSize() != null) { // nothing will work if we don't know the total split size
        long totalSplitSize = 0;
        for (TaskProfile task : tasks) {
          if (getDuration(task) <= 0 || !task.getType().equals(TaskType.MAP))
            continue;
          Long inputSize = getSplitSize(task, job);
          totalSplitSize += inputSize;
          Double duration = getDuration(task).doubleValue();
          // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
          Double boundedInputSize = Math.max(inputSize, 1.0);
          if ("org.apache.hadoop.mapreduce.lib.map.RegexMapper".equals(job.getMapperClass()))
            System.out.println("Adding " + boundedInputSize + "  with " + duration);
          mapDurationEntry.addSample(boundedInputSize, duration);
          if (task.isLocal() != null) {
            if (task.isLocal()) {
              localDurationEntry.addSample(boundedInputSize, duration);
            } else {
              remoteDurationEntry.addSample(boundedInputSize, duration);
            }
          }
        }
        addEntry(MAP_DURATION, mapDurationEntry);
        if (job.getMapOutputBytes() != null) {
          Double avgInputSize = 1.0 * totalSplitSize / mapDurationEntry.getSampleSize();
          Double avgOutputSize = 1.0 * job.getMapOutputBytes() / sampleNo;
          addEntry(MAP_SELECTIVITY, new RegressionWithFallbackStatEntry(avgOutputSize / avgInputSize, sampleNo));
        }
        addEntry(MAP_LOCAL_DURATION, localDurationEntry);
        addEntry(MAP_REMOTE_DURATION, remoteDurationEntry);
      } else {
        addEntry(MAP_DURATION, new RegressionWithFallbackStatEntry(avgDuration.doubleValue(), sampleNo));
      }
    }
  }

  @Override
  protected RegressionWithFallbackStatEntry emptyEntry() {
    return new RegressionWithFallbackStatEntry();
  }
}
