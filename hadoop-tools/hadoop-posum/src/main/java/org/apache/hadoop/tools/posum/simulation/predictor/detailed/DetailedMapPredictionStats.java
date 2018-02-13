package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.AveragingStatEntry;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getSplitSize;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_LOCAL_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_REMOTE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;

class DetailedMapPredictionStats extends PredictionStats<AveragingStatEntry> {

  DetailedMapPredictionStats(int relevance) {
    super(relevance, MAP_DURATION, MAP_RATE, MAP_LOCAL_RATE, MAP_REMOTE_RATE, MAP_SELECTIVITY);
  }

  public void addSamples(JobProfile job, List<TaskProfile> tasks) {

    int sampleNo = job.getCompletedMaps();
    Long avgDuration = job.getAvgMapDuration();

    double rate = 0.0, remoteRate = 0.0, localRate = 0.0;
    int localNo = 0, remoteNo = 0;

    if (sampleNo > 0 && avgDuration != null) {
      addEntry(MAP_DURATION, new AveragingStatEntry(avgDuration, sampleNo));

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
          double crtRate = boundedInputSize / duration;
          rate += crtRate;
          if (task.isLocal() != null) {
            if (task.isLocal()) {
              localRate += crtRate;
              localNo++;
            } else {
              remoteRate += crtRate;
              remoteNo++;
            }
          }
        }
        addEntry(MAP_RATE, new AveragingStatEntry(rate / sampleNo, sampleNo));
        if (job.getMapOutputBytes() != null)
          addEntry(MAP_SELECTIVITY, new AveragingStatEntry(1.0 * job.getMapOutputBytes() / totalSplitSize, sampleNo));
        if (localNo > 0)
          addEntry(MAP_LOCAL_RATE, new AveragingStatEntry(localRate / localNo, localNo));
        if (remoteNo > 0)
          addEntry(MAP_REMOTE_RATE, new AveragingStatEntry(remoteRate / remoteNo, remoteNo));
      }
    }
  }

  @Override
  protected AveragingStatEntry emptyEntry() {
    return new AveragingStatEntry();
  }
}
