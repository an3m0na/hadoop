package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.AveragingStatEntryImpl;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.PredictionStats;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getSplitSize;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.MAP_SELECTIVITY;

class StandardMapPredictionStats extends PredictionStats<AveragingStatEntryImpl> {

  StandardMapPredictionStats(int relevance) {
    super(relevance, MAP_DURATION, MAP_RATE, MAP_SELECTIVITY);
  }

  public void addSamples(JobProfile job, List<TaskProfile> tasks) {
    int sampleNo = job.getCompletedMaps();
    Long avgDuration = job.getAvgMapDuration();

    if (sampleNo > 0 && avgDuration != null) {
      addEntry(MAP_DURATION, new AveragingStatEntryImpl(avgDuration, sampleNo));

      if (job.getTotalSplitSize() != null) { // nothing will work if we don't know the total split size
        Long totalInputSize = 0L;
        int mapNo = 0; // use a separate map counter in case tasks finished in between getting job info and task info
        for (TaskProfile task : tasks) {
          if (getDuration(task) > 0 && task.getType().equals(TaskType.MAP)) {
            totalInputSize += getSplitSize(task, job);
            mapNo++;
          }
        }
        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        Double avgInputSize = Math.max(1.0 * totalInputSize / mapNo, 1.0);
        addEntry(MAP_RATE, new AveragingStatEntryImpl(avgInputSize / avgDuration, sampleNo));
        if (job.getMapOutputBytes() != null) {
          Double avgOutputSize = 1.0 * job.getMapOutputBytes() / sampleNo;
          addEntry(MAP_SELECTIVITY, new AveragingStatEntryImpl(avgOutputSize / avgInputSize, sampleNo));
        }
      }
    }
  }

  @Override
  protected AveragingStatEntryImpl emptyEntry() {
    return new AveragingStatEntryImpl();
  }
}
