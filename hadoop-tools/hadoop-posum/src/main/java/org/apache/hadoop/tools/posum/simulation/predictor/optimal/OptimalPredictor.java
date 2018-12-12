package org.apache.hadoop.tools.posum.simulation.predictor.optimal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleRateBasedPredictor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDoubleField;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getIntField;
import static org.apache.hadoop.tools.posum.simulation.predictor.optimal.FlexKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.optimal.FlexKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.optimal.FlexKeys.PROFILED_MAPS;
import static org.apache.hadoop.tools.posum.simulation.predictor.optimal.FlexKeys.PROFILED_REDUCES;
import static org.apache.hadoop.tools.posum.simulation.predictor.optimal.FlexKeys.REDUCE_RATE;

public class OptimalPredictor extends SimpleRateBasedPredictor<OptimalPredictionModel> {

  private static final Log logger = LogFactory.getLog(OptimalPredictor.class);

  public OptimalPredictor(Configuration conf) {
    super(conf);
  }

  @Override
  protected OptimalPredictionModel initializeModel() {
    return new OptimalPredictionModel(historyBuffer);
  }

  @Override
  protected Map<String, String> getPredictionProfileUpdates(JobProfile job, boolean fromHistory) {
    Map<String, String> fieldMap = new HashMap<>(FlexKeys.values().length);

    if (!getIntField(job, PROFILED_MAPS.getKey(), 0).equals(job.getCompletedMaps())) {
      // nothing will work if we don't have input size info
      if (job.getTotalSplitSize() != null) {
        long parsedInputBytes = 0L;
        double mapRate = 0.0;
        int taskNo = 0;

        // calculate input by task split size, because it may differ from input byte counts
        List<TaskProfile> tasks = getJobTasks(job.getId(), fromHistory);
        if (tasks == null)
          throw new PosumException("Tasks not found or finished for job " + job.getId());
        for (TaskProfile task : tasks) {
          if (!task.isFinished() || !task.getType().equals(TaskType.MAP))
            continue;
          // this is a finished map task; calculate general, local and remote processing rates
          Long taskInput = getSplitSize(task, job);
          parsedInputBytes += taskInput;
          mapRate += 1.0 * taskInput / getDuration(task);
          taskNo++;
        }
        if (taskNo != 0) {
          fieldMap.put(MAP_RATE.getKey(), Double.toString(mapRate / taskNo));
          if (job.getMapOutputBytes() != null) {
            fieldMap.put(MAP_SELECTIVITY.getKey(),
              // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
              Double.toString(1.0 * orZero(job.getMapOutputBytes()) / parsedInputBytes));
          }
        }
      }

      fieldMap.put(PROFILED_MAPS.getKey(), Integer.toString(job.getCompletedMaps()));
    }

    if (!getIntField(job, PROFILED_REDUCES.getKey(), 0).equals(job.getCompletedReduces())) {
      if (job.getReduceInputBytes() != null && job.getAvgReduceDuration() != null) {
        long inputSize = Math.max(job.getReduceInputBytes() / job.getTotalReduceTasks(), 1);
        double reduceRate = 1.0 * inputSize / job.getAvgReduceDuration();
        fieldMap.put(REDUCE_RATE.getKey(), Double.toString(reduceRate));
      }

      fieldMap.put(PROFILED_REDUCES.getKey(), Integer.toString(job.getCompletedReduces()));
    }

    return fieldMap;
  }

  @Override
  protected TaskPredictionOutput predictMapTaskBehavior(TaskPredictionInput input) {
    JobProfile job = input.getJob();
    OptimalMapPredictionStats jobStats = new OptimalMapPredictionStats(1, 0);
    jobStats.addSource(job);

    Double rate = jobStats.getAvgRate();

    if (rate == null) {
      // we don't know the rate of that type
      // get the appropriate average map processing rate from history
      OptimalMapPredictionStats mapStats = model.getRelevantMapStats(job);
      if (mapStats == null || mapStats.getRelevance() > 1 || mapStats.getAvgRate() == null) {
        // history is not relevant or we don't have enough for a detailed calculation
        if (job.getAvgMapDuration() != null)
          return new TaskPredictionOutput(job.getAvgMapDuration());
        return handleNoMapInfo(job);
      }
      rate = mapStats.getAvgRate();
      if (rate == null)
        return handleNoMapInfo(job);
    }
    // multiply by how much input each task has
    Long inputPerMap = getSplitSize(input.getTask(), job);
    double duration = 1.0 * inputPerMap / rate;
    logger.trace("Map duration for " + job.getId() + " should be " + inputPerMap + " / " + rate + " = " + duration);
    return new TaskPredictionOutput((long) duration);
  }

  @Override
  protected TaskPredictionOutput predictReduceTaskBehavior(TaskPredictionInput input) {
    JobProfile job = input.getJob();
    if (job.getAvgReduceDuration() != null)
      return new TaskPredictionOutput(job.getAvgReduceDuration());

    // calculate average duration based on map selectivity and historical processing rates
    Double avgSelectivity = getMapTaskSelectivity(
      job,
      model.getRelevantMapStats(job),
      MAP_SELECTIVITY.getKey()
    );

    OptimalReducePredictionStats reduceStats = model.getRelevantReduceStats(job);
    if (reduceStats == null) {
      return handleNoReduceInfo(job, avgSelectivity, getDoubleField(job, MAP_RATE.getKey(), null));
    }

    if (avgSelectivity == null || reduceStats.getRelevance() > 1 && reduceStats.getAvgReduceDuration() != null) {
      // our selectivity or reduce rate data is unreliable
      // just return average reduce duration of historical jobs
      logger.trace("Reduce duration calculated as simple average for " + job.getId() + " =  " + reduceStats.getAvgReduceDuration());
      return new TaskPredictionOutput(reduceStats.getAvgReduceDuration().longValue());
    }

    Long duration = getReduceDuration(job, avgSelectivity, reduceStats.getAvgReduceRate());
    if (duration == null)
      return handleNoReduceInfo(job, avgSelectivity, getDoubleField(job, MAP_RATE.getKey(), null));

    logger.trace("Reduce duration computed for " + job.getId() + " as " + duration + "from (remote) avgRate=" + reduceStats.getAvgReduceRate() + " and selectivity=" + avgSelectivity);
    return new TaskPredictionOutput(duration);
  }
}
