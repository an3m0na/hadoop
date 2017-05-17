package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleRateBasedPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.util.Utils.getDoubleField;
import static org.apache.hadoop.tools.posum.common.util.Utils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.Utils.getIntField;
import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.PROFILED_MAPS;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.PROFILED_REDUCES;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.REDUCE_RATE;

public class StandardPredictor extends SimpleRateBasedPredictor<StandardPredictionModel> {

  private static final Log logger = LogFactory.getLog(StandardPredictor.class);

  public StandardPredictor(Configuration conf) {
    super(conf);
  }

  @Override
  protected StandardPredictionModel initializeModel() {
    return new StandardPredictionModel(historyBuffer);
  }

  @Override
  protected Map<String, String> getPredictionProfileUpdates(JobProfile job, boolean fromHistory) {
    Map<String, String> fieldMap = new HashMap<>(FlexKeys.values().length);

    if (!getIntField(job, PROFILED_MAPS.getKey(), 0).equals(job.getCompletedMaps())) {
      // nothing will work if we don't have input size info
      if (job.getTotalInputBytes() != null) {
        long parsedInputBytes = 0L;
        double mapRate = 0.0;
        int taskNo = 0;

        // calculate input by task split size, because it may differ from input byte counts
        List<TaskProfile> tasks = getJobTasks(job.getId(), fromHistory);
        if (tasks == null)
          throw new PosumException("Tasks not found or finished for job " + job.getId());
        for (TaskProfile task : tasks) {
          if (getDuration(task) <= 0 || !task.getType().equals(TaskType.MAP))
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
    if (orZero(job.getAvgMapDuration()) != 0)
      // we have average map duration; assume it will be the same
      return new TaskPredictionOutput(orZero(job.getAvgMapDuration()));
    // we have no information about this job; predict from history
    StandardMapPredictionStats mapStats = model.getRelevantMapStats(job);
    Long inputPerMap = getAvgSplitSize(job);
    if (mapStats == null || inputPerMap == null) {
      return handleNoMapInfo(job);
    }

    double duration = 1.0 * inputPerMap / mapStats.getAvgRate();
    logger.debug("Map duration for " + job.getId() + " should be " + inputPerMap + " / " + mapStats.getAvgRate() + " = " + duration);
    return new TaskPredictionOutput((long)duration);
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

    StandardReducePredictionStats reduceStats = model.getRelevantReduceStats(job);
    if (reduceStats == null) {
      return handleNoReduceInfo(job, avgSelectivity, getDoubleField(job, MAP_RATE.getKey(), null));
    }

    if (avgSelectivity == null || reduceStats.getRelevance() > 1 && reduceStats.getAvgReduceDuration() != null) {
      // our selectivity or reduce rate data is unreliable
      // just return average reduce duration of historical jobs
      logger.debug("Reduce duration calculated as simple average for " + job.getId() + " =  " + reduceStats.getAvgReduceDuration());
      return new TaskPredictionOutput(reduceStats.getAvgReduceDuration().longValue());
    }

    Long duration = getReduceDuration(job, avgSelectivity, reduceStats.getAvgReduceRate());
    if (duration == null)
      return handleNoReduceInfo(job, avgSelectivity, getDoubleField(job, MAP_RATE.getKey(), null));

    logger.debug("Reduce duration computed for " + job.getId() + " as " + duration + "from (remote) avgRate=" + reduceStats.getAvgReduceRate() + " and selectivity=" + avgSelectivity);
    return new TaskPredictionOutput(duration);
  }
}
