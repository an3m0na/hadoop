package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public class BasicMRPredictor extends JobBehaviorPredictor {

    private static final Log logger = LogFactory.getLog(BasicMRPredictor.class);

    private List<JobProfile> getComparableProfiles(JobProfile job, TaskType type) {
        // get past jobs with the same name
        List<JobProfile> comparable = getDataStore().find(
                DataEntityType.JOB_HISTORY,
                type.equals(TaskType.MAP) ? "mapperClass" : "reducerClass",
                type.equals(TaskType.MAP) ? job.getMapperClass() : job.getReducerClass(),
                0,
                conf.getInt(POSUMConfiguration.PREDICTION_BUFFER,
                        POSUMConfiguration.PREDICTION_BUFFER_DEFAULT)
        );
        if (comparable.size() < 1) {
            // get past jobs at least by the same user
            comparable = getDataStore().find(
                    DataEntityType.JOB_HISTORY,
                    "user",
                    job.getUser(),
                    0,
                    conf.getInt(POSUMConfiguration.PREDICTION_BUFFER,
                            POSUMConfiguration.PREDICTION_BUFFER_DEFAULT)
            );
        }
        return comparable;
    }

    @Override
    public Long predictJobDuration(String jobId) {
        JobProfile job = getDataStore().findById(DataEntityType.JOB, jobId);
        return predictMapTaskDuration(job) * job.getTotalMapTasks() +
                predictReduceTaskDuration(job) * job.getTotalReduceTasks();
    }

    @Override
    public Long predictLocalMapTaskDuration(String jobId) {
        return predictTaskDuration(jobId, TaskType.MAP);
    }


    private Long predictMapTaskDuration(JobProfile job) {
        if (job.getAvgMapDuration() != 0)
            return job.getAvgMapDuration();
        // we have no information about this job; predict from history
        List<JobProfile> comparable = getComparableProfiles(job, TaskType.MAP);
        if (comparable.size() < 1) {
            logger.debug("No map history data for job " + job.getId() + ". Using default");
            return conf.getLong(POSUMConfiguration.AVERAGE_JOB_DURATION,
                    POSUMConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
        }
        double avgMapRate = 0;
        for (JobProfile profile : comparable) {
            avgMapRate += 1.0 * profile.getInputBytes() / profile.getAvgMapDuration();
        }
        avgMapRate /= comparable.size();
        logger.debug("Map duration for " + job.getId() + " should be " + job.getTotalInputBytes() + " / " + avgMapRate);
        return Double.valueOf(job.getTotalInputBytes() / avgMapRate).longValue();
    }

    private double calculateMapTaskSelectivity(JobProfile job) {
        if (job.getCompletedMaps() > 0)
            // we know the current selectivity
            return 1.0 * job.getMapOutputBytes() / job.getInputBytes();

        // we have to compute selectivity from the map history
        List<JobProfile> comparable = getComparableProfiles(job, TaskType.MAP);
        if (comparable.size() < 1) {
            return 1.0;
        }
        double avgSelectivity = 0;
        for (JobProfile profile : comparable) {
            avgSelectivity += 1.0 * profile.getMapOutputBytes() / profile.getInputBytes();
        }
        return avgSelectivity / comparable.size();
    }

    private Long predictReduceTaskDuration(JobProfile job) {
        if (job.getAvgReduceDuration() != 0)
            return job.getAvgReduceDuration();
        List<JobProfile> comparable = getComparableProfiles(job, TaskType.REDUCE);
        if (comparable.size() < 1 || !comparable.get(0).getReducerClass().equals(job.getReducerClass())) {
            // non-existent or irrelevant historical data
            if (job.getCompletedMaps() == 0) {
                logger.debug("No data to compute reduce for job " + job.getName() + ". Using default");
                return conf.getLong(POSUMConfiguration.AVERAGE_JOB_DURATION,
                        POSUMConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
            }
            logger.debug("Reduce duration computed based on map data for job" + job.getId());
            double mapRate = 1.0 * job.getInputBytes() / job.getAvgMapDuration();
            double selectivity = 1.0 * job.getMapOutputBytes() / job.getInputBytes();
            // we assume the reduce processing rate is the same as the map processing rate
            return Double.valueOf(job.getTotalInputBytes() * selectivity / mapRate).longValue();
        }
        // we have reduce history
        // we need to compute the selectivity, either currently or from the map history
        double avgSelectivity = calculateMapTaskSelectivity(job);
        double avgReduceRate = 0;
        for (JobProfile profile : comparable) {
            avgReduceRate += 1.0 * profile.getReduceInputBytes() / profile.getAvgReduceDuration();
        }
        avgReduceRate /= comparable.size();
        double inputPerTask = job.getTotalInputBytes() * avgSelectivity / job.getTotalReduceTasks();
        logger.debug("Reduce duration for " + job.getId() + " should be " + inputPerTask + " / " + avgReduceRate);
        return Double.valueOf(inputPerTask / avgReduceRate).longValue();
    }

    @Override
    public Long predictTaskDuration(String jobId, TaskType type) {
        JobProfile job = getDataStore().findById(DataEntityType.JOB, jobId);
        if (type.equals(TaskType.MAP)) {
            return predictMapTaskDuration(job);
        }
        return predictReduceTaskDuration(job);
    }

    @Override
    public Long predictTaskDuration(String taskId) {
        TaskProfile task = getDataStore().findById(DataEntityType.TASK, taskId);
        if (task == null)
            throw new POSUMException("Task could not be found with id: " + taskId);
        return predictTaskDuration(task.getJobId(), task.getType());
    }

}
