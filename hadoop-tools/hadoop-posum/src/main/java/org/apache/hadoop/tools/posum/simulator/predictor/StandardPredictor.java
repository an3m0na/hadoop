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
public class StandardPredictor extends JobBehaviorPredictor {

    private static final Log logger = LogFactory.getLog(StandardPredictor.class);

    private List<JobProfile> getComparableProfiles(JobProfile job, TaskType type) {
        int bufferLimit = conf.getInt(POSUMConfiguration.PREDICTION_BUFFER,
                POSUMConfiguration.PREDICTION_BUFFER_DEFAULT);
        // get past jobs with the same name
        List<JobProfile> comparable = getDataStore().find(
                DataEntityType.JOB_HISTORY,
                type.equals(TaskType.MAP) ? "mapperClass" : "reducerClass",
                type.equals(TaskType.MAP) ? job.getMapperClass() : job.getReducerClass(),
                -bufferLimit,
                bufferLimit
        );
        if (comparable.size() < 1) {
            // get past jobs at least by the same user
            comparable = getDataStore().find(
                    DataEntityType.JOB_HISTORY,
                    "user",
                    job.getUser(),
                    -bufferLimit,
                    bufferLimit
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
            logger.debug("No map history data for " + job.getId() + ". Using default");
            return conf.getLong(POSUMConfiguration.AVERAGE_TASK_DURATION,
                    POSUMConfiguration.AVERAGE_TASK_DURATION_DEFAULT);
        }
        double avgMapRate = 0;
        for (JobProfile profile : comparable) {
            //restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
            avgMapRate += 1.0 * Math.max(profile.getTotalInputBytes(), profile.getTotalMapTasks()) / profile.getAvgMapDuration();
        }
        avgMapRate /= comparable.size();
        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        Double duration = Math.max(job.getTotalInputBytes(), job.getTotalMapTasks()) / avgMapRate;
        logger.debug("Map duration for " + job.getId() + " should be " + job.getTotalInputBytes() + " / " + avgMapRate + " = " + duration);
        return duration.longValue();
    }

    private double calculateMapTaskSelectivity(JobProfile job) {
        // we try to compute selectivity from the map history
        List<JobProfile> comparable = getComparableProfiles(job, TaskType.MAP);
        if (comparable.size() < 1 || !comparable.get(0).getMapperClass().equals(job.getMapperClass())) {
            // there is no history, or it is not relevant for selectivity
            if (job.getCompletedMaps() > 0) {
                // we know the current selectivity
                // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
                Long parsedInputBytes = job.getCompletedMaps() * Math.max(job.getTotalInputBytes() / job.getTotalMapTasks(), 1);
                return 1.0 * job.getMapOutputBytes() / parsedInputBytes;
            }
            // we don't know anything about selectivity
            return 0;
        }
        double avgSelectivity = 0;
        for (JobProfile profile : comparable) {
            // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
            avgSelectivity += 1.0 * profile.getMapOutputBytes() / Math.max(profile.getTotalInputBytes(), profile.getTotalMapTasks());
        }
        return avgSelectivity / comparable.size();
    }

    private Long handleNoReduceHistory(JobProfile job, Double avgSelectivity) {
        if (avgSelectivity == 0 || job.getCompletedMaps() == 0) {
            // our selectivity or map rate data is unreliable
            // just return default duration
            logger.debug("No data to compute reduce for " + job.getName() + ". Using default");
            return conf.getLong(POSUMConfiguration.AVERAGE_TASK_DURATION,
                    POSUMConfiguration.AVERAGE_TASK_DURATION_DEFAULT);
        }

        // calculate the current map rate and assume reduce rate is the same

        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        Long parsedInputBytes = job.getCompletedMaps() * Math.max(job.getTotalInputBytes() / job.getTotalMapTasks(), 1);
        double mapRate = 1.0 * parsedInputBytes / job.getAvgMapDuration();
        // we assume the reduce processing rate is the same as the map processing rate
        Double inputPerTask = Math.max(job.getTotalInputBytes(), 1) * avgSelectivity / job.getTotalReduceTasks();
        Double duration = inputPerTask / mapRate;
        logger.debug("Reduce duration computed based on map data for " + job.getId() + " as " + duration + "from mapRate=" + mapRate + " and selectivity=" + avgSelectivity);
        return duration.longValue();
    }

    private Long predictReduceTaskDuration(JobProfile job) {
        if (job.getAvgReduceDuration() != 0)
            return job.getAvgReduceDuration();

        // calculate average duration based on map selectivity and historical processing rates

        double avgSelectivity = calculateMapTaskSelectivity(job);

        List<JobProfile> comparable = getComparableProfiles(job, TaskType.REDUCE);
        if (comparable.size() < 1) {
            // non-existent or irrelevant historical data
            return handleNoReduceHistory(job, avgSelectivity);
        }
        boolean relevantHistory = comparable.get(0).getReducerClass().equals(job.getReducerClass());

        // we have reduce history; calculate reduce rate
        double avgReduceRate = 0, avgReduceDuration = 0;
        int comparableNo = 0;
        for (JobProfile profile : comparable) {
            if (profile.getTotalReduceTasks() < 1)
                continue;
            comparableNo++;
            if (avgSelectivity == 0 || !relevantHistory)
                // our selectivity or reduce rate data is unreliable
                // just compute average reduce duration of historical jobs
                avgReduceDuration += profile.getAvgReduceDuration();
            else {
                // calculate reduce rate; restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
                Double inputPerTask = 1.0 * Math.max(profile.getReduceInputBytes() / profile.getTotalReduceTasks(), 1);
                avgReduceRate += inputPerTask / profile.getAvgReduceDuration();
            }
        }
        if (comparableNo < 1)
            // non-existent or irrelevant historical data
            return handleNoReduceHistory(job, avgSelectivity);
        if (avgSelectivity == 0 || !relevantHistory)
            // our selectivity or reduce rate data is unreliable
            // just compute average reduce duration of historical jobs
            return Double.valueOf(avgReduceDuration / comparableNo).longValue();
        avgReduceRate /= comparableNo;
        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        double inputPerTask = Math.max(job.getTotalInputBytes() * avgSelectivity / job.getTotalReduceTasks(), 1);
        Double duration = inputPerTask / avgReduceRate;
        logger.debug("Reduce duration for " + job.getId() + " should be " + inputPerTask + " / " + avgReduceRate + "=" + duration);
        return duration.longValue();
    }

    @Override
    public Long predictTaskDuration(String jobId, TaskType type) {
        JobProfile job = getDataStore().findById(DataEntityType.JOB, jobId);
        if (type.equals(TaskType.MAP)) {
            return predictMapTaskDuration(job);
        }
        return predictReduceTaskDuration(job);
    }
}
