package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public class DetailedPredictor extends JobBehaviorPredictor {

    public DetailedPredictor(Configuration conf) {
        super(conf);
    }

    private static final String FLEX_KEY_PREFIX = DetailedPredictor.class.getSimpleName() + "::";

    private enum FlexKeys {
        MAP_REMOTE, MAP_LOCAL, MAP_SELECTIVITY, SHUFFLE_FIRST, SHUFFLE_TYPICAL, MERGE, REDUCE
    }

    @Override
    public void preparePredictor() {
        // populate flex-fields for jobs in history
        List<String> historyJobIds = getDataStore().listIds(DataEntityType.JOB_HISTORY,
                Collections.<String, Object>emptyMap());
        for (String jobId : historyJobIds) {
            JobProfile job = getDataStore().findById(DataEntityType.JOB_HISTORY, jobId);
            List<TaskProfile> tasks = getDataStore().find(DataEntityType.TASK_HISTORY, "jobId", jobId);
            getDataStore().saveFlexFields(jobId, calculateFlexFields(job, tasks), false);
        }
    }

    private Map<String, String> calculateFlexFields(JobProfile job, List<TaskProfile> tasks) {

        //WARNING! the job and tasks stats might not be consistent because they were queried separately

        Map<String, String> fieldMap = new HashMap<>(FlexKeys.values().length);
        long mapFinish = 0L;
        Double mapRemoteRate = 0.0, mapLocalRate = 0.0, shuffleTypicalRate = 0.0, mergeRate = 0.0, reduceRate = 0.0;
        int mapRemoteNo = 0, mapLocalNo = 0, typicalShuffleNo = 0, reduceNo = 0;
        Long shuffleFirstTime = 0L;

        if (job.getCompletedMaps() > 0) {
            for (TaskProfile task : tasks) {
                if (task.getDuration() <= 0)
                    continue;
                if (task.getType().equals(TaskType.MAP)) {
                    // this is a finished map task; split stats into remote and local
                    if (mapFinish < task.getFinishTime())
                        mapFinish = task.getFinishTime();
                    double newRate = task.getInputBytes() / task.getDuration();
                    if (task.isLocal()) {
                        mapLocalRate += newRate;
                        mapLocalNo++;
                    } else {
                        mapRemoteRate += newRate;
                        mapRemoteNo++;
                    }
                }
            }
            if (mapLocalNo != 0 && mapLocalRate != 0) {
                fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.MAP_LOCAL,
                        Double.toString(mapLocalRate / mapLocalNo));
            }
            if (mapRemoteNo != 0 && mapRemoteRate != 0) {
                fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.MAP_REMOTE,
                        Double.toString(mapRemoteRate / mapLocalNo));
            }
            fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.MAP_SELECTIVITY,
                    Double.toString(1.0 * job.getInputBytes() / job.getOutputBytes()));
            if (mapLocalNo + mapRemoteNo != job.getTotalMapTasks()) {
                // map phase has not finished yet
                mapFinish = Long.MAX_VALUE;
            }
        }

        if (job.getCompletedReduces() > 0) {
            for (TaskProfile task : tasks) {
                if (task.getDuration() <= 0)
                    continue;
                reduceNo++;
                if (task.getType().equals(TaskType.REDUCE)) {
                    // this is a finished reduce task; split stats into shuffle, merge and reduce
                    if (task.getReduceTime() > 0)
                        reduceRate += task.getInputBytes() / task.getReduceTime();
                    if (task.getMergeTime() > 0)
                        mergeRate += task.getInputBytes() / task.getMergeTime();
                    if (task.getStartTime() > mapFinish) {
                        // the task was not in the first reduce wave; store shuffle time under typical
                        shuffleTypicalRate += task.getInputBytes() / task.getShuffleTime();
                        typicalShuffleNo++;
                    } else {
                        // record only the time spent shuffling after map phase finished
                        shuffleFirstTime += task.getShuffleTime() - (mapFinish - task.getStartTime());
                    }
                }
            }
            fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.REDUCE, Double.toString(reduceRate / reduceNo));
            fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.MERGE, Double.toString(mergeRate / reduceNo));
            if (shuffleFirstTime != 0) {
                fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_FIRST,
                        Double.toString(shuffleFirstTime / (reduceNo - typicalShuffleNo)));
            }
            if (shuffleTypicalRate != 0) {
                fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_TYPICAL,
                        Double.toString(shuffleTypicalRate / reduceNo));
            }

        }
        return fieldMap;
    }


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
                    DataEntityType.JOB,
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

        Map<String, String> flexFields =
                calculateFlexFields(job, getDataStore().<TaskProfile>find(DataEntityType.TASK, "jobId", job.getId()));

        getDataStore().saveFlexFields(job.getId(), flexFields, false);

        return predictMapTaskDuration(job, flexFields, true) * job.getTotalMapTasks() +
                predictReduceTaskDuration(job, flexFields) * job.getTotalReduceTasks();
    }

    @Override
    public Long predictTaskDuration(String jobId, TaskType type) {
        JobProfile job = getDataStore().findById(DataEntityType.JOB, jobId);

        Map<String, String> flexFields =
                calculateFlexFields(job, getDataStore().<TaskProfile>find(DataEntityType.TASK, "jobId", job.getId()));

        getDataStore().saveFlexFields(job.getId(), flexFields, false);

        if (type.equals(TaskType.MAP)) {
            // assuming the task is remote
            return predictMapTaskDuration(job, flexFields, true);
        } else {
            return predictReduceTaskDuration(job, flexFields);
        }
    }

    @Override
    public Long predictLocalMapTaskDuration(String jobId) {
        JobProfile job = getDataStore().findById(DataEntityType.JOB, jobId);

        Map<String, String> flexFields =
                calculateFlexFields(job, getDataStore().<TaskProfile>find(DataEntityType.TASK, "jobId", job.getId()));

        getDataStore().saveFlexFields(job.getId(), flexFields, false);

        return predictMapTaskDuration(job, flexFields, false);
    }

    private Long predictMapTaskDuration(JobProfile job, Map<String, String> flexFields, boolean remote) {

        String rateKey = FLEX_KEY_PREFIX + (remote ? FlexKeys.MAP_REMOTE : FlexKeys.MAP_LOCAL);
        String rateString = flexFields.get(rateKey);

        Double rate = 0.0;
        if (rateString == null) {
            // compute the average map processing rate from history
            List<JobProfile> comparable = getComparableProfiles(job, TaskType.MAP);
            if (comparable.size() < 1)
                // return the default; there is nothing we can do
                return conf.getLong(POSUMConfiguration.AVERAGE_JOB_DURATION,
                        POSUMConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
            for (JobProfile profile : comparable)
                rate += Double.valueOf(profile.getFlexField(rateKey));
            rate /= comparable.size();
        } else {
            rate = Double.valueOf(rateString);
        }
        // multiply by how much input each task has
        long splitSize = job.getInputBytes() / job.getTotalMapTasks();
        return Double.valueOf(splitSize / rate).longValue();
    }

    private Long predictReduceTaskDuration(JobProfile job, Map<String, String> flexFields) {

        Double mergeRate = null, reduceRate = null, shuffleRate = null;
        Long shuffleTime = null;

        String flexString;
        flexString = flexFields.get(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_FIRST);
        if (flexString != null)
            shuffleTime = Long.valueOf(flexString);
        flexString = flexFields.get(FLEX_KEY_PREFIX + FlexKeys.MERGE);
        if (flexString != null)
            mergeRate = Double.valueOf(flexString);
        flexString = flexFields.get(FLEX_KEY_PREFIX + FlexKeys.REDUCE);
        if (flexString != null)
            reduceRate = Double.valueOf(flexString);
        flexString = flexFields.get(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_TYPICAL);
        if (flexString != null)
            shuffleRate = Double.valueOf(flexString);

        if (shuffleRate == null) {
            // if the typical shuffle rate is not calculated, we are missing information
            // compute averages based on history
            List<JobProfile> comparable = getComparableProfiles(job, TaskType.REDUCE);
            if (comparable.size() < 1) {
                // return the default; there is nothing we can do
                return conf.getLong(POSUMConfiguration.AVERAGE_JOB_DURATION,
                        POSUMConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
            } else {
                // compute the reducer processing rates
                Double avgMergeRate = 0.0, avgReduceRate = 0.0, avgShuffleRate = 0.0;
                Long avgShuffleTime = 0L;
                for (JobProfile profile : comparable) {
                    avgShuffleTime += Long.valueOf(profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_FIRST));
                    avgShuffleRate += Long.valueOf(profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_TYPICAL));
                    avgMergeRate += Double.valueOf(profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.MERGE));
                    avgReduceRate += Double.valueOf(profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.REDUCE));
                }
                shuffleRate = avgShuffleRate / comparable.size();
                if (shuffleTime == null)
                    shuffleTime = avgShuffleTime / comparable.size();
                if (mergeRate == null)
                    mergeRate = avgMergeRate / comparable.size();
                if (reduceRate == null)
                    reduceRate = avgReduceRate / comparable.size();
            }
        }

        // calculate how much input the task has
        long reduceInputSize = Double.valueOf(job.getTotalInputBytes() /
                Double.valueOf(job.getFlexField(FLEX_KEY_PREFIX + FlexKeys.MAP_SELECTIVITY))).longValue();
        long inputPerTask = reduceInputSize / job.getTotalReduceTasks();

        if (job.getCompletedMaps().equals(job.getTotalMapTasks())) {
            // this is not the first wave; shuffle time will be dependent on input
            shuffleTime = Double.valueOf(inputPerTask / shuffleRate).longValue();

        }
        if (shuffleTime == null || mergeRate == null || reduceRate == null)
            throw new POSUMException("Something went wrong when calculating rates for prediction" +
                    " shuffleTime=" + shuffleTime +
                    " mergeRate" + mergeRate +
                    " reduceRate" + reduceRate);
        return shuffleTime + Double.valueOf(inputPerTask / mergeRate).longValue() +
                Double.valueOf(inputPerTask / reduceRate).longValue();
    }

    @Override
    public Long predictTaskDuration(String jobId, String taskId) {
        return predictTaskDuration(jobId, Utils.getTaskTypeFromId(taskId));
    }
}
