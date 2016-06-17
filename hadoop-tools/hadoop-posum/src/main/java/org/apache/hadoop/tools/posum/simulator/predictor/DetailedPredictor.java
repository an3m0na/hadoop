package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.database.client.DBInterface;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public class DetailedPredictor extends JobBehaviorPredictor {

    private static final Log logger = LogFactory.getLog(DetailedPredictor.class);

    private static final String FLEX_KEY_PREFIX = DetailedPredictor.class.getSimpleName() + "::";

    private enum FlexKeys {
        PROFILED, MAP_REMOTE, MAP_LOCAL, MAP_SELECTIVITY, SHUFFLE_FIRST, SHUFFLE_TYPICAL, MERGE, REDUCE
    }

    @Override
    public void initialize(DBInterface dataStore) {
        super.initialize(dataStore);

        // populate flex-fields for jobs in history
        List<String> historyJobIds = dataStore.listIds(DataEntityType.JOB_HISTORY,
                Collections.<String, Object>emptyMap());
        for (String jobId : historyJobIds) {
            JobProfile job = dataStore.findById(DataEntityType.JOB_HISTORY, jobId);
            if (job.getFlexField(FLEX_KEY_PREFIX + FlexKeys.PROFILED) == null)
                completeProfile(job);
        }
    }

    private void completeProfile(JobProfile job) {
        List<TaskProfile> tasks = getDataStore().find(DataEntityType.TASK_HISTORY, "jobId", job.getId());
        Map<String, String> fields = calculateCurrentProfile(job, tasks);
        fields.put(FLEX_KEY_PREFIX + FlexKeys.PROFILED, "true");
        getDataStore().saveFlexFields(job.getId(), fields, true);
    }

    private Map<String, String> calculateCurrentProfile(JobProfile job, List<TaskProfile> tasks) {

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
                    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
                    double newRate = 1.0 * Math.max(task.getInputBytes(), 1) / task.getDuration();
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
                        Double.toString(mapRemoteRate / mapRemoteNo));
            }
            fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.MAP_SELECTIVITY,
                    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
                    Double.toString(1.0 * job.getMapOutputBytes() / Math.max(job.getInputBytes(), job.getTotalMapTasks())));
            if (mapLocalNo + mapRemoteNo != job.getTotalMapTasks()) {
                // map phase has not finished yet
                mapFinish = Long.MAX_VALUE;
            }
            logger.debug("mapFinish for " + job.getId() + " is " + mapFinish);
        }

        if (job.getCompletedReduces() > 0 && job.getTotalReduceTasks() > 0) {
            logger.debug("Some reduces are complete for " + job.getId());
            for (TaskProfile task : tasks) {
                if (task.getDuration() <= 0)
                    continue;
                reduceNo++;
                if (task.getType().equals(TaskType.REDUCE)) {
                    // this is a finished reduce task; split stats into shuffle, merge and reduce
                    double taskInputBytes = Math.max(task.getInputBytes(), 1);
                    if (task.getReduceTime() > 0)
                        reduceRate += taskInputBytes / task.getReduceTime();
                    if (task.getMergeTime() > 0)
                        mergeRate += taskInputBytes / task.getMergeTime();
                    if (task.getStartTime() > mapFinish) {
                        // the task was not in the first reduce wave; store shuffle time under typical
                        shuffleTypicalRate += taskInputBytes / task.getShuffleTime();
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
                        Long.toString(shuffleFirstTime / (reduceNo - typicalShuffleNo)));
            }
            if (shuffleTypicalRate != 0) {
                fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_TYPICAL,
                        Double.toString(shuffleTypicalRate / reduceNo));
            }
        }
        job.getFlexFields().putAll(fieldMap);
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

        Map<String, String> flexFields =
                calculateCurrentProfile(job, getDataStore().<TaskProfile>find(DataEntityType.TASK, "jobId", job.getId()));

        getDataStore().saveFlexFields(job.getId(), flexFields, false);

        return predictMapTaskDuration(job, true) * job.getTotalMapTasks() +
                predictReduceTaskDuration(job) * job.getTotalReduceTasks();
    }

    @Override
    public Long predictTaskDuration(String jobId, TaskType type) {
        JobProfile job = getDataStore().findById(DataEntityType.JOB, jobId);

        Map<String, String> flexFields =
                calculateCurrentProfile(job, getDataStore().<TaskProfile>find(DataEntityType.TASK, "jobId", job.getId()));

        getDataStore().saveFlexFields(job.getId(), flexFields, false);

        if (type.equals(TaskType.MAP)) {
            // assuming the task is remote
            return predictMapTaskDuration(job, true);
        } else {
            return predictReduceTaskDuration(job);
        }
    }

    @Override
    public Long predictLocalMapTaskDuration(String jobId) {
        JobProfile job = getDataStore().findById(DataEntityType.JOB, jobId);

        Map<String, String> flexFields =
                calculateCurrentProfile(job, getDataStore().<TaskProfile>find(DataEntityType.TASK, "jobId", job.getId()));

        getDataStore().saveFlexFields(job.getId(), flexFields, false);

        return predictMapTaskDuration(job, false);
    }

    private Long predictMapTaskDuration(JobProfile job, boolean remote) {

        String rateKey = FLEX_KEY_PREFIX + (remote ? FlexKeys.MAP_REMOTE : FlexKeys.MAP_LOCAL);
        Map<String, String> flexFields = job.getFlexFields();
        String rateString = flexFields.get(rateKey);

        Double rate = 0.0;
        if (rateString == null) {
            // compute the average map processing rate from history
            List<JobProfile> comparable = getComparableProfiles(job, TaskType.MAP);
            if (comparable.size() < 1) {
                logger.debug("No map history data for job " + job.getId() + ". Using default");
                // return the default; there is nothing we can do
                return conf.getLong(POSUMConfiguration.AVERAGE_JOB_DURATION,
                        POSUMConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
            }
            int numRates = 0;
            for (JobProfile profile : comparable) {
                if (profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.PROFILED) == null) {
                    completeProfile(profile);
                }
                rateString = profile.getFlexField(rateKey);
                if (rateString != null) {
                    rate += Double.valueOf(rateString);
                    numRates++;
                }
            }
            rate /= numRates;
        } else {
            rate = Double.valueOf(rateString);
        }
        // multiply by how much input each task has
        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        long splitSize = Math.max(job.getInputBytes() / job.getTotalMapTasks(), 1);
        logger.debug("Map duration for " + job.getId() + " should be " + splitSize + " / " + rate);
        return Double.valueOf(splitSize / rate).longValue();
    }

    private double calculateMapTaskSelectivity(JobProfile job) {
        String selectivityString = job.getFlexField(FLEX_KEY_PREFIX + FlexKeys.MAP_SELECTIVITY);
        if (selectivityString != null)
            // we know the current selectivity
            return Double.valueOf(selectivityString);

        // we have to compute selectivity from the map history
        List<JobProfile> comparable = getComparableProfiles(job, TaskType.MAP);
        if (comparable.size() < 1) {
            return 1.0;
        }
        double avgSelectivity = 0;
        for (JobProfile profile : comparable) {
            if (profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.PROFILED) == null) {
                completeProfile(profile);
            }
            avgSelectivity += Double.valueOf(profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.MAP_SELECTIVITY));
        }
        return avgSelectivity / comparable.size();
    }

    private Long handleNoReduceHistory(JobProfile job, double inputPerTask) {
        if (job.getCompletedMaps() == 0) {
            logger.debug("No data to compute reduce for job " + job.getName() + ". Using default");
            // return the default; there is nothing we can do
            return conf.getLong(POSUMConfiguration.AVERAGE_JOB_DURATION,
                    POSUMConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
        }
        logger.debug("Reduce duration computed based on map data for job" + job.getId());
        //restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        double mapRate = 1.0 * Math.max(job.getInputBytes(), job.getTotalMapTasks()) / job.getAvgMapDuration();
        // we assume the reduce processing rate is the same as the average map processing rate
        return Double.valueOf(inputPerTask / mapRate).longValue();
    }

    private Long predictReduceTaskDuration(JobProfile job) {
        if (job.getTotalReduceTasks() < 1)
            throw new POSUMException("Job does not have reduce tasks for prediction");

        Double mergeRate = null, reduceRate = null, shuffleRate = null;
        Long shuffleTime = null;

        // calculate how much input the task has
        double reduceInputSize = job.getTotalInputBytes() * calculateMapTaskSelectivity(job);
        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        double inputPerTask = Math.max(reduceInputSize / job.getTotalReduceTasks(), 1);

        String flexString;
        Map<String, String> flexFields = job.getFlexFields();
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
                return handleNoReduceHistory(job, inputPerTask);
            } else {
                // compute the reducer processing rates
                Double avgMergeRate = 0.0, avgReduceRate = 0.0, avgShuffleRate = 0.0;
                Long avgShuffleTime = 0L;
                int comparableNo = 0, firstShuffles = 0, typicalShuffles = 0;
                String rateString;
                for (JobProfile profile : comparable) {
                    if (profile.getTotalReduceTasks() < 1)
                        continue;
                    if (profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.PROFILED) == null) {
                        completeProfile(profile);
                    }
                    comparableNo++;
                    rateString = profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_FIRST);
                    if (rateString != null) {
                        avgShuffleTime += Long.valueOf(rateString);
                        firstShuffles++;
                    }
                    rateString = profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_TYPICAL);
                    if (rateString != null) {
                        avgShuffleRate += Double.valueOf(rateString);
                        typicalShuffles++;
                    }
                    avgMergeRate += Double.valueOf(profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.MERGE));
                    avgReduceRate += Double.valueOf(profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.REDUCE));
                }
                if (comparableNo < 1)
                    return handleNoReduceHistory(job, inputPerTask);
                if (typicalShuffles > 0)
                    shuffleRate = avgShuffleRate / typicalShuffles;
                if (shuffleTime == null && firstShuffles > 0)
                    shuffleTime = avgShuffleTime / firstShuffles;
                if (mergeRate == null)
                    mergeRate = avgMergeRate / comparableNo;
                if (reduceRate == null)
                    reduceRate = avgReduceRate / comparableNo;
            }
        }

        if (mergeRate == null || reduceRate == null)
            throw new POSUMException("Something went wrong when calculating rates for prediction" +
                    " shuffleTime=" + shuffleTime +
                    " mergeRate=" + mergeRate +
                    " reduceRate=" + reduceRate);

        // if it is a typical shuffle or there is no first shuffle information
        if (job.getCompletedMaps().equals(job.getTotalMapTasks()) || shuffleTime == null) {
            // shuffle time will be dependent on input
            if (shuffleRate != null)
                shuffleTime = Double.valueOf(inputPerTask / shuffleRate).longValue();
            else
                // there is no first or typical shuffle information; shuffle should take at least as merge
                shuffleTime = Double.valueOf(inputPerTask / mergeRate).longValue();
        }

        logger.debug("Reduce duration for " + job.getId() + " should be " + shuffleTime + " + " +
                inputPerTask + " / " + mergeRate + " + " +
                inputPerTask + " / " + reduceRate);
        return shuffleTime + Double.valueOf(inputPerTask / mergeRate + inputPerTask / reduceRate).longValue();
    }

    @Override
    public Long predictTaskDuration(String taskId) {
        TaskProfile task = getDataStore().findById(DataEntityType.TASK, taskId);
        if (task == null)
            return null;
        if (task.getType().equals(TaskType.MAP)) {
            if (task.isLocal())
                return predictLocalMapTaskDuration(task.getJobId());
        }
        return predictTaskDuration(task.getJobId(), task.getType());
    }
}
