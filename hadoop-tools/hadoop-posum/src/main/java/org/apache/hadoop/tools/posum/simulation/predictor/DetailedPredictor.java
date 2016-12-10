package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.SaveJobFlexFieldsCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DetailedPredictor extends JobBehaviorPredictor {

    private static final Log logger = LogFactory.getLog(DetailedPredictor.class);

    private static final String FLEX_KEY_PREFIX = DetailedPredictor.class.getSimpleName() + "::";

    private enum FlexKeys {
        PROFILED, MAP_REMOTE, MAP_LOCAL, MAP_SELECTIVITY, SHUFFLE_FIRST, SHUFFLE_TYPICAL, MERGE, REDUCE
    }

    @Override
    public void initialize(Database db) {
        super.initialize(db);

        // populate flex-fields for jobs in history
        IdsByQueryCall getFinishedJobIds = IdsByQueryCall.newInstance(DataEntityCollection.JOB_HISTORY, null);
        List<String> historyJobIds = db.executeDatabaseCall(getFinishedJobIds).getEntries();
        for (String jobId : historyJobIds) {
            FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB_HISTORY, jobId);
            JobProfile job = getDatabase().executeDatabaseCall(getJob).getEntity();
            if (job.getFlexField(FLEX_KEY_PREFIX + FlexKeys.PROFILED) == null)
                completeProfile(job);
        }
    }

    private void completeProfile(JobProfile job) {
        FindByQueryCall getTasks = FindByQueryCall.newInstance(DataEntityCollection.TASK_HISTORY,
                QueryUtils.is("jobId", job.getId()));
        List<TaskProfile> tasks = getDatabase().executeDatabaseCall(getTasks).getEntities();
        Map<String, String> fields = calculateCurrentProfile(job, tasks);
        fields.put(FLEX_KEY_PREFIX + FlexKeys.PROFILED, "true");
        SaveJobFlexFieldsCall saveFlexFields = SaveJobFlexFieldsCall.newInstance(job.getId(), fields, true);
        getDatabase().executeDatabaseCall(saveFlexFields);
    }

    private JobProfile calculateCurrentProfile(String jobId) {
        FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB, jobId);
        JobProfile job = getDatabase().executeDatabaseCall(getJob).getEntity();
        FindByQueryCall getTasks = FindByQueryCall.newInstance(DataEntityCollection.TASK,
                QueryUtils.is("jobId", job.getId()));
        Map<String, String> flexFields = calculateCurrentProfile(job,
                getDatabase().executeDatabaseCall(getTasks).<TaskProfile>getEntities());
        SaveJobFlexFieldsCall saveFlexFields = SaveJobFlexFieldsCall.newInstance(job.getId(), flexFields, false);
        getDatabase().executeDatabaseCall(saveFlexFields);
        return job;
    }

    private Map<String, String> calculateCurrentProfile(JobProfile job, List<TaskProfile> tasks) {

        //WARNING! the job and tasks stats might not be consistent because they were queried separately

        Map<String, String> fieldMap = new HashMap<>(FlexKeys.values().length);
        Long mapFinish = 0L;
        Double mapRemoteRate = 0.0, mapLocalRate = 0.0, shuffleTypicalRate = 0.0, mergeRate = 0.0, reduceRate = 0.0;
        Integer mapRemoteNo = 0, mapLocalNo = 0, typicalShuffleNo = 0, firstShuffleNo = 0, reduceNo = 0;
        Long shuffleFirstTime = 0L;

        if (job.getCompletedMaps() > 0) {
            Long inputPerMap = Math.max(job.getTotalInputBytes() / job.getTotalMapTasks(), 1);
            Long parsedInputBytes = job.getCompletedMaps() * inputPerMap;
            fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.MAP_SELECTIVITY,
                    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
                    Double.toString(1.0 * job.getMapOutputBytes() / parsedInputBytes));

            for (TaskProfile task : tasks) {
                if (task.getDuration() <= 0)
                    continue;
                if (task.getType().equals(TaskType.MAP)) {
                    // this is a finished map task; split stats into remote and local
                    if (mapFinish < task.getFinishTime())
                        mapFinish = task.getFinishTime();
                    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
                    Double newRate = 1.0 * inputPerMap / task.getDuration();
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
            if (mapLocalNo + mapRemoteNo != job.getTotalMapTasks()) {
                // map phase has not finished yet
                mapFinish = Long.MAX_VALUE;
            }
        }

        if (job.getCompletedReduces() > 0 && job.getTotalReduceTasks() > 0) {
            for (TaskProfile task : tasks) {
                if (task.getDuration() <= 0)
                    continue;
                if (task.getType().equals(TaskType.REDUCE)) {
                    reduceNo++;
                    // this is a finished reduce task; split stats into shuffle, merge and reduce
                    Long taskInputBytes = Math.max(task.getInputBytes(), 1);
                    if (task.getReduceTime() > 0)
                        reduceRate += 1.0 * taskInputBytes / task.getReduceTime();
                    if (task.getMergeTime() > 0)
                        mergeRate += 1.0 * taskInputBytes / task.getMergeTime();
                    if (task.getStartTime() > mapFinish) {
                        // the task was not in the first reduce wave; store shuffle time under typical
                        shuffleTypicalRate += 1.0 * taskInputBytes / task.getShuffleTime();
                        typicalShuffleNo++;
                    } else {
                        logger.debug("When this happens, mapFinish is " + mapFinish);
                        shuffleFirstTime += task.getShuffleTime() - (mapFinish - task.getStartTime());
                        firstShuffleNo++;
                    }
                }
            }
            if (reduceNo > 0) {
                fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.REDUCE, Double.toString(reduceRate / reduceNo));
                fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.MERGE, Double.toString(mergeRate / reduceNo));
                if (shuffleFirstTime != 0) {
                    fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_FIRST,
                            Long.toString(shuffleFirstTime / firstShuffleNo));
                }
                if (shuffleTypicalRate != 0) {
                    fieldMap.put(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_TYPICAL,
                            Double.toString(shuffleTypicalRate / typicalShuffleNo));
                }
            }
        }
        job.getFlexFields().putAll(fieldMap);
        return fieldMap;
    }

    private List<JobProfile> getComparableProfiles(JobProfile job, TaskType type) {
        int bufferLimit = conf.getInt(PosumConfiguration.PREDICTION_BUFFER,
                PosumConfiguration.PREDICTION_BUFFER_DEFAULT);
        // get past jobs with the same name
        FindByQueryCall getComparableJobs = FindByQueryCall.newInstance(
                DataEntityCollection.JOB_HISTORY,
                QueryUtils.is(type.equals(TaskType.MAP) ? "mapperClass" : "reducerClass",
                        type.equals(TaskType.MAP) ? job.getMapperClass() : job.getReducerClass()),
                -bufferLimit,
                bufferLimit
        );
        List<JobProfile> comparable = getDatabase().executeDatabaseCall(getComparableJobs).getEntities();
        if (comparable.size() < 1) {
            // get past jobs at least by the same user
            getComparableJobs.setQuery(QueryUtils.is("user", job.getUser()));
            comparable = getDatabase().executeDatabaseCall(getComparableJobs).getEntities();
        }
        return comparable;
    }

    @Override
    public Long predictJobDuration(String jobId) {
        JobProfile job = calculateCurrentProfile(jobId);
        return predictMapTaskDuration(job, null) * job.getTotalMapTasks() +
                predictReduceTaskDuration(job) * job.getTotalReduceTasks();
    }

    @Override
    public Long predictTaskDuration(String jobId, TaskType type) {
        JobProfile job = calculateCurrentProfile(jobId);
        if (type.equals(TaskType.MAP)) {
            // assuming the task is remote
            return predictMapTaskDuration(job, null);
        } else {
            return predictReduceTaskDuration(job);
        }
    }

    @Override
    public Long predictTaskDuration(String taskId) {
        FindByIdCall getTask = FindByIdCall.newInstance(DataEntityCollection.TASK, taskId);
        TaskProfile task = getDatabase().executeDatabaseCall(getTask).getEntity();
        if (task == null)
            throw new PosumException("Task not found for id " + taskId);
        if (task.getDuration() > 0)
            throw new PosumException("Task has already finished: " + taskId);
        if (task.getType().equals(TaskType.MAP) && task.getHttpAddress() != null) {
            // apply locality-aware logic for this map task
            JobProfile job = calculateCurrentProfile(task.getJobId());
            Boolean locality = task.isLocal();
            return predictMapTaskDuration(job, locality);
        }
        // apply general task duration calculation
        return predictTaskDuration(task.getJobId(), task.getType());
    }

    private Long handleNoMapHistory(JobProfile job) {
        if (job.getAvgMapDuration() != 0)
            // if we do have at least the current average duration, return that, regardless of location
            return job.getAvgMapDuration();
        logger.debug("No map history data for " + job.getId() + ". Using default");
        // return the default; there is nothing we can do
        return conf.getLong(PosumConfiguration.AVERAGE_TASK_DURATION,
                PosumConfiguration.AVERAGE_TASK_DURATION_DEFAULT);
    }

    private Long predictMapTaskDuration(JobProfile job, Boolean remote) {
        Double rate = 0.0;
        String rateKey = FLEX_KEY_PREFIX + (remote == null || remote ? FlexKeys.MAP_REMOTE : FlexKeys.MAP_LOCAL);

        if (remote == null) {
            // we don't know the task locality
            if (job.getAvgMapDuration() != 0)
                // we have the current average duration, so return it
                return job.getAvgMapDuration();
        } else {
            // we know the locality, so consider the rate of that type
            Map<String, String> flexFields = job.getFlexFields();
            String rateString = flexFields.get(rateKey);
            if (rateString != null)
                rate = Double.valueOf(rateString);
        }

        if (rate == 0) {
            // we don't know the rate of that type
            // compute the appropriate average map processing rate from history
            List<JobProfile> comparable = getComparableProfiles(job, TaskType.MAP);
            if (comparable.size() < 1)
                return handleNoMapHistory(job);
            if (!comparable.get(0).getMapperClass().equals(job.getMapperClass()) && job.getAvgMapDuration() != 0)
                // if history is not relevant and we have the current average duration, return it
                return job.getAvgMapDuration();
            Integer numRates = 0;
            for (JobProfile profile : comparable) {
                logger.debug("Comparing map of " + job.getId() + " with " + profile.getId());
                if (profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.PROFILED) == null) {
                    completeProfile(profile);
                }
                if (remote != null) {
                    // if we know task locality, we average the map rates of tasks with the same locality
                    String rateString = profile.getFlexField(rateKey);
                    if (rateString != null) {
                        rate += Double.valueOf(rateString);
                        numRates++;
                    }
                } else {
                    // locality is unknown; average general map rates
                    Long inputPerMap = Math.max(profile.getTotalInputBytes() / profile.getTotalMapTasks(), 1);
                    rate += 1.0 * inputPerMap / profile.getAvgMapDuration();
                    numRates++;
                }
            }
            if (rate == 0)
                return handleNoMapHistory(job);
            rate /= numRates;
        }
        // multiply by how much input each task has
        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        Long splitSize = Math.max(job.getTotalInputBytes() / job.getTotalMapTasks(), 1);
        Double duration = splitSize / rate;
        logger.debug("Map duration for " + job.getId() + " should be " + splitSize + " / " + rate + "=" + duration);
        return duration.longValue();
    }

    private Double calculateMapTaskSelectivity(JobProfile job) {
        // we try to compute selectivity from the map history
        List<JobProfile> comparable = getComparableProfiles(job, TaskType.MAP);
        if (comparable.size() < 1 || !comparable.get(0).getMapperClass().equals(job.getMapperClass())) {
            // there is no history, or it is not relevant for selectivity
            if (job.getCompletedMaps() > 0) {
                String selectivityString = job.getFlexField(FLEX_KEY_PREFIX + FlexKeys.MAP_SELECTIVITY);
                if (selectivityString != null) {
                    // we know the current selectivity
                    logger.debug("Using own selectivity for " + job.getId() + ": " + selectivityString);
                    return Double.valueOf(selectivityString);
                }
            }
            // we don't know anything about selectivity
            return 0.0;
        }
        Double avgSelectivity = 0.0;
        for (JobProfile profile : comparable) {
            if (profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.PROFILED) == null) {
                completeProfile(profile);
            }
            logger.debug("Comparing " + job.getId() + " with other for selectivity: " + profile.getId());
            avgSelectivity += Double.valueOf(profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.MAP_SELECTIVITY));
        }
        return avgSelectivity / comparable.size();
    }

    private Long handleNoReduceHistory(JobProfile job, Double avgSelectivity) {
        if (avgSelectivity == 0 || job.getCompletedMaps() == 0) {
            // our selectivity or map rate data is unreliable
            // just return default duration
            logger.debug("No data to compute reduce for " + job.getName() + ". Using default");
            return conf.getLong(PosumConfiguration.AVERAGE_TASK_DURATION,
                    PosumConfiguration.AVERAGE_TASK_DURATION_DEFAULT);
        }

        // calculate the current map rate and assume reduce rate is the same

        // we assume the reduce processing rate is the same as the map processing rate
        String durationString = job.getFlexField(FLEX_KEY_PREFIX + FlexKeys.MAP_REMOTE);
        if (durationString == null)
            durationString = job.getFlexField(FLEX_KEY_PREFIX + FlexKeys.MAP_LOCAL);
        Double mapRate = durationString == null ? job.getAvgMapDuration() : Double.valueOf(durationString);
        // calculate how much input the task has
        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        Double inputPerTask = Math.max(job.getTotalInputBytes() * avgSelectivity / job.getTotalReduceTasks(), 1);
        Double duration = inputPerTask / mapRate;
        logger.debug("Reduce duration computed based on map data for " + job.getId() + " as " + duration + "from (remote) mapRate=" + mapRate + " and selectivity=" + avgSelectivity);
        return duration.longValue();
    }

    private Long predictReduceTaskDuration(JobProfile job) {
        Double mergeRate = null, reduceRate = null, typicalShuffleRate = null;
        Long shuffleFirst = null;

        Double avgSelectivity = calculateMapTaskSelectivity(job);

        String flexString;
        Map<String, String> flexFields = job.getFlexFields();
        flexString = flexFields.get(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_FIRST);
        if (flexString != null)
            shuffleFirst = Long.valueOf(flexString);
        flexString = flexFields.get(FLEX_KEY_PREFIX + FlexKeys.MERGE);
        if (flexString != null)
            mergeRate = Double.valueOf(flexString);
        flexString = flexFields.get(FLEX_KEY_PREFIX + FlexKeys.REDUCE);
        if (flexString != null)
            reduceRate = Double.valueOf(flexString);
        flexString = flexFields.get(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_TYPICAL);
        if (flexString != null)
            typicalShuffleRate = Double.valueOf(flexString);

        Double avgReduceDuration = 0.0;

        if (typicalShuffleRate == null || shuffleFirst == null || mergeRate == null || reduceRate == null) {
            // if the typical shuffle rate is not calculated, we are clearly missing information
            // compute averages based on history
            List<JobProfile> comparable = getComparableProfiles(job, TaskType.REDUCE);
            if (comparable.size() < 1)
                return handleNoReduceHistory(job, avgSelectivity);

            boolean relevantHistory = comparable.get(0).getReducerClass().equals(job.getReducerClass());
            // compute the reducer processing rates
            Double avgMergeRate = 0.0, avgReduceRate = 0.0, avgShuffleRate = 0.0;
            Long avgShuffleFirst = 0L;
            Integer comparableNo = 0, firstShuffles = 0, typicalShuffles = 0;
            String rateString;
            for (JobProfile profile : comparable) {
                if (profile.getTotalReduceTasks() < 1)
                    continue;
                logger.debug("Comparing reduce of " + job.getId() + " with " + profile.getId());
                if (profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.PROFILED) == null) {
                    completeProfile(profile);
                }
                comparableNo++;
                avgReduceDuration += profile.getAvgReduceDuration();
                if (avgSelectivity != 0 && relevantHistory) {
                    // we have relevant info; calculate segmented durations and rates
                    rateString = profile.getFlexField(FLEX_KEY_PREFIX + FlexKeys.SHUFFLE_FIRST);
                    if (rateString != null) {
                        avgShuffleFirst += Long.valueOf(rateString);
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
            }
            if (comparableNo < 1)
                return handleNoReduceHistory(job, avgSelectivity);
            avgReduceDuration /= comparableNo;
            if (avgSelectivity == 0 || !relevantHistory) {
                // our selectivity or reduce rate data is unreliable
                // just return average reduce duration of historical jobs
                logger.debug("Reduce duration calculated as simple average for " + job.getId() + " =  " + avgReduceDuration);
                return avgReduceDuration.longValue();
            }
            if (typicalShuffles > 0)
                typicalShuffleRate = avgShuffleRate / typicalShuffles;
            if (shuffleFirst == null && firstShuffles > 0)
                shuffleFirst = avgShuffleFirst / firstShuffles;
            if (mergeRate == null)
                mergeRate = avgMergeRate / comparableNo;
            if (reduceRate == null)
                reduceRate = avgReduceRate / comparableNo;
        }

        // our selectivity and reduce rate data is reliable
        if (mergeRate != null && reduceRate != null) {
            boolean isFirstShuffle = job.getCompletedMaps().equals(job.getTotalMapTasks());
            if ((isFirstShuffle && shuffleFirst != null) || (!isFirstShuffle && typicalShuffleRate != null)) {
                // calculate how much input the task should have based on how much is left and how many reduces remain
                // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
                Double inputLeft = job.getTotalInputBytes() * avgSelectivity - job.getReduceInputBytes();
                Double inputPerTask = Math.max(inputLeft / (job.getTotalReduceTasks() - job.getCompletedReduces()), 1);
                // shuffle time depends on whether it is the first shuffle and the size of the input
                Long shuffleTime = isFirstShuffle ? shuffleFirst :
                        Double.valueOf(inputPerTask / typicalShuffleRate).longValue();
                Double duration = shuffleTime + inputPerTask / mergeRate + inputPerTask / reduceRate;
                logger.debug("Reduce duration for " + job.getId() + " should be " + shuffleTime + " + " +
                        inputPerTask + " / " + mergeRate + " + " +
                        inputPerTask + " / " + reduceRate + "=" + duration);
                return duration.longValue();
            }
        }
        // we are missing information; guessing now won't work
        // just return average reduce duration of historical jobs
        logger.debug("Reduce duration calculated as simple average for " + job.getId() + " =  " + avgReduceDuration);
        return avgReduceDuration.longValue();
    }
}
