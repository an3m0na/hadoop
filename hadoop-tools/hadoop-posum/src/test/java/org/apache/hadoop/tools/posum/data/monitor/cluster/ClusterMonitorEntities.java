package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CounterGroupInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.CounterInfoPayload;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ClusterMonitorEntities {

  final AppProfile FINISHED_APP, RUNNING_APP;
  final String APP_ID, JOB_ID;
  final AppProfile[] FINISHED_APPS, RUNNING_APPS;
  final JobProfile FINISHED_JOB, RUNNING_JOB;
  final JobProfile[] RUNNING_JOBS;
  final TaskProfile[] FINISHED_TASKS, RUNNING_TASKS;
  final TaskProfile RUNNING_REDUCE_TASK, RUNNING_MAP_TASK;
  final TaskProfile DETAILED_REDUCE_TASK;
  final TaskProfile FINISHED_DETAILED_REDUCE_TASK;
  final CountersProxy JOB_COUNTERS, TASK_COUNTERS;
  final JobConfProxy JOB_CONF;

  ClusterMonitorEntities() {
    APP_ID = "application_1326821518301_0005";

    RUNNING_APP = Records.newRecord(AppProfile.class);
    RUNNING_APP.setId(APP_ID);
    RUNNING_APP.setQueue("a1");
    RUNNING_APP.setUser("user1");
    RUNNING_APP.setState(YarnApplicationState.RUNNING);
    RUNNING_APP.setName("Sleep job");
    RUNNING_APP.setStartTime(1326824544552L);
    RUNNING_APP.setTrackingUI(RestClient.TrackingUI.AM);
    RUNNING_APP.setStatus(FinalApplicationStatus.UNDEFINED);

    RUNNING_APPS = new AppProfile[]{RUNNING_APP};

    FINISHED_APP = RUNNING_APP.copy();
    FINISHED_APP.setFinishTime(1326824991300L);
    FINISHED_APP.setState(YarnApplicationState.FINISHED);
    FINISHED_APP.setTrackingUI(RestClient.TrackingUI.HISTORY);
    FINISHED_APP.setStatus(FinalApplicationStatus.SUCCEEDED);

    FINISHED_APPS = new AppProfile[]{FINISHED_APP};

    JOB_ID = "job_1326821518301_0005";

    RUNNING_JOB = Records.newRecord(JobProfile.class);
    RUNNING_JOB.setId(JOB_ID);
    RUNNING_JOB.setAppId(APP_ID);
    RUNNING_JOB.setFinishTime(0L);
    RUNNING_JOB.setQueue("a1");
    RUNNING_JOB.setUser("user1");
    RUNNING_JOB.setName("Sleep job");
    RUNNING_JOB.setStartTime(1326381446529L);
    RUNNING_JOB.setUberized(false);
    RUNNING_JOB.setTotalMapTasks(1);
    RUNNING_JOB.setTotalReduceTasks(1);
    RUNNING_JOB.setState(JobState.RUNNING);
    RUNNING_JOB.setCompletedMaps(0);
    RUNNING_JOB.setCompletedReduces(0);
    RUNNING_JOB.setMapProgress(58f);
    RUNNING_JOB.setReduceProgress(0f);

    RUNNING_JOBS = new JobProfile[]{RUNNING_JOB};

    FINISHED_JOB = RUNNING_JOB.copy();
    FINISHED_JOB.setSubmitTime(1326381446500L);
    FINISHED_JOB.setFinishTime(1326381582106L);
    FINISHED_JOB.setState(JobState.SUCCEEDED);
    FINISHED_JOB.setCompletedMaps(1);
    FINISHED_JOB.setCompletedReduces(1);
    FINISHED_JOB.setAvgMapDuration(2638L);
    FINISHED_JOB.setAvgReduceDuration(130090L);
    FINISHED_JOB.setAvgShuffleTime(2540L);
    FINISHED_JOB.setAvgMergeTime(2589L);
    FINISHED_JOB.setAvgReduceTime(124961L);
    FINISHED_JOB.setMapProgress(100f);
    FINISHED_JOB.setReduceProgress(100f);

    RUNNING_MAP_TASK = Records.newRecord(TaskProfile.class);
    RUNNING_MAP_TASK.setId("task_1326821518301_0005_m_0");
    RUNNING_MAP_TASK.setAppId(APP_ID);
    RUNNING_MAP_TASK.setJobId(JOB_ID);
    RUNNING_MAP_TASK.setType(TaskType.MAP);
    RUNNING_MAP_TASK.setStartTime(1326381446541L);
    RUNNING_MAP_TASK.setFinishTime(0L);
    RUNNING_MAP_TASK.setState(TaskState.RUNNING);
    RUNNING_MAP_TASK.setReportedProgress(58f);

    RUNNING_REDUCE_TASK = Records.newRecord(TaskProfile.class);
    RUNNING_REDUCE_TASK.setId("task_1326821518301_0005_r_0");
    RUNNING_REDUCE_TASK.setAppId(APP_ID);
    RUNNING_REDUCE_TASK.setJobId(JOB_ID);
    RUNNING_REDUCE_TASK.setType(TaskType.REDUCE);
    RUNNING_REDUCE_TASK.setStartTime(1326381446544L);
    RUNNING_REDUCE_TASK.setFinishTime(0L);
    RUNNING_REDUCE_TASK.setState(TaskState.RUNNING);
    RUNNING_REDUCE_TASK.setReportedProgress(0f);

    RUNNING_TASKS = new TaskProfile[]{RUNNING_MAP_TASK, RUNNING_REDUCE_TASK};

    TaskProfile FINISHED_MAP_TASK = RUNNING_MAP_TASK.copy();
    FINISHED_MAP_TASK.setAppId(null);
    FINISHED_MAP_TASK.setFinishTime(1326381453318L);
    FINISHED_MAP_TASK.setSuccessfulAttempt("attempt_1326821518301_0005_m_0_0");
    FINISHED_MAP_TASK.setState(TaskState.SUCCEEDED);
    FINISHED_MAP_TASK.setReportedProgress(100f);

    TaskProfile FINISHED_REDUCE_TASK = RUNNING_REDUCE_TASK.copy();
    FINISHED_REDUCE_TASK.setAppId(null);
    FINISHED_REDUCE_TASK.setFinishTime(1326381582103L);
    FINISHED_REDUCE_TASK.setSuccessfulAttempt("attempt_1326821518301_0005_r_0_0");
    FINISHED_REDUCE_TASK.setState(TaskState.SUCCEEDED);
    FINISHED_REDUCE_TASK.setReportedProgress(100f);

    FINISHED_TASKS = new TaskProfile[]{FINISHED_MAP_TASK, FINISHED_REDUCE_TASK};

    DETAILED_REDUCE_TASK = RUNNING_REDUCE_TASK.copy();
    DETAILED_REDUCE_TASK.setShuffleTime(2592L);
    DETAILED_REDUCE_TASK.setReduceTime(0L);
    DETAILED_REDUCE_TASK.setMergeTime(47L);
    DETAILED_REDUCE_TASK.setHttpAddress("host.domain.com");

    FINISHED_DETAILED_REDUCE_TASK = DETAILED_REDUCE_TASK.copy();
    FINISHED_DETAILED_REDUCE_TASK.setReduceTime(311L);


    JOB_COUNTERS = Records.newRecord(CountersProxy.class);
    JOB_COUNTERS.setId(JOB_ID);
    JOB_COUNTERS.setCounterGroup(createCounterGroups(true));

    TASK_COUNTERS = Records.newRecord(CountersProxy.class);
    TASK_COUNTERS.setId(RUNNING_MAP_TASK.getId());
    TASK_COUNTERS.setTaskCounterGroup(createCounterGroups(false));

    JOB_CONF = Records.newRecord(JobConfProxy.class);
    JOB_CONF.setId(JOB_ID);
    JOB_CONF.setConfPath("hdfs://host.domain.com:9000/user/user1/.staging/job_1326381300833_0002/profile.xml");
    Map<String, String> properties = new HashMap<>();
    properties.put("dfs.datanode.data.dir", "/home/hadoop/hdfs/data");
    properties.put("hadoop.http.filter.initializers", "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
    properties.put("mapreduce.cluster.temp.dir", "/home/hadoop/tmp");
    properties.put("mapreduce.job.name", "Sleep job");
    properties.put("mapreduce.job.user.name", "user1");
    properties.put("mapreduce.job.queuename", "a1");
    properties.put("mapreduce.job.reduces", "1");
    JOB_CONF.setPropertyMap(properties);
  }

  private static List<CounterGroupInfoPayload> createCounterGroups(boolean includeMapAndReduceCounters) {
    List<CounterGroupInfoPayload> counterGroups = Arrays.asList(
      CounterGroupInfoPayload.newInstance("Shuffle Errors", Arrays.asList(
        CounterInfoPayload.newInstance("BAD_ID", 0),
        CounterInfoPayload.newInstance("CONNECTION", 0),
        CounterInfoPayload.newInstance("IO_ERROR", 0),
        CounterInfoPayload.newInstance("WRONG_LENGTH", 0),
        CounterInfoPayload.newInstance("WRONG_MAP", 0),
        CounterInfoPayload.newInstance("WRONG_REDUCE", 0)
      )),
      CounterGroupInfoPayload.newInstance("org.apache.hadoop.mapreduce.FileSystemCounter", Arrays.asList(
        CounterInfoPayload.newInstance("FILE_BYTES_READ", 2483),
        CounterInfoPayload.newInstance("FILE_BYTES_WRITTEN", 108525),
        CounterInfoPayload.newInstance("FILE_READ_OPS", 0),
        CounterInfoPayload.newInstance("FILE_LARGE_READ_OPS", 0),
        CounterInfoPayload.newInstance("FILE_WRITE_OPS", 0),
        CounterInfoPayload.newInstance("HDFS_BYTES_READ", 48),
        CounterInfoPayload.newInstance("HDFS_BYTES_WRITTEN", 0),
        CounterInfoPayload.newInstance("HDFS_READ_OPS", 1),
        CounterInfoPayload.newInstance("HDFS_LARGE_READ_OPS", 0),
        CounterInfoPayload.newInstance("HDFS_WRITE_OPS", 0)
      )),
      CounterGroupInfoPayload.newInstance("org.apache.hadoop.mapreduce.TaskCounter", Arrays.asList(
        CounterInfoPayload.newInstance("MAP_INPUT_RECORDS", 1),
        CounterInfoPayload.newInstance("MAP_OUTPUT_RECORDS", 1200),
        CounterInfoPayload.newInstance("MAP_OUTPUT_BYTES", 4800),
        CounterInfoPayload.newInstance("MAP_OUTPUT_MATERIALIZED_BYTES", 2235),
        CounterInfoPayload.newInstance("SPLIT_RAW_BYTES", 48),
        CounterInfoPayload.newInstance("COMBINE_INPUT_RECORDS", 0),
        CounterInfoPayload.newInstance("COMBINE_OUTPUT_RECORDS", 0),
        CounterInfoPayload.newInstance("REDUCE_INPUT_GROUPS", 1200),
        CounterInfoPayload.newInstance("REDUCE_SHUFFLE_BYTES", 2235),
        CounterInfoPayload.newInstance("REDUCE_INPUT_RECORDS", 1200),
        CounterInfoPayload.newInstance("REDUCE_OUTPUT_RECORDS", 0),
        CounterInfoPayload.newInstance("SPILLED_RECORDS", 2400),
        CounterInfoPayload.newInstance("SHUFFLED_MAPS", 1),
        CounterInfoPayload.newInstance("FAILED_SHUFFLE", 0),
        CounterInfoPayload.newInstance("MERGED_MAP_OUTPUTS", 1),
        CounterInfoPayload.newInstance("GC_TIME_MILLIS", 113),
        CounterInfoPayload.newInstance("CPU_MILLISECONDS", 1830),
        CounterInfoPayload.newInstance("PHYSICAL_MEMORY_BYTES", 478068736),
        CounterInfoPayload.newInstance("VIRTUAL_MEMORY_BYTES", 2159284224L),
        CounterInfoPayload.newInstance("COMMITTED_HEAP_BYTES", 378863616)
      )),
      CounterGroupInfoPayload.newInstance("org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter", Collections.singletonList(
        CounterInfoPayload.newInstance("BYTES_READ", 0)
      )),
      CounterGroupInfoPayload.newInstance("org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter", Collections.singletonList(
        CounterInfoPayload.newInstance("BYTES_WRITTEN", 0)
      ))
    );
    if (includeMapAndReduceCounters) {
      for (CounterGroupInfoPayload counterGroup : counterGroups) {
        for (CounterInfoPayload counterInfoPayload : counterGroup.getCounter()) {
          counterInfoPayload.setMapCounterValue(0);
          counterInfoPayload.setReduceCounterValue(0);
        }
      }
    }
    return counterGroups;
  }
}
