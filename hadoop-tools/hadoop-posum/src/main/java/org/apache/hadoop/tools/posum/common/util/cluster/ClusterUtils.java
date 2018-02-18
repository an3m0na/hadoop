package org.apache.hadoop.tools.posum.common.util.cluster;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CounterGroupInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.CounterInfoPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;
import static org.apache.hadoop.yarn.server.utils.BuilderUtils.newResourceRequest;

public class ClusterUtils {
  public final static Priority DEFAULT_PRIORITY = Priority.newInstance(1);

  public static ApplicationId parseApplicationId(String id) {
    try {
      String[] parts = id.split("_");
      return ApplicationId.newInstance(Long.parseLong(parts[1]),
        Integer.parseInt(parts[2]));
    } catch (Exception e) {
      throw new PosumException("Id parse exception for " + id, e);
    }
  }

  private static JobId composeJobId(Long timestamp, Integer actualId) {
    return MRBuilderUtils.newJobId(ApplicationId.newInstance(timestamp, actualId), actualId);
  }

  public static JobId parseJobId(String id) {
    try {
      String[] parts = id.split("_");
      return composeJobId(Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
    } catch (Exception e) {
      throw new PosumException("Id parse exception for " + id, e);
    }
  }

  public static TaskId parseTaskId(String id) {
    try {
      String[] parts = id.split("_");
      return MRBuilderUtils.newTaskId(
        composeJobId(Long.parseLong(parts[1]), Integer.parseInt(parts[2])),
        Integer.parseInt(parts[4]),
        "m".equals(parts[3]) ? TaskType.MAP : TaskType.REDUCE
      );
    } catch (Exception e) {
      throw new PosumException("Id parse exception for " + id, e);
    }
  }

  public static void updateJobStatisticsFromCounters(JobProfile job, CountersProxy counters) {
    if (counters == null)
      return;
    for (CounterGroupInfoPayload group : counters.getCounterGroup()) {
      for (CounterInfoPayload counter : group.getCounter()) {
        switch (counter.getName()) {
          // make sure to record map materialized (compressed) bytes if compression is enabled
          // this is because reduce_shuffle_bytes are also in compressed form
          case "MAP_OUTPUT_BYTES":
            Long previous = orZero(job.getMapOutputBytes());
            if (previous == 0)
              job.setMapOutputBytes(counter.getTotalCounterValue());
            break;
          case "MAP_OUTPUT_MATERIALIZED_BYTES":
            Long value = counter.getTotalCounterValue();
            if (value > 0)
              job.setMapOutputBytes(value);
            break;
          case "REDUCE_SHUFFLE_BYTES":
            job.setReduceInputBytes(counter.getTotalCounterValue());
            break;
          case "BYTES_READ":
            job.setInputBytes(counter.getTotalCounterValue());
            break;
          case "BYTES_WRITTEN":
            job.setOutputBytes(counter.getTotalCounterValue());
            break;
        }
      }
    }
  }

  public static Long getDuration(JobProfile job) {
    if (orZero(job.getStartTime()) == 0 || orZero(job.getFinishTime()) == 0)
      return 0L;
    return job.getFinishTime() - job.getStartTime();
  }

  public static Long getDuration(TaskProfile task) {
    if (orZero(task.getStartTime()) == 0 || orZero(task.getFinishTime()) == 0)
      return 0L;
    return task.getFinishTime() - task.getStartTime();
  }

  public static Long getSplitSize(TaskProfile task, JobProfile job) {
    if (task != null && task.getSplitSize() != null)
      return task.getSplitSize();
    return getAvgSplitSize(job);
  }

  public static Long getAvgSplitSize(JobProfile job) {
    if (job.getTotalSplitSize() == null || job.getTotalMapTasks() < 1)
      return null;
    // consider equal sizes; restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    return Math.max(job.getTotalSplitSize() / job.getTotalMapTasks(), 1);
  }

  public static void updateJobStatisticsFromTasks(JobProfile job, List<TaskProfile> tasks) {
    if (tasks == null)
      return;
    long mapDuration = 0, reduceDuration = 0, reduceTime = 0, shuffleTime = 0, mergeTime = 0;
    int mapNo = 0, reduceNo = 0, avgNo = 0;
    long mapInputSize = 0, mapOutputSize = 0, reduceInputSize = 0, reduceOutputSize = 0;

    for (TaskProfile task : tasks) {
      if (getDuration(task) <= 0)
        // skip unfinished tasks
        continue;
      if (TaskType.MAP.equals(task.getType())) {
        mapDuration += getDuration(task);
        mapNo++;
        mapInputSize += orZero(task.getInputBytes());
        mapOutputSize += orZero(task.getOutputBytes());
        if (task.getSplitLocations() != null && task.getHostName() != null) {
          if (task.getSplitLocations().contains(task.getHostName()))
            task.setLocal(true);
        }
      }
      if (TaskType.REDUCE.equals(task.getType())) {
        reduceDuration += getDuration(task);
        reduceTime += orZero(task.getReduceTime());
        shuffleTime += orZero(task.getShuffleTime());
        mergeTime += orZero(task.getMergeTime());
        reduceNo++;
        reduceInputSize += orZero(task.getInputBytes());
        reduceOutputSize += orZero(task.getOutputBytes());
      }
      avgNo++;
    }

    if (avgNo > 0) {
      if (mapNo > 0) {
        job.setAvgMapDuration(mapDuration / mapNo);
      }
      if (reduceNo > 0) {
        job.setAvgReduceDuration(reduceDuration / reduceNo);
        job.setAvgShuffleTime(shuffleTime / reduceNo);
        job.setAvgMergeTime(mergeTime / reduceNo);
        job.setAvgReduceTime(reduceTime / reduceNo);
      }
    }

    job.setInputBytes(mapInputSize);
    job.setMapOutputBytes(mapOutputSize);
    job.setReduceInputBytes(reduceInputSize);
    job.setOutputBytes(reduceOutputSize);
    job.setCompletedMaps(mapNo);
    job.setCompletedReduces(reduceNo);
  }

  public static void updateTaskStatisticsFromCounters(TaskProfile task, CountersProxy counters) {
    if (counters == null)
      return;
    for (CounterGroupInfoPayload group : counters.getCounterGroup()) {
      for (CounterInfoPayload counter : group.getCounter()) {
        switch (counter.getName()) {
          // make sure to record map materialized (compressed) bytes if compression is enabled
          // this is because reduce_shuffle_bytes are also in compressed form
          case "MAP_OUTPUT_BYTES":
            if (task.getType().equals(TaskType.MAP)) {
              Long previous = orZero(task.getOutputBytes());
              if (previous == 0)
                task.setOutputBytes(counter.getTotalCounterValue());
            }
            break;
          case "MAP_OUTPUT_MATERIALIZED_BYTES":
            if (task.getType().equals(TaskType.MAP)) {
              Long value = counter.getTotalCounterValue();
              if (value > 0)
                task.setOutputBytes(value);
            }
            break;
          case "REDUCE_SHUFFLE_BYTES":
            if (task.getType().equals(TaskType.REDUCE))
              task.setInputBytes(counter.getTotalCounterValue());
            break;
          case "BYTES_READ":
            if (task.getType().equals(TaskType.MAP)) {
              task.setInputBytes(counter.getTotalCounterValue());
            }
            break;
          case "BYTES_WRITTEN":
            if (task.getType().equals(TaskType.REDUCE))
              task.setOutputBytes(counter.getTotalCounterValue());
            break;
        }
      }
    }
  }

  public static Double getDouble(Map<String, String> map, String key, Double defaultValue) {
    String valueString = map.get(key);
    return valueString == null ? defaultValue : Double.valueOf(valueString);
  }

  public static Integer getInteger(Map<String, String> map, String key, Integer defaultValue) {
    String valueString = map.get(key);
    return valueString == null ? defaultValue : Integer.valueOf(valueString);
  }

  public static void copyRunningAppInfo(DataStore dataStore, DatabaseReference source, DatabaseReference target) {
    Database simDb = Database.from(dataStore, target);
    simDb.clear();
    dataStore.copyCollections(source, target, Arrays.asList(APP, JOB, JOB_CONF, TASK, COUNTER));
  }

  public static ResourceRequest createResourceRequest(Resource resource,
                                                      String host,
                                                      int numContainers) {
    return newResourceRequest(DEFAULT_PRIORITY, host, resource, numContainers);
  }

  public static ResourceRequest createResourceRequest(Priority prioriy,
                                                      Resource resource,
                                                      String host,
                                                      int numContainers) {
    return newResourceRequest(prioriy, host, resource, numContainers);
  }
}
