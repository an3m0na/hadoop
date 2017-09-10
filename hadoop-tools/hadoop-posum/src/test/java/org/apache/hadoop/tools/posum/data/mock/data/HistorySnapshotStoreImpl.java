package org.apache.hadoop.tools.posum.data.mock.data;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.call.ThreePhaseDatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.data.core.DataImporter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK_HISTORY;
import static org.apache.hadoop.tools.posum.common.util.Utils.ID_FIELD;
import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;

public class HistorySnapshotStoreImpl implements HistorySnapshotStore {

  private Long traceStart = 0L;
  private Long traceFinish = 0L;
  private Long currentOffset = 0L;
  private Long currentTime = 0L;
  private DatabaseReference mainDB = DatabaseReference.getMain();
  private DatabaseReference shadowDB = DatabaseReference.get(DatabaseReference.Type.MAIN, "shadow");
  private DataStore broker;

  public HistorySnapshotStoreImpl(String dataDumpFolderName) {
    broker = new MockDataStoreImpl();
    new DataImporter(dataDumpFolderName).importTo(broker);
    storeAllEntitiesInShadowHistory();
    broker.clearDatabase(mainDB);
    traceStart = findEarliestTime();
    traceFinish = findLatestTime();
  }

  private void storeAllEntitiesInShadowHistory() {
    FindByQueryCall findAll = FindByQueryCall.newInstance(APP_HISTORY, null);
    runOnShadow(StoreAllCall.newInstance(APP_HISTORY, runOnMain(findAll).getEntities()));
    findAll.setEntityCollection(APP);
    runOnShadow(StoreAllCall.newInstance(APP_HISTORY, runOnMain(findAll).getEntities()));
    findAll.setEntityCollection(JOB_HISTORY);
    runOnShadow(StoreAllCall.newInstance(JOB_HISTORY, runOnMain(findAll).getEntities()));
    findAll.setEntityCollection(JOB);
    runOnShadow(StoreAllCall.newInstance(JOB_HISTORY, runOnMain(findAll).getEntities()));
    findAll.setEntityCollection(JOB_CONF_HISTORY);
    runOnShadow(StoreAllCall.newInstance(JOB_CONF_HISTORY, runOnMain(findAll).getEntities()));
    findAll.setEntityCollection(JOB_CONF);
    runOnShadow(StoreAllCall.newInstance(JOB_CONF_HISTORY, runOnMain(findAll).getEntities()));
    findAll.setEntityCollection(TASK_HISTORY);
    runOnShadow(StoreAllCall.newInstance(TASK_HISTORY, runOnMain(findAll).getEntities()));
    findAll.setEntityCollection(TASK);
    runOnShadow(StoreAllCall.newInstance(TASK_HISTORY, runOnMain(findAll).getEntities()));
    findAll.setEntityCollection(COUNTER_HISTORY);
    runOnShadow(StoreAllCall.newInstance(COUNTER_HISTORY, runOnMain(findAll).getEntities()));
    findAll.setEntityCollection(COUNTER);
    runOnShadow(StoreAllCall.newInstance(COUNTER_HISTORY, runOnMain(findAll).getEntities()));
  }

  private Long findEarliestTime() {
    List<JobProfile> jobSingletonList = runOnShadow(
      FindByQueryCall.newInstance(JOB_HISTORY, null, "startTime", false, 0, 1)
    ).getEntities();
    return jobSingletonList.isEmpty() ? 0L : orZero(jobSingletonList.get(0).getStartTime());
  }

  private Long findLatestTime() {
    Long latestTime = 0L;
    FindByQueryCall findLastEntity =
      FindByQueryCall.newInstance(JOB_HISTORY, null, "finishTime", true, 0, 1);
    List<JobProfile> jobSingletonList = runOnShadow(findLastEntity).getEntities();
    if (!jobSingletonList.isEmpty())
      latestTime = orZero(jobSingletonList.get(0).getFinishTime());
    // check if there are any tasks that finished later
    findLastEntity.setEntityCollection(DataEntityCollection.TASK_HISTORY);
    List<TaskProfile> taskSingletonList = runOnShadow(findLastEntity).getEntities();
    if (!taskSingletonList.isEmpty()) {
      Long latestTaskFinish = taskSingletonList.get(0).getFinishTime();
      if (latestTaskFinish > latestTime)
        latestTime = latestTaskFinish;
    }
    // check if there are any tasks that started even later
    findLastEntity.setSortField("startTime");
    taskSingletonList = runOnShadow(findLastEntity).getEntities();
    if (!taskSingletonList.isEmpty()) {
      Long latestTaskStart = taskSingletonList.get(0).getStartTime();
      if (latestTaskStart > latestTime)
        latestTime = latestTaskStart;
    }
    return latestTime;
  }


  private void recomputeSnapshot() {
    // get all finished job ids in shadow that finished before currentTime
    List<String> newFinishedJobs = getFinishedJobIds();
    // get all finished job ids in main
    List<String> oldFinishedJobs = getOldFinishedJobIds();

    List<String> toRemove = new ArrayList<>(oldFinishedJobs);
    toRemove.removeAll(newFinishedJobs);
    removeHistoryDataRelatedToJobs(toRemove);

    List<String> toAdd = new ArrayList<>(newFinishedJobs);
    toAdd.removeAll(oldFinishedJobs);

    addFinishedAppsRelatedToJobs(toAdd);
    addFinishedJobInfoRelatedToJobs(toAdd);
    addFinishedTaskInfoRelatedToJobs(toAdd);

    // get all finished job ids in shadow that started before currentTime, but have not finished
    List<String> newRunningJobs = getUnfinishedJobIds();
    // get all job ids in main
    List<String> oldRunningJobs = getOldUnfinishedJobIds();

    toRemove = new ArrayList<>(oldRunningJobs);
    toRemove.removeAll(newRunningJobs);
    removeDataRelatedToJobs(toRemove);
    toAdd = new ArrayList<>(newRunningJobs);
    toAdd.removeAll(oldRunningJobs);

    addUnfinishedAppsRelatedToJobs(toAdd);
    updateUnfinishedJobInfo(oldRunningJobs, newRunningJobs);
  }

  private List<String> getFinishedJobIds() {
    return runOnShadow(
      IdsByQueryCall.newInstance(JOB_HISTORY,
        QueryUtils.and(
          QueryUtils.isNot("finishTime", 0L),
          QueryUtils.lessThanOrEqual("finishTime", currentTime)))
    ).getEntries();
  }

  private List<String> getOldFinishedJobIds() {
    return runOnMain(
      IdsByQueryCall.newInstance(JOB_HISTORY, null)
    ).getEntries();
  }

  private List<String> getUnfinishedJobIds() {
    return runOnShadow(
      IdsByQueryCall.newInstance(JOB_HISTORY,
        QueryUtils.and(
          QueryUtils.lessThanOrEqual("startTime", currentTime),
          QueryUtils.or(
            QueryUtils.greaterThan("finishTime", currentTime),
            QueryUtils.is("finishTime", 0L)
          )))
    ).getEntries();
  }

  private List<String> getOldUnfinishedJobIds() {
    return runOnMain(
      IdsByQueryCall.newInstance(DataEntityCollection.JOB, null)
    ).getEntries();
  }

  private void removeHistoryDataRelatedToJobs(List<String> toRemove) {
    removeDataRelatedToJobs(toRemove, true);
  }

  private void removeDataRelatedToJobs(List<String> toRemove) {
    removeDataRelatedToJobs(toRemove, false);
  }

  private void removeDataRelatedToJobs(List<String> toRemove, boolean fromHistory) {
    List<String> appIdsToRemove = new ArrayList<>(toRemove.size());
    for (String jobId : toRemove) {
      JobId realId = Utils.parseJobId(jobId);
      appIdsToRemove.add(realId.getAppId().toString());
    }

    List<String> taskIdsToRemove = runOnMain(
      IdsByQueryCall.newInstance(fromHistory ? TASK_HISTORY : TASK,
        QueryUtils.in("jobId", toRemove))
    ).getEntries();

    TransactionCall deleteAssociatedCall = TransactionCall.newInstance()
      .addCall(DeleteByQueryCall.newInstance(fromHistory ? APP_HISTORY : APP,
        QueryUtils.in(ID_FIELD, appIdsToRemove)))
      .addCall(DeleteByQueryCall.newInstance(fromHistory ? JOB_HISTORY : JOB,
        QueryUtils.in(ID_FIELD, toRemove)))
      .addCall(DeleteByQueryCall.newInstance(fromHistory ? TASK_HISTORY : TASK,
        QueryUtils.in(ID_FIELD, taskIdsToRemove)))
      .addCall(DeleteByQueryCall.newInstance(fromHistory ? JOB_CONF_HISTORY : JOB_CONF,
        QueryUtils.in(ID_FIELD, toRemove)))
      .addCall(DeleteByQueryCall.newInstance(fromHistory ? COUNTER_HISTORY : COUNTER,
        QueryUtils.in(ID_FIELD, toRemove)))
      // delete also the counters of their tasks
      .addCall(DeleteByQueryCall.newInstance(fromHistory ? COUNTER_HISTORY : COUNTER,
        QueryUtils.in(ID_FIELD, taskIdsToRemove)));
    runOnMain(deleteAssociatedCall);

  }

  private void addFinishedAppsRelatedToJobs(List<String> jobIds) {
    addAppsRelatedToJobs(jobIds, true);
  }

  private void addUnfinishedAppsRelatedToJobs(List<String> jobIds) {
    addAppsRelatedToJobs(jobIds, false);
  }

  private void addAppsRelatedToJobs(List<String> jobIds, boolean toHistory) {
    List<String> appIdsToUpdate = new ArrayList<>(jobIds.size());
    for (String jobId : jobIds) {
      JobId realId = Utils.parseJobId(jobId);
      appIdsToUpdate.add(realId.getAppId().toString());
    }
    List<AppProfile> apps = runOnShadow(
      FindByQueryCall.newInstance(APP_HISTORY, QueryUtils.in(ID_FIELD, appIdsToUpdate))
    ).getEntities();
    if (!toHistory)
      obfuscateApps(apps);
    runOnMain(StoreAllCall.newInstance(toHistory ? APP_HISTORY : APP, apps));
  }

  private void obfuscateApps(List<AppProfile> apps) {
    for (AppProfile app : apps) {
      app.setFinishTime(0L);
    }
  }

  private void addFinishedTaskInfoRelatedToJobs(List<String> toAdd) {
    List<String> taskIdsToAdd = runOnShadow(
      IdsByQueryCall.newInstance(TASK_HISTORY, QueryUtils.in("jobId", toAdd))
    ).getEntries();
    FindByQueryCall getInfo = FindByQueryCall.newInstance(TASK_HISTORY,
      QueryUtils.in(ID_FIELD, taskIdsToAdd));
    runOnMain(StoreAllCall.newInstance(TASK_HISTORY, runOnShadow(getInfo).getEntities()));
    getInfo.setEntityCollection(COUNTER_HISTORY);
    runOnMain(StoreAllCall.newInstance(COUNTER_HISTORY, runOnShadow(getInfo).getEntities()));
  }

  private void addFinishedJobInfoRelatedToJobs(List<String> toAdd) {
    FindByQueryCall getInfo = FindByQueryCall.newInstance(JOB_HISTORY, QueryUtils.in(ID_FIELD, toAdd));
    runOnMain(StoreAllCall.newInstance(JOB_HISTORY, runOnShadow(getInfo).getEntities()));
    getInfo.setEntityCollection(JOB_CONF_HISTORY);
    runOnMain(StoreAllCall.newInstance(JOB_CONF_HISTORY, runOnShadow(getInfo).getEntities()));
    getInfo.setEntityCollection(COUNTER_HISTORY);
    runOnMain(StoreAllCall.newInstance(COUNTER_HISTORY, runOnShadow(getInfo).getEntities()));
  }

  private void updateUnfinishedJobInfo(List<String> oldRunningJobs, List<String> newRunningJobs) {
    List<String> newJobs = new ArrayList<>(newRunningJobs);
    newJobs.removeAll(oldRunningJobs);
    List<JobProfile> runningJobs = runOnShadow(
      FindByQueryCall.newInstance(JOB_HISTORY, QueryUtils.in(ID_FIELD, newRunningJobs))
    ).getEntities();

    for (JobProfile job : runningJobs) {
      updateUnfinishedJobInfo(job, newJobs.contains(job.getId()));
    }
  }

  private void updateUnfinishedJobInfo(JobProfile job, boolean isNewJob) {
    TransactionCall updateCalls = TransactionCall.newInstance();
    job.setFinishTime(0L);
    FindByQueryCall getTasks = FindByQueryCall.newInstance(TASK_HISTORY, QueryUtils.is("jobId", job.getId()));
    List<TaskProfile> tasks = runOnShadow(getTasks).getEntities();
    for (TaskProfile task : tasks) {
      updateCalls.addAllCalls(addTaskInfo(task));
    }
    Utils.updateJobStatisticsFromTasks(job, tasks);
    updateCalls.addCall(UpdateOrStoreCall.newInstance(JOB, job));
    if (isNewJob) {
      // add its configuration
      updateCalls.addCall(StoreCall.newInstance(JOB_CONF, runOnShadow(
        FindByIdCall.newInstance(JOB_CONF_HISTORY, job.getId())
      ).getEntity()));
      // do not add counters because job is not yet done
    }
    runOnMain(updateCalls);
  }

  private List<? extends ThreePhaseDatabaseCall> addTaskInfo(TaskProfile task) {
    List<ThreePhaseDatabaseCall> ret = new LinkedList<>();
    if (orZero(task.getFinishTime()) > currentTime)
      task.setFinishTime(0L);
    else {
      // add counters only for finished tasks
      FindByIdCall findCounters = FindByIdCall.newInstance(COUNTER_HISTORY, task.getId());
      ret.add(UpdateOrStoreCall.newInstance(COUNTER, runOnShadow(findCounters).getEntity()));
    }
    ret.add(UpdateOrStoreCall.newInstance(TASK, task));
    return ret;
  }

  private <T extends Payload> T runOnMain(DatabaseCall<T> call) {
    return broker.execute(call, mainDB);
  }

  private <T extends Payload> T runOnShadow(DatabaseCall<T> call) {
    return broker.execute(call, shadowDB);
  }


  @Override
  public Long getSnapshotTime() {
    return currentTime;
  }

  @Override
  public void setSnapshotTime(Long time) {
    currentTime = time;
    currentOffset = time - traceStart;
    recomputeSnapshot();
  }

  @Override
  public Long getSnapshotOffset() {
    return currentOffset;
  }

  @Override
  public void setSnapshotOffset(Long offset) {
    currentTime = traceStart + offset;
    currentOffset = offset;
    recomputeSnapshot();
  }

  @Override
  public Long getTraceStartTime() {
    return traceStart;
  }

  @Override
  public Long getTraceFinishTime() {
    return traceFinish;
  }

  @Override
  public <T extends Payload> T execute(DatabaseCall<T> call, DatabaseReference db) {
    return broker.execute(call, db);
  }

  @Override
  public Map<DatabaseReference, List<DataEntityCollection>> listCollections() {
    return broker.listCollections();
  }

  @Override
  public void clear() {
    broker.clear();
  }

  @Override
  public void clearDatabase(DatabaseReference db) {
    broker.clearDatabase(db);
  }

  @Override
  public void copyDatabase(DatabaseReference sourceDB, DatabaseReference destinationDB) {
    broker.copyDatabase(sourceDB, destinationDB);
  }

  @Override
  public void copyCollection(DataEntityCollection collection, DatabaseReference sourceDB, DatabaseReference destinationDB) {
    broker.copyCollection(collection, sourceDB, destinationDB);
  }

  @Override
  public void awaitUpdate(DatabaseReference db) throws InterruptedException {
    broker.awaitUpdate(db);
  }

  @Override
  public void notifyUpdate(DatabaseReference db) {
    broker.notifyUpdate(db);
  }

}
