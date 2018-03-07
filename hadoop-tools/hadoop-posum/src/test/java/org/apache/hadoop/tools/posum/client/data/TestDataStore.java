package org.apache.hadoop.tools.posum.client.data;

import org.apache.hadoop.tools.posum.common.records.call.DeleteByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.DeleteByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.call.SaveJobFlexFieldsCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreLogCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.HistoryProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.test.Utils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.hadoop.tools.posum.client.data.DatabaseUtils.ID_FIELD;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference.Type.MAIN;
import static org.apache.hadoop.tools.posum.test.Utils.APP1;
import static org.apache.hadoop.tools.posum.test.Utils.APP1_ID;
import static org.apache.hadoop.tools.posum.test.Utils.APP2;
import static org.apache.hadoop.tools.posum.test.Utils.APP2_ID;
import static org.apache.hadoop.tools.posum.test.Utils.APP3;
import static org.apache.hadoop.tools.posum.test.Utils.APP3_ID;
import static org.apache.hadoop.tools.posum.test.Utils.CLUSTER_TIMESTAMP;
import static org.apache.hadoop.tools.posum.test.Utils.JOB1;
import static org.apache.hadoop.tools.posum.test.Utils.JOB1_ID;
import static org.apache.hadoop.tools.posum.test.Utils.JOB2;
import static org.apache.hadoop.tools.posum.test.Utils.JOB2_ID;
import static org.apache.hadoop.tools.posum.test.Utils.JOB3;
import static org.apache.hadoop.tools.posum.test.Utils.JOB3_ID;
import static org.apache.hadoop.tools.posum.test.Utils.USER2;
import static org.apache.hadoop.tools.posum.test.Utils.newView;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class TestDataStore {
  protected DataStore dataStore;
  protected Database db;

  @Before
  public void setUp() throws Exception {
    setUpDataStore();
    db = Database.from(dataStore, DatabaseReference.get(MAIN, newView()));
    Utils.loadThreeDefaultAppsAndJobs(db);
  }

  protected abstract void setUpDataStore() throws Exception;

  @After
  public void tearDown() throws Exception {
    dataStore.clearDatabase(db.getTarget());
  }

  @Test
  public void testFindById() throws Exception {
    FindByIdCall findApp = FindByIdCall.newInstance(APP, APP1_ID.toString());
    AppProfile app = db.execute(findApp).getEntity();
    assertThat(app, is(APP1));
  }

  @Test
  public void testListIds() throws Exception {
    IdsByQueryCall listIds = IdsByQueryCall.newInstance(APP,
      QueryUtils.is("user", USER2));
    List<String> returnedAppIds = db.execute(listIds).getEntries();
    Collections.sort(returnedAppIds);
    assertThat(returnedAppIds, containsInAnyOrder(APP2_ID.toString(), APP3_ID.toString()));
  }

  @Test
  public void testFindAll() throws Exception {
    FindByQueryCall findAll = FindByQueryCall.newInstance(JOB, null);
    List<JobProfile> jobs = db.execute(findAll).getEntities();
    assertThat(jobs, containsInAnyOrder(JOB1, JOB2, JOB3));
  }

  @Test
  public void testFindSelected() throws Exception {
    DatabaseQuery query = QueryUtils.and(
      QueryUtils.is("finishTime", JOB1.getFinishTime()),
      QueryUtils.is("totalMapTasks", 1)
    );
    FindByQueryCall findByProperties = FindByQueryCall.newInstance(JOB, query);
    List<JobProfile> jobs = db.execute(findByProperties).getEntities();
    assertThat(jobs.get(0), is(JOB1));
  }

  @Test
  public void testSortByString() throws Exception {
    IdsByQueryCall sortedIds = IdsByQueryCall.newInstance(APP, null, ID_FIELD, true);
    List<String> ids = db.execute(sortedIds).getEntries();
    assertThat(ids, contains(APP3_ID.toString(), APP2_ID.toString(), APP1_ID.toString()));
  }

  @Test
  public void testSortByNumber() throws Exception {
    FindByQueryCall findAll = FindByQueryCall.newInstance(JOB, null);
    List<JobProfile> jobs = db.execute(findAll).getEntities();

    IdsByQueryCall sortedIds = IdsByQueryCall.newInstance(JOB, null, "totalReduceTasks", false);
    List<String> ids = db.execute(sortedIds).getEntries();
    assertThat(ids, contains(JOB2_ID.toString(), JOB1_ID.toString(), JOB3_ID.toString()));
  }

  @Test
  public void testFindLimit() throws Exception {
    FindByQueryCall findByFinishTime = FindByQueryCall.newInstance(APP,
      QueryUtils.is("finishTime", APP2.getFinishTime()),
      ID_FIELD,
      false
    );
    List<AppProfile> apps = db.execute(findByFinishTime).getEntities();
    assertThat(apps, contains(APP2, APP3));
    findByFinishTime.setLimitOrZero(1);
    apps = db.execute(findByFinishTime).getEntities();
    assertThat(apps, contains(APP2));
  }

  @Test
  public void testFindOffset() throws Exception {
    FindByQueryCall findByFinishTime = FindByQueryCall.newInstance(
      APP,
      QueryUtils.is("finishTime", APP2.getFinishTime()),
      ID_FIELD,
      false
    );
    List<AppProfile> apps = db.execute(findByFinishTime).getEntities();
    assertThat(apps, contains(APP2, APP3));
    findByFinishTime.setOffsetOrZero(1);
    apps = db.execute(findByFinishTime).getEntities();
    assertThat(apps, contains(APP3));
  }

  @Test
  public void testFindOffsetAndLimit() throws Exception {
    FindByQueryCall findByFinishTime = FindByQueryCall.newInstance(
      APP,
      QueryUtils.is("finishTime", APP2.getFinishTime()),
      ID_FIELD,
      false
    );
    List<AppProfile> apps = db.execute(findByFinishTime).getEntities();
    assertThat(apps, contains(APP2, APP3));
    findByFinishTime.setOffsetOrZero(-1);
    findByFinishTime.setLimitOrZero(2);
    apps = db.execute(findByFinishTime).getEntities();
    assertThat(apps, contains(APP3));
  }

  @Test
  public void testInStringsQuery() throws Exception {
    IdsByQueryCall findTwoAndThree = IdsByQueryCall.newInstance(
      APP,
      QueryUtils.in(ID_FIELD, Arrays.<Object>asList(APP2_ID.toString(), APP3_ID.toString())),
      ID_FIELD,
      false
    );
    List<String> appIds = db.execute(findTwoAndThree).getEntries();
    assertThat(appIds, contains(APP2_ID.toString(), APP3_ID.toString()));
  }

  @Test
  public void testInNumbersQuery() throws Exception {
    IdsByQueryCall findTwoAndThree = IdsByQueryCall.newInstance(
      APP,
      QueryUtils.in("finishTime", Collections.<Object>singletonList(APP2.getFinishTime())),
      ID_FIELD,
      false
    );
    List<String> appIds = db.execute(findTwoAndThree).getEntries();
    assertThat(appIds, contains(APP2_ID.toString(), APP3_ID.toString()));
  }

  @Test
  public void testStoreFailsForDuplicate() throws Exception {
    try {
      AppProfile app3 = APP3.copy();
      app3.setName("Modified Name");
      StoreCall storeApp = StoreCall.newInstance(APP, app3);
      db.execute(storeApp);
      fail();
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("duplicate key"));
    }
  }

  @Test
  public void testUpdateOrStore() throws Exception {
    AppProfile app3 = APP3.copy();
    String modifiedName = "Modified Name";
    YarnApplicationState state = YarnApplicationState.ACCEPTED;
    app3.setName(modifiedName);
    app3.setState(state);
    UpdateOrStoreCall updateApp = UpdateOrStoreCall.newInstance(APP, app3);
    String upsertedId = db.execute(updateApp).getValueAs();
    assertNull(upsertedId);
    FindByQueryCall findAppsByName = FindByQueryCall.newInstance(APP,
      QueryUtils.is("name", modifiedName));
    List<AppProfile> returnedApps = db.execute(findAppsByName).getEntities();
    assertThat(returnedApps, contains(app3));

    AppProfile app4 = Records.newRecord(AppProfile.class);
    ApplicationId app4Id = ApplicationId.newInstance(CLUSTER_TIMESTAMP, 4);
    String app4IdString = app4Id.toString();
    app4.setId(app4IdString);
    app4.setName(modifiedName);
    updateApp.setEntity(app4);
    upsertedId = db.execute(updateApp).getValueAs();
    assertThat(app4.getId(), is(upsertedId));
    returnedApps = db.execute(findAppsByName).getEntities();
    assertThat(returnedApps, containsInAnyOrder(app3, app4));
  }

  @Test
  public void testDeleteById() throws Exception {
    DeleteByIdCall deleteApp = DeleteByIdCall.newInstance(APP, APP2_ID.toString());
    db.execute(deleteApp);
    IdsByQueryCall listIds = IdsByQueryCall.newInstance(APP,
      QueryUtils.is("user", USER2));
    List<String> returnedAppIds = db.execute(listIds).getEntries();
    assertThat(returnedAppIds, contains(APP3_ID.toString()));
  }

  @Test
  public void testDeleteByParams() throws Exception {
    IdsByQueryCall listIds = IdsByQueryCall.newInstance(JOB, null);
    List<String> returnedJobIds = db.execute(listIds).getEntries();
    assertThat(returnedJobIds, containsInAnyOrder(JOB1_ID.toString(), JOB2_ID.toString(), JOB3_ID.toString()));
    DeleteByQueryCall deleteJob = DeleteByQueryCall.newInstance(JOB,
      QueryUtils.is("appId", APP2_ID.toString()));
    db.execute(deleteJob);
    listIds.setQuery(null);
    returnedJobIds = db.execute(listIds).getEntries();
    assertThat(returnedJobIds, containsInAnyOrder(JOB1_ID.toString(), JOB3_ID.toString()));
  }

  @Test
  public void testJobByAppId() throws Exception {
    JobForAppCall getJob = JobForAppCall.newInstance(APP2_ID.toString());
    JobProfile job = db.execute(getJob).getEntity();
    assertThat(job, is(JOB2));
  }

  @Test
  public void testSaveFlexFields() throws Exception {
    FindByIdCall findJob = FindByIdCall.newInstance(JOB, JOB2_ID.toString());
    JobProfile job = db.execute(findJob).getEntity();
    assertThat(job.getFlexFields(), nullValue());
    String key = "SOME_FLEX_KEY", value = "6";
    SaveJobFlexFieldsCall saveFlexFields = SaveJobFlexFieldsCall.newInstance(JOB2_ID.toString(),
      Collections.singletonMap(key, value), false);
    db.execute(saveFlexFields);
    job = db.execute(findJob).getEntity();
    assertThat(job.getFlexFields().entrySet(), hasSize(1));
    assertThat(job.getFlexField(key), is(value));
  }

  @Test
  public void testTransaction() throws Exception {
    TransactionCall transaction = TransactionCall.newInstance();
    AppProfile app3 = APP3.copy();
    String modifiedName = "Modified Name";
    YarnApplicationState state = YarnApplicationState.ACCEPTED;
    app3.setName(modifiedName);
    app3.setState(state);
    transaction.addCall(UpdateOrStoreCall.newInstance(APP, app3));

    AppProfile app4 = Records.newRecord(AppProfile.class);
    ApplicationId app4Id = ApplicationId.newInstance(CLUSTER_TIMESTAMP, 4);
    String app4IdString = app4Id.toString();
    app4.setId(app4IdString);
    app4.setName(modifiedName);
    transaction.addCall(StoreCall.newInstance(APP, app4));

    transaction.addCall(DeleteByIdCall.newInstance(APP, APP1_ID.toString()));
    db.execute(transaction);

    FindByQueryCall listAll = FindByQueryCall.newInstance(APP, null);
    List<AppProfile> allApps = db.execute(listAll).getEntities();
    assertThat(allApps, containsInAnyOrder(APP2, app3, app4));

    transaction.setCallList(Collections.singletonList(DeleteByIdCall.newInstance(APP, app4.getId())));
    db.execute(transaction);

    allApps = db.execute(listAll).getEntities();
    assertThat(allApps, containsInAnyOrder(APP2, app3));
  }

  @Test
  public void testListCollections() throws Exception {
    Map<DatabaseReference, List<DataEntityCollection>> collectionMap = dataStore.listCollections();
    System.out.println("Collections are: " + collectionMap);
    List<DataEntityCollection> collections = collectionMap.get(db.getTarget());
    assertTrue(collections.contains(JOB));
    assertTrue(collections.contains(APP));
  }

  @Test
  public void testClear() throws Exception {
    dataStore.clear();
    assertThat(dataStore.listCollections().entrySet(), hasSize(0));
    FindByQueryCall allEntities = FindByQueryCall.newInstance(APP, null);
    assertThat(db.execute(allEntities).getEntities(), hasSize(0));
    allEntities.setEntityCollection(JOB);
    assertThat(db.execute(allEntities).getEntities(), hasSize(0));
  }

  private class Reader extends Thread {
    private Random random;
    private volatile boolean done = false;

    private Reader(Random random) {
      this.random = random;
    }

    public void stopNow() {
      done = true;
    }

    @Override
    public void run() {
      while (!done) {
        if (random.nextInt() % 100 != 0) {
          DatabaseQuery query = QueryUtils.and(
            QueryUtils.is("finishTime", JOB1.getFinishTime()),
            QueryUtils.is("totalMapTasks", 1)
          );
          FindByQueryCall findByProperties = FindByQueryCall.newInstance(JOB, query);
          db.execute(findByProperties).getEntities();
        } else {
          dataStore.clear();
        }
      }
    }
  }

  @Test
  public void testClearWhileBeingAccessed() throws Exception {
    List<Reader> readers = new ArrayList<>(100);
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < 100; i++) {
      Reader thread = new Reader(random);
      readers.add(thread);
      thread.start();
    }
    for (Reader thread : readers) {
      thread.stopNow();
      thread.join();
    }
  }

  @Test
  public void testMove() throws Exception {
    int collectionNo = dataStore.listCollections().get(db.getTarget()).size();
    IdsByQueryCall allIds = IdsByQueryCall.newInstance(APP, null);
    int appNo = db.execute(allIds).getEntries().size();
    allIds.setEntityCollection(JOB);
    int jobNo = db.execute(allIds).getEntries().size();
    DatabaseReference otherDB = DatabaseReference.get(DatabaseReference.Type.MAIN, "testCopy");
    dataStore.copyDatabase(db.getTarget(), otherDB);
    dataStore.clearDatabase(db.getTarget());
    Map<DatabaseReference, List<DataEntityCollection>> collectionMap = dataStore.listCollections();
    assertNull(collectionMap.get(db.getTarget()));
    assertThat(collectionMap.get(otherDB), hasSize(collectionNo));
    assertThat(dataStore.execute(allIds, otherDB).getEntries(), hasSize(jobNo));
    allIds.setEntityCollection(APP);
    assertThat(dataStore.execute(allIds, otherDB).getEntries(), hasSize(appNo));
  }

  @Test
  public void testClearEmptyDatabase() throws Exception {
    dataStore.clearDatabase(db.getTarget());
    dataStore.clearDatabase(db.getTarget());
  }

  @Test
  public void testLogging() throws Exception {
    String message = "Some message";
    LogEntry logEntry = DatabaseUtils.newLogEntry(message);
    Long timestamp = logEntry.getLastUpdated();
    String logId = dataStore.execute(StoreLogCall.newInstance(logEntry), null).getValueAs();
    assertNotNull(logId);
    FindByIdCall getLog = FindByIdCall.newInstance(
      DataEntityCollection.AUDIT_LOG,
      logId
    );
    LogEntry<SimplePropertyPayload> log = dataStore.execute(getLog, DatabaseReference.getLogs()).getEntity();
    assertThat(log.getId(), is(logId));
    assertThat(log.getLastUpdated(), is(timestamp));
    assertThat(log.getDetails().getValue(), is((Object) message));
  }

  @Test
  public void testLogChronology() throws Exception {
    LogEntry first =  DatabaseUtils.newLogEntry("First");
    dataStore.execute(StoreLogCall.newInstance(first), null);

    LogEntry second = DatabaseUtils.newLogEntry("Second");
    Long secondTimestamp = first.getLastUpdated() + 1000;
    second.setLastUpdated(secondTimestamp);
    dataStore.execute(StoreLogCall.newInstance(second), null);

    FindByQueryCall getLog = FindByQueryCall.newInstance(
      DataEntityCollection.AUDIT_LOG,
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.GENERAL),
        QueryUtils.greaterThan("lastUpdated", first.getLastUpdated())
      )
    );
    List<LogEntry> logs = dataStore.execute(getLog, DatabaseReference.getLogs()).getEntities();
    assertThat(logs, contains(second));

    getLog = FindByQueryCall.newInstance(
      DataEntityCollection.AUDIT_LOG,
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.GENERAL),
        QueryUtils.greaterThanOrEqual("lastUpdated", first.getLastUpdated()),
        QueryUtils.lessThan("lastUpdated", secondTimestamp)
      )
    );
    logs = dataStore.execute(getLog, DatabaseReference.getLogs()).getEntities();
    assertThat(logs, contains(first));
  }

  @Test
  public void testHistoryProfileManipulation() {
    HistoryProfile appHistory = new HistoryProfilePBImpl<>(DataEntityCollection.APP, APP1);
    db.execute(StoreCall.newInstance(HISTORY, appHistory));

    String appId = APP1_ID.toString();
    FindByQueryCall findHistory = FindByQueryCall.newInstance(HISTORY, QueryUtils.is("originalId", appId));
    List<HistoryProfile> profilesById = db.execute(findHistory).getEntities();
    assertThat(profilesById, hasSize(1));
    assertThat((AppProfile) profilesById.get(0).getOriginal(), is(APP1));
  }
}
