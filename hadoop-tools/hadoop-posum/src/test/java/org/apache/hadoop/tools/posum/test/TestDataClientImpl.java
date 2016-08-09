package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.tools.posum.common.records.call.*;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.database.client.Database;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.apache.hadoop.tools.posum.test.Utils.*;
import static org.junit.Assert.*;

/**
 * Created by ane on 7/26/16.
 */
public abstract class TestDataClientImpl {
    protected Database db;
    protected final Long clusterTimestamp = System.currentTimeMillis();

    @Before
    public void setUp() throws Exception {
        setUpDataStore();
        Utils.loadThreeDefaultAppsAndJobs(clusterTimestamp, db);
    }

    protected abstract void setUpDataStore() throws Exception;

    @Test
    public void testFindById() throws Exception {
        String appId = ApplicationId.newInstance(clusterTimestamp, 1).toString();
        FindByIdCall findApp = FindByIdCall.newInstance(DataEntityCollection.APP, appId);
        AppProfile app = db.executeDatabaseCall(findApp).getEntity();
        assertEquals(appId, app.getId());
        assertEquals(JOB_NAME_ROOT + " 1", app.getName());
        assertEquals(FIRST_USER, app.getUser());
        assertEquals(Long.valueOf(clusterTimestamp - 5 * DURATION_UNIT), app.getStartTime());
        assertEquals(clusterTimestamp, app.getFinishTime());
    }

    @Test
    public void testListIds() throws Exception {
        IdsByParamsCall listIds = IdsByParamsCall.newInstance(DataEntityCollection.APP,
                Collections.singletonMap("user", (Object) SECOND_USER));
        List<String> returnedAppIds = db.executeDatabaseCall(listIds).getEntries();
        String appId2 = ApplicationId.newInstance(clusterTimestamp, 2).toString();
        String appId3 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        Collections.sort(returnedAppIds);
        assertArrayEquals(new String[]{appId2, appId3}, returnedAppIds.toArray());
    }

    @Test
    public void testFindAll() throws Exception {
        FindByParamsCall findAll = FindByParamsCall.newInstance(DataEntityCollection.JOB, Collections.<String, Object>emptyMap());
        List<JobProfile> jobs = db.executeDatabaseCall(findAll).getEntities();
        assertEquals(3, jobs.size());
    }

    @Test
    public void testFindSelected() throws Exception {
        Map<String, Object> properties = new HashMap<>(2);
        properties.put("finishTime", clusterTimestamp - DURATION_UNIT);
        properties.put("totalMapTasks", 10);
        FindByParamsCall findByProperties = FindByParamsCall.newInstance(DataEntityCollection.JOB, properties);
        List<JobProfile> jobs = db.executeDatabaseCall(findByProperties).getEntities();
        assertEquals(1, jobs.size());
        JobId job2Id = new JobIdPBImpl();
        job2Id.setAppId(ApplicationId.newInstance(clusterTimestamp, 2));
        job2Id.setId(2);
        assertEquals(job2Id.toString(), jobs.get(0).getId());
    }

    @Test
    public void testSortByString() throws Exception {
        IdsByParamsCall sortedIds = IdsByParamsCall.newInstance(
                DataEntityCollection.APP,
                Collections.<String, Object>emptyMap(),
                "_id",
                true
        );
        List<String> ids = db.executeDatabaseCall(sortedIds).getEntries();
        assertArrayEquals(new String[]{
                ApplicationId.newInstance(clusterTimestamp, 3).toString(),
                ApplicationId.newInstance(clusterTimestamp, 2).toString(),
                ApplicationId.newInstance(clusterTimestamp, 1).toString()
        }, ids.toArray(new String[ids.size()]));

    }

    @Test
    public void testSortByNumber() throws Exception {
        IdsByParamsCall sortedIds = IdsByParamsCall.newInstance(
                DataEntityCollection.APP,
                Collections.<String, Object>emptyMap(),
                "startTime",
                false
        );
        List<String> ids = db.executeDatabaseCall(sortedIds).getEntries();
        assertArrayEquals(new String[]{
                ApplicationId.newInstance(clusterTimestamp, 1).toString(),
                ApplicationId.newInstance(clusterTimestamp, 3).toString(),
                ApplicationId.newInstance(clusterTimestamp, 2).toString()
        }, ids.toArray(new String[ids.size()]));
    }

    @Test
    public void testFindLimit() throws Exception {
        FindByParamsCall findByFinishTime = FindByParamsCall.newInstance(
                DataEntityCollection.APP,
                Collections.singletonMap("finishTime", (Object) (clusterTimestamp - DURATION_UNIT)),
                "_id",
                false
        );
        List<AppProfile> apps = db.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(2, apps.size());
        findByFinishTime.setLimitOrZero(1);
        apps = db.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(1, apps.size());
        ApplicationId app2Id = ApplicationId.newInstance(clusterTimestamp, 2);
        assertEquals(app2Id.toString(), apps.get(0).getId());
    }

    @Test
    public void testFindOffset() throws Exception {
        FindByParamsCall findByFinishTime = FindByParamsCall.newInstance(
                DataEntityCollection.APP,
                Collections.singletonMap("finishTime", (Object) (clusterTimestamp - DURATION_UNIT)),
                "_id",
                false
        );
        List<AppProfile> apps = db.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(2, apps.size());
        findByFinishTime.setOffsetOrZero(1);
        apps = db.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(1, apps.size());
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        assertEquals(app3Id.toString(), apps.get(0).getId());
    }

    @Test
    public void testFindOffsetAndLimit() throws Exception {
        FindByParamsCall findByFinishTime = FindByParamsCall.newInstance(
                DataEntityCollection.APP,
                Collections.singletonMap("finishTime", (Object) (clusterTimestamp - DURATION_UNIT)),
                "_id",
                false,
                0,
                0
        );
        List<AppProfile> apps = db.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(2, apps.size());
        findByFinishTime.setOffsetOrZero(-1);
        findByFinishTime.setLimitOrZero(2);
        apps = db.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(1, apps.size());
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        assertEquals(app3Id.toString(), apps.get(0).getId());
    }

    @Test
    public void testStoreFailsForDuplicate() throws Exception {
        try {
            AppProfile app3 = Records.newRecord(AppProfile.class);
            ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
            app3.setId(app3Id.toString());
            app3.setName("Modified Name");
            app3.setQueue("Now it has a queue");
            StoreCall storeApp = StoreCall.newInstance(DataEntityCollection.APP, app3);
            db.executeDatabaseCall(storeApp);
            fail();
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("duplicate key"));
        }
    }

    @Test
    public void testUpdateOrStore() throws Exception {
        AppProfile app3 = Records.newRecord(AppProfile.class);
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        app3.setId(app3Id.toString());
        String modifiedName = "Modified Name", queueName = "NotNullQueue";
        app3.setName(modifiedName);
        app3.setQueue(queueName);
        UpdateOrStoreCall updateApp = UpdateOrStoreCall.newInstance(DataEntityCollection.APP, app3);
        String upsertedId = (String) db.executeDatabaseCall(updateApp).getValue();
        assertNull(upsertedId);
        FindByParamsCall findAppsByName = FindByParamsCall.newInstance(DataEntityCollection.APP,
                Collections.singletonMap("name", (Object) modifiedName));
        List<AppProfile> returnedApps = db.executeDatabaseCall(findAppsByName).getEntities();
        assertEquals(1, returnedApps.size());
        AppProfile returned = returnedApps.get(0);
        assertEquals(app3Id.toString(), returned.getId());
        assertEquals(queueName, returned.getQueue());
        assertEquals("", returned.getUser());
        assertEquals(new Long(0), returned.getStartTime());
        assertEquals(new Long(0), returned.getFinishTime());

        AppProfile app4 = Records.newRecord(AppProfile.class);
        ApplicationId app4Id = ApplicationId.newInstance(clusterTimestamp, 4);
        String app4IdString = app4Id.toString();
        app4.setId(app4IdString);
        app4.setName(modifiedName);
        updateApp.setEntity(app4);
        upsertedId = (String) db.executeDatabaseCall(updateApp).getValue();
        assertEquals(app4.getId(), upsertedId);
        returnedApps = db.executeDatabaseCall(findAppsByName).getEntities();
        assertEquals(2, returnedApps.size());
        assertTrue(returnedApps.get(0).getId().equals(app4IdString) ||
                returnedApps.get(1).getId().equals(app4IdString));
    }


    @Test
    public void testDeleteById() throws Exception {
        DeleteByIdCall deleteApp = DeleteByIdCall.newInstance(DataEntityCollection.APP,
                ApplicationId.newInstance(clusterTimestamp, 2).toString());
        db.executeDatabaseCall(deleteApp);
        IdsByParamsCall listIds = IdsByParamsCall.newInstance(DataEntityCollection.APP,
                Collections.singletonMap("user", (Object) SECOND_USER));
        List<String> returnedAppIds = db.executeDatabaseCall(listIds).getEntries();
        String appId3 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        assertArrayEquals(new String[]{appId3}, returnedAppIds.toArray());

    }

    @Test
    public void testDeleteByParams() throws Exception {
        IdsByParamsCall listIds = IdsByParamsCall.newInstance(DataEntityCollection.JOB, Collections.<String, Object>emptyMap());
        List<String> returnedJobIds = db.executeDatabaseCall(listIds).getEntries();
        assertEquals(3, returnedJobIds.size());
        String appId1 = ApplicationId.newInstance(clusterTimestamp, 1).toString();
        listIds.setParams(Collections.singletonMap("appId", (Object) appId1));
        returnedJobIds = db.executeDatabaseCall(listIds).getEntries();
        assertEquals(1, returnedJobIds.size());
        DeleteByParamsCall deleteJob = DeleteByParamsCall.newInstance(DataEntityCollection.JOB,
                Collections.singletonMap("appId", (Object) appId1));
        db.executeDatabaseCall(deleteJob);
        listIds.setParams(Collections.<String, Object>emptyMap());
        returnedJobIds = db.executeDatabaseCall(listIds).getEntries();
        assertEquals(2, returnedJobIds.size());
        listIds.setParams(Collections.singletonMap("appId", (Object) appId1));
        returnedJobIds = db.executeDatabaseCall(listIds).getEntries();
        assertEquals(0, returnedJobIds.size());
    }

    @Test
    public void testJobByAppId() throws Exception {
        String appId2 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        JobForAppCall getJob = JobForAppCall.newInstance(appId2, SECOND_USER);
        JobProfile job = db.executeDatabaseCall(getJob).getEntity();
        assertEquals(JOB_NAME_ROOT + " 3", job.getName());
    }

    @Test
    public void testSaveFlexFields() throws Exception {
        String appId2 = ApplicationId.newInstance(clusterTimestamp, 2).toString();
        IdsByParamsCall listIds = IdsByParamsCall.newInstance(DataEntityCollection.JOB,
                Collections.singletonMap("appId", (Object) appId2));
        List<String> returnedJobIds = db.executeDatabaseCall(listIds).getEntries();
        assertEquals(1, returnedJobIds.size());
        String jobId = returnedJobIds.get(0);
        String key = "SOME_FLEX_KEY", value = "6";
        SaveJobFlexFieldsCall saveFlexFields = SaveJobFlexFieldsCall.newInstance(jobId,
                Collections.singletonMap(key, value), false);
        db.executeDatabaseCall(saveFlexFields);
        FindByIdCall findJob = FindByIdCall.newInstance(DataEntityCollection.JOB, jobId);
        JobProfile job = db.executeDatabaseCall(findJob).getEntity();
        assertEquals(1, job.getFlexFields().size());
        assertEquals(value, job.getFlexField(key));
    }

    @Test
    public void testTransaction() throws Exception {
        TransactionCall transaction = TransactionCall.newInstance();
        AppProfile app3 = Records.newRecord(AppProfile.class);
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        app3.setId(app3Id.toString());
        String modifiedName = "Modified Name";
        app3.setName(modifiedName);
        transaction.addCall(UpdateOrStoreCall.newInstance(DataEntityCollection.APP, app3));
        AppProfile app4 = Records.newRecord(AppProfile.class);
        ApplicationId app4Id = ApplicationId.newInstance(clusterTimestamp, 4);
        String app4IdString = app4Id.toString();
        app4.setId(app4IdString);
        app4.setName(modifiedName);
        transaction.addCall(StoreCall.newInstance(DataEntityCollection.APP, app4));
        String appId1 = ApplicationId.newInstance(clusterTimestamp, 1).toString();
        transaction.addCall(DeleteByIdCall.newInstance(DataEntityCollection.APP, appId1));
        db.executeDatabaseCall(transaction);
        IdsByParamsCall listIdsForName = IdsByParamsCall.newInstance(DataEntityCollection.APP,
                Collections.singletonMap("name", (Object) modifiedName));
        List<String> idsForName = db.executeDatabaseCall(listIdsForName).getEntries();
        Collections.sort(idsForName);
        assertArrayEquals(new String[]{app3.getId(), app4.getId()}, idsForName.toArray());
        FindByIdCall findApp = FindByIdCall.newInstance(DataEntityCollection.APP, appId1);
        assertNull(db.executeDatabaseCall(findApp).getEntity());
        transaction.setCallList(Collections.singletonList(
                DeleteByIdCall.newInstance(DataEntityCollection.APP, app4.getId())));
        db.executeDatabaseCall(transaction);
        findApp.setId(app4.getId());
        assertNull(db.executeDatabaseCall(findApp).getEntity());
    }
}
