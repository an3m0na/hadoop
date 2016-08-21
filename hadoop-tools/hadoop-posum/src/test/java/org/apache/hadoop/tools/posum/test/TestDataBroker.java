package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.tools.posum.common.records.call.*;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.database.client.DataBroker;
import org.apache.hadoop.tools.posum.database.client.Database;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.apache.hadoop.tools.posum.common.util.Utils.ID_FIELD;
import static org.apache.hadoop.tools.posum.test.Utils.*;
import static org.junit.Assert.*;

/**
 * Created by ane on 7/26/16.
 */
public abstract class TestDataBroker {
    protected DataBroker dataBroker;
    protected Database mainDB;
    protected final Long clusterTimestamp = System.currentTimeMillis();

    @Before
    public void setUp() throws Exception {
        setUpDataBroker();
        mainDB = dataBroker.bindTo(DataEntityDB.getMain());
        Utils.loadThreeDefaultAppsAndJobs(clusterTimestamp, mainDB);
    }

    protected abstract void setUpDataBroker() throws Exception;

    @Test
    public void testFindById() throws Exception {
        String appId = ApplicationId.newInstance(clusterTimestamp, 1).toString();
        FindByIdCall findApp = FindByIdCall.newInstance(DataEntityCollection.APP, appId);
        AppProfile app = mainDB.executeDatabaseCall(findApp).getEntity();
        assertEquals(appId, app.getId());
        assertEquals(JOB_NAME_ROOT + " 1", app.getName());
        assertEquals(FIRST_USER, app.getUser());
        assertEquals(Long.valueOf(clusterTimestamp - 5 * DURATION_UNIT), app.getStartTime());
        assertEquals(clusterTimestamp, app.getFinishTime());
    }

    @Test
    public void testListIds() throws Exception {
        IdsByQueryCall listIds = IdsByQueryCall.newInstance(DataEntityCollection.APP,
                QueryUtils.is("user", SECOND_USER));
        List<String> returnedAppIds = mainDB.executeDatabaseCall(listIds).getEntries();
        String appId2 = ApplicationId.newInstance(clusterTimestamp, 2).toString();
        String appId3 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        Collections.sort(returnedAppIds);
        assertArrayEquals(new String[]{appId2, appId3}, returnedAppIds.toArray());
    }

    @Test
    public void testFindAll() throws Exception {
        FindByQueryCall findAll = FindByQueryCall.newInstance(DataEntityCollection.JOB, null);
        List<JobProfile> jobs = mainDB.executeDatabaseCall(findAll).getEntities();
        assertEquals(3, jobs.size());
    }

    @Test
    public void testFindSelected() throws Exception {
        DatabaseQuery query = QueryUtils.and(
                QueryUtils.is("finishTime", clusterTimestamp - DURATION_UNIT),
                QueryUtils.is("totalMapTasks", 10)
        );
        FindByQueryCall findByProperties = FindByQueryCall.newInstance(DataEntityCollection.JOB, query);
        List<JobProfile> jobs = mainDB.executeDatabaseCall(findByProperties).getEntities();
        assertEquals(1, jobs.size());
        JobId job2Id = new JobIdPBImpl();
        job2Id.setAppId(ApplicationId.newInstance(clusterTimestamp, 2));
        job2Id.setId(2);
        assertEquals(job2Id.toString(), jobs.get(0).getId());
    }

    @Test
    public void testSortByString() throws Exception {
        IdsByQueryCall sortedIds = IdsByQueryCall.newInstance(DataEntityCollection.APP, null, ID_FIELD, true);
        List<String> ids = mainDB.executeDatabaseCall(sortedIds).getEntries();
        assertArrayEquals(new String[]{
                ApplicationId.newInstance(clusterTimestamp, 3).toString(),
                ApplicationId.newInstance(clusterTimestamp, 2).toString(),
                ApplicationId.newInstance(clusterTimestamp, 1).toString()
        }, ids.toArray(new String[ids.size()]));

    }

    @Test
    public void testSortByNumber() throws Exception {
        IdsByQueryCall sortedIds = IdsByQueryCall.newInstance(DataEntityCollection.APP, null, "startTime", false);
        List<String> ids = mainDB.executeDatabaseCall(sortedIds).getEntries();
        assertArrayEquals(new String[]{
                ApplicationId.newInstance(clusterTimestamp, 1).toString(),
                ApplicationId.newInstance(clusterTimestamp, 3).toString(),
                ApplicationId.newInstance(clusterTimestamp, 2).toString()
        }, ids.toArray(new String[ids.size()]));
    }

    @Test
    public void testFindLimit() throws Exception {
        FindByQueryCall findByFinishTime = FindByQueryCall.newInstance(DataEntityCollection.APP,
                QueryUtils.is("finishTime", clusterTimestamp - DURATION_UNIT),
                ID_FIELD,
                false
        );
        List<AppProfile> apps = mainDB.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(2, apps.size());
        findByFinishTime.setLimitOrZero(1);
        apps = mainDB.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(1, apps.size());
        ApplicationId app2Id = ApplicationId.newInstance(clusterTimestamp, 2);
        assertEquals(app2Id.toString(), apps.get(0).getId());
    }

    @Test
    public void testFindOffset() throws Exception {
        FindByQueryCall findByFinishTime = FindByQueryCall.newInstance(
                DataEntityCollection.APP,
                QueryUtils.is("finishTime", clusterTimestamp - DURATION_UNIT),
                ID_FIELD,
                false
        );
        List<AppProfile> apps = mainDB.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(2, apps.size());
        findByFinishTime.setOffsetOrZero(1);
        apps = mainDB.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(1, apps.size());
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        assertEquals(app3Id.toString(), apps.get(0).getId());
    }

    @Test
    public void testFindOffsetAndLimit() throws Exception {
        FindByQueryCall findByFinishTime = FindByQueryCall.newInstance(
                DataEntityCollection.APP,
                QueryUtils.is("finishTime", clusterTimestamp - DURATION_UNIT),
                ID_FIELD,
                false
        );
        List<AppProfile> apps = mainDB.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(2, apps.size());
        findByFinishTime.setOffsetOrZero(-1);
        findByFinishTime.setLimitOrZero(2);
        apps = mainDB.executeDatabaseCall(findByFinishTime).getEntities();
        assertEquals(1, apps.size());
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        assertEquals(app3Id.toString(), apps.get(0).getId());
    }

    @Test
    public void testInStringsQuery() throws Exception {
        ApplicationId app2Id = ApplicationId.newInstance(clusterTimestamp, 2);
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);

        IdsByQueryCall findTwoAndThree = IdsByQueryCall.newInstance(
                DataEntityCollection.APP,
                QueryUtils.in(ID_FIELD, Arrays.<Object>asList(app2Id.toString(), app3Id.toString())),
                ID_FIELD,
                false
        );
        List<String> appIds = mainDB.executeDatabaseCall(findTwoAndThree).getEntries();
        assertEquals(2, appIds.size());
        assertEquals(app2Id.toString(), appIds.get(0));
        assertEquals(app3Id.toString(), appIds.get(1));
    }

    @Test
    public void testInNumbersQuery() throws Exception {
        ApplicationId app2Id = ApplicationId.newInstance(clusterTimestamp, 2);
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);

        IdsByQueryCall findTwoAndThree = IdsByQueryCall.newInstance(
                DataEntityCollection.APP,
                QueryUtils.in("finishTime", Collections.<Object>singletonList(clusterTimestamp - DURATION_UNIT)),
                ID_FIELD,
                false
        );
        List<String> appIds = mainDB.executeDatabaseCall(findTwoAndThree).getEntries();
        assertEquals(2, appIds.size());
        assertEquals(app2Id.toString(), appIds.get(0));
        assertEquals(app3Id.toString(), appIds.get(1));
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
            mainDB.executeDatabaseCall(storeApp);
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
        String upsertedId = (String) mainDB.executeDatabaseCall(updateApp).getValue();
        assertNull(upsertedId);
        FindByQueryCall findAppsByName = FindByQueryCall.newInstance(DataEntityCollection.APP,
                QueryUtils.is("name", modifiedName));
        List<AppProfile> returnedApps = mainDB.executeDatabaseCall(findAppsByName).getEntities();
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
        upsertedId = (String) mainDB.executeDatabaseCall(updateApp).getValue();
        assertEquals(app4.getId(), upsertedId);
        returnedApps = mainDB.executeDatabaseCall(findAppsByName).getEntities();
        assertEquals(2, returnedApps.size());
        assertTrue(returnedApps.get(0).getId().equals(app4IdString) ||
                returnedApps.get(1).getId().equals(app4IdString));
    }


    @Test
    public void testDeleteById() throws Exception {
        DeleteByIdCall deleteApp = DeleteByIdCall.newInstance(DataEntityCollection.APP,
                ApplicationId.newInstance(clusterTimestamp, 2).toString());
        mainDB.executeDatabaseCall(deleteApp);
        IdsByQueryCall listIds = IdsByQueryCall.newInstance(DataEntityCollection.APP,
                QueryUtils.is("user", SECOND_USER));
        List<String> returnedAppIds = mainDB.executeDatabaseCall(listIds).getEntries();
        String appId3 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        assertArrayEquals(new String[]{appId3}, returnedAppIds.toArray());

    }

    @Test
    public void testDeleteByParams() throws Exception {
        IdsByQueryCall listIds = IdsByQueryCall.newInstance(DataEntityCollection.JOB, null);
        List<String> returnedJobIds = mainDB.executeDatabaseCall(listIds).getEntries();
        assertEquals(3, returnedJobIds.size());
        String appId1 = ApplicationId.newInstance(clusterTimestamp, 1).toString();
        listIds.setQuery(QueryUtils.is("appId", appId1));
        returnedJobIds = mainDB.executeDatabaseCall(listIds).getEntries();
        assertEquals(1, returnedJobIds.size());
        DeleteByQueryCall deleteJob = DeleteByQueryCall.newInstance(DataEntityCollection.JOB,
                QueryUtils.is("appId", appId1));
        mainDB.executeDatabaseCall(deleteJob);
        listIds.setQuery(null);
        returnedJobIds = mainDB.executeDatabaseCall(listIds).getEntries();
        assertEquals(2, returnedJobIds.size());
        listIds.setQuery(QueryUtils.is("appId", appId1));
        returnedJobIds = mainDB.executeDatabaseCall(listIds).getEntries();
        assertEquals(0, returnedJobIds.size());
    }

    @Test
    public void testJobByAppId() throws Exception {
        String appId2 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        JobForAppCall getJob = JobForAppCall.newInstance(appId2, SECOND_USER);
        JobProfile job = mainDB.executeDatabaseCall(getJob).getEntity();
        assertEquals(JOB_NAME_ROOT + " 3", job.getName());
    }

    @Test
    public void testSaveFlexFields() throws Exception {
        String appId2 = ApplicationId.newInstance(clusterTimestamp, 2).toString();
        IdsByQueryCall listIds = IdsByQueryCall.newInstance(DataEntityCollection.JOB, QueryUtils.is("appId", appId2));
        List<String> returnedJobIds = mainDB.executeDatabaseCall(listIds).getEntries();
        assertEquals(1, returnedJobIds.size());
        String jobId = returnedJobIds.get(0);
        String key = "SOME_FLEX_KEY", value = "6";
        SaveJobFlexFieldsCall saveFlexFields = SaveJobFlexFieldsCall.newInstance(jobId,
                Collections.singletonMap(key, value), false);
        mainDB.executeDatabaseCall(saveFlexFields);
        FindByIdCall findJob = FindByIdCall.newInstance(DataEntityCollection.JOB, jobId);
        JobProfile job = mainDB.executeDatabaseCall(findJob).getEntity();
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
        mainDB.executeDatabaseCall(transaction);
        IdsByQueryCall listIdsForName = IdsByQueryCall.newInstance(DataEntityCollection.APP,
                QueryUtils.is("name", modifiedName));
        List<String> idsForName = mainDB.executeDatabaseCall(listIdsForName).getEntries();
        Collections.sort(idsForName);
        assertArrayEquals(new String[]{app3.getId(), app4.getId()}, idsForName.toArray());
        FindByIdCall findApp = FindByIdCall.newInstance(DataEntityCollection.APP, appId1);
        assertNull(mainDB.executeDatabaseCall(findApp).getEntity());
        transaction.setCallList(Collections.singletonList(
                DeleteByIdCall.newInstance(DataEntityCollection.APP, app4.getId())));
        mainDB.executeDatabaseCall(transaction);
        findApp.setId(app4.getId());
        assertNull(mainDB.executeDatabaseCall(findApp).getEntity());
    }

    @Test
    public void testListCollections() throws Exception {
        Map<DataEntityDB, List<DataEntityCollection>> collectionMap = dataBroker.listExistingCollections();
        System.out.println("Collections are: " + collectionMap);
        List<DataEntityCollection> collections = collectionMap.get(DataEntityDB.getMain());
        assertNotNull(collections);
        assertTrue(collections.contains(DataEntityCollection.JOB));
        assertTrue(collections.contains(DataEntityCollection.APP));

    }

    @Test
    public void testClear() throws Exception {
        dataBroker.clear();
        assertEquals(0, dataBroker.listExistingCollections().size());
        FindByQueryCall allEntities = FindByQueryCall.newInstance(DataEntityCollection.APP, null);
        assertEquals(0, mainDB.executeDatabaseCall(allEntities).getEntities().size());
        allEntities.setEntityCollection(DataEntityCollection.JOB);
        assertEquals(0, mainDB.executeDatabaseCall(allEntities).getEntities().size());
    }

    @Test
    public void testMove() throws Exception {
        int collectionNo = dataBroker.listExistingCollections().get(mainDB.getTarget()).size();
        IdsByQueryCall allIds = IdsByQueryCall.newInstance(DataEntityCollection.APP, null);
        int appNo = mainDB.executeDatabaseCall(allIds).getEntries().size();
        allIds.setEntityCollection(DataEntityCollection.JOB);
        int jobNo = mainDB.executeDatabaseCall(allIds).getEntries().size();
        DataEntityDB otherDB = DataEntityDB.get(DataEntityDB.Type.MAIN, "testCopy");
        dataBroker.copyDatabase(mainDB.getTarget(), otherDB);
        dataBroker.clearDatabase(mainDB.getTarget());
        Map<DataEntityDB, List<DataEntityCollection>> collectionMap = dataBroker.listExistingCollections();
        assertNull(collectionMap.get(mainDB.getTarget()));
        assertEquals(collectionNo, collectionMap.get(otherDB).size());
        assertEquals(jobNo, dataBroker.executeDatabaseCall(allIds, otherDB).getEntries().size());
        allIds.setEntityCollection(DataEntityCollection.APP);
        assertEquals(appNo, dataBroker.executeDatabaseCall(allIds, otherDB).getEntries().size());

    }
}
