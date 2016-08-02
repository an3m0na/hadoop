package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.database.client.DataClientInterface;
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
    protected DataClientInterface dataStore;
    protected DBInterface db;
    protected final Long clusterTimestamp = System.currentTimeMillis();

    @Before
    public void setUp() throws Exception {
        setUpDataStore();
        db = dataStore.bindTo(DataEntityDB.getMain());
        Utils.loadThreeDefaultAppsAndJobs(clusterTimestamp, db);
    }

    protected abstract void setUpDataStore() throws Exception;

    @Test
    public void testFindById() throws Exception {
        String appId = ApplicationId.newInstance(clusterTimestamp, 1).toString();
        AppProfile app = db.findById(DataEntityCollection.APP, appId);
        assertEquals(appId, app.getId());
        assertEquals(JOB_NAME_ROOT + " 1", app.getName());
        assertEquals(FIRST_USER, app.getUser());
        assertEquals(Long.valueOf(clusterTimestamp - 5 * DURATION_UNIT), app.getStartTime());
        assertEquals(clusterTimestamp, app.getFinishTime());
    }

    @Test
    public void testListIds() throws Exception {
        List<String> returnedAppIds = db.listIds(DataEntityCollection.APP, Collections.singletonMap("user", (Object) SECOND_USER));
        String appId2 = ApplicationId.newInstance(clusterTimestamp, 2).toString();
        String appId3 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        Collections.sort(returnedAppIds);
        assertArrayEquals(new String[]{appId2, appId3}, returnedAppIds.toArray());
    }

    @Test
    public void testFindAll() throws Exception {
        List<JobProfile> jobs = db.find(DataEntityCollection.JOB, Collections.<String, Object>emptyMap());
        assertEquals(3, jobs.size());
    }

    @Test
    public void testFindSelected() throws Exception {
        Map<String, Object> properties = new HashMap<>(2);
        properties.put("finishTime", clusterTimestamp - DURATION_UNIT);
        properties.put("totalMapTasks", 10);
        List<JobProfile> jobs = db.find(DataEntityCollection.JOB, properties);
        assertEquals(1, jobs.size());
        JobId job2Id = new JobIdPBImpl();
        job2Id.setAppId(ApplicationId.newInstance(clusterTimestamp, 2));
        job2Id.setId(2);
        assertEquals(job2Id.toString(), jobs.get(0).getId());
    }

    @Test
    public void testFindLimit() throws Exception {
        List<AppProfile> apps = db.find(DataEntityCollection.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)));
        assertEquals(2, apps.size());
        apps = db.find(DataEntityCollection.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)), 0, 1);
        assertEquals(1, apps.size());
        ApplicationId app2Id = ApplicationId.newInstance(clusterTimestamp, 2);
        assertEquals(app2Id.toString(), apps.get(0).getId());
    }

    @Test
    public void testFindOffset() throws Exception {
        List<AppProfile> apps = db.find(DataEntityCollection.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)));
        assertEquals(2, apps.size());
        apps = db.find(DataEntityCollection.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)), 1, 0);
        assertEquals(1, apps.size());
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        assertEquals(app3Id.toString(), apps.get(0).getId());
    }

    @Test
    public void testFindOffsetAndLimit() throws Exception {
        List<AppProfile> apps = db.find(DataEntityCollection.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)));
        assertEquals(2, apps.size());
        apps = db.find(DataEntityCollection.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)), -1, 2);
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
            db.store(DataEntityCollection.APP, app3);
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
        db.updateOrStore(DataEntityCollection.APP, app3);
        List<AppProfile> returnedApps = db.find(DataEntityCollection.APP,
                Collections.singletonMap("name", (Object) modifiedName));
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
        db.updateOrStore(DataEntityCollection.APP, app4);
        returnedApps = db.find(DataEntityCollection.APP,
                Collections.singletonMap("name", (Object) modifiedName));
        assertEquals(2, returnedApps.size());
        assertTrue(returnedApps.get(0).getId().equals(app4IdString) || returnedApps.get(1).getId().equals(app4IdString));
    }


    @Test
    public void testDeleteById() throws Exception {
        db.delete(DataEntityCollection.APP, ApplicationId.newInstance(clusterTimestamp, 2).toString());
        List<String> returnedAppIds = db.listIds(DataEntityCollection.APP, Collections.singletonMap("user", (Object) SECOND_USER));
        String appId3 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        assertArrayEquals(new String[]{appId3}, returnedAppIds.toArray());

    }

    @Test
    public void testDeleteByParams() throws Exception {
        List<String> returnedJobIds = db.listIds(DataEntityCollection.JOB, Collections.<String, Object>emptyMap());
        assertEquals(3, returnedJobIds.size());
        String appId1 = ApplicationId.newInstance(clusterTimestamp, 1).toString();
        returnedJobIds = db.listIds(DataEntityCollection.JOB, Collections.singletonMap("appId", (Object) appId1));
        assertEquals(1, returnedJobIds.size());
        db.delete(DataEntityCollection.JOB, Collections.singletonMap("appId", (Object) appId1));
        returnedJobIds = db.listIds(DataEntityCollection.JOB, Collections.<String, Object>emptyMap());
        assertEquals(2, returnedJobIds.size());
        returnedJobIds = db.listIds(DataEntityCollection.JOB, Collections.singletonMap("appId", (Object) appId1));
        assertEquals(0, returnedJobIds.size());
    }

    @Test
    public void testJobByAppId() throws Exception {
        String appId2 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        JobProfile job = db.getJobProfileForApp(appId2, SECOND_USER);
        assertEquals(JOB_NAME_ROOT + " 3", job.getName());
    }

    @Test
    public void testSaveFlexFields() throws Exception {
        String appId2 = ApplicationId.newInstance(clusterTimestamp, 2).toString();
        List<String> returnedJobIds = db.listIds(DataEntityCollection.JOB, Collections.singletonMap("appId", (Object) appId2));
        assertEquals(1, returnedJobIds.size());
        String jobId = returnedJobIds.get(0);
        String key = "SOME_FLEX_KEY", value = "6";
        db.saveFlexFields(jobId, Collections.singletonMap(key, value), false);
        JobProfile job = db.findById(DataEntityCollection.JOB, jobId);
        assertEquals(1, job.getFlexFields().size());
        assertEquals(value, job.getFlexField(key));
    }
}
