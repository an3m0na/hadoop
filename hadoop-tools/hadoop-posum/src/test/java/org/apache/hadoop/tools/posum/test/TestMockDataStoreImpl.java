package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ane on 7/26/16.
 */
public class TestMockDataStoreImpl {
    private MockDataStore dataStore;
    private DBInterface db;
    private static final Long DURATION_UNIT = 60000L; // 1 minute
    private static final String JOB_NAME_ROOT = "Dummy Job";
    private static final String FIRST_USER = "dummy";
    private static final String SECOND_USER = "geek";
    private final Long clusterTimestamp = System.currentTimeMillis();

    @Before
    public void setUp() throws Exception {
        dataStore = new MockDataStoreImpl();
        db = dataStore.bindTo(DataEntityDB.getMain());

        AppProfile app1 = Records.newRecord(AppProfile.class);
        ApplicationId app1Id = ApplicationId.newInstance(clusterTimestamp, 1);
        app1.setId(app1Id.toString());
        app1.setName(JOB_NAME_ROOT + " 1");
        app1.setUser(FIRST_USER);
        app1.setStartTime(clusterTimestamp - 5 * DURATION_UNIT);
        app1.setFinishTime(clusterTimestamp);
        db.store(DataEntityType.APP, app1);

        JobProfile job1 = Records.newRecord(JobProfile.class);
        JobId job1Id = new JobIdPBImpl();
        job1Id.setAppId(app1Id);
        job1Id.setId(1);
        job1.setId(job1Id.toString());
        job1.setAppId(app1.getId());
        job1.setName(app1.getName());
        job1.setUser(app1.getUser());
        job1.setTotalMapTasks(5);
        job1.setTotalReduceTasks(1);
        job1.setStartTime(app1.getStartTime());
        job1.setFinishTime(app1.getFinishTime());
        db.store(DataEntityType.JOB, job1);


        AppProfile app2 = Records.newRecord(AppProfile.class);
        ApplicationId app2Id = ApplicationId.newInstance(clusterTimestamp, 2);
        app2.setId(app2Id.toString());
        app2.setName(JOB_NAME_ROOT + " 2");
        app2.setUser(SECOND_USER);
        app2.setStartTime(clusterTimestamp - 4 * DURATION_UNIT);
        app2.setFinishTime(clusterTimestamp - DURATION_UNIT);
        db.store(DataEntityType.APP, app2);

        JobProfile job2 = Records.newRecord(JobProfile.class);
        JobId job2Id = new JobIdPBImpl();
        job2Id.setAppId(app2Id);
        job2Id.setId(2);
        job2.setId(job2Id.toString());
        job2.setAppId(app2.getId());
        job2.setName(app2.getName());
        job2.setUser(app2.getUser());
        job2.setTotalMapTasks(10);
        job2.setTotalReduceTasks(3);
        job2.setStartTime(app2.getStartTime());
        job2.setFinishTime(app2.getFinishTime());
        db.store(DataEntityType.JOB, job2);

        AppProfile app3 = Records.newRecord(AppProfile.class);
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        app3.setId(app3Id.toString());
        app3.setName(JOB_NAME_ROOT + " 3");
        app3.setUser(SECOND_USER);
        app3.setStartTime(clusterTimestamp - 2 * DURATION_UNIT);
        app3.setFinishTime(clusterTimestamp - DURATION_UNIT);
        db.store(DataEntityType.APP, app3);

        JobProfile job3 = Records.newRecord(JobProfile.class);
        JobId job3Id = new JobIdPBImpl();
        job3Id.setAppId(app3Id);
        job3Id.setId(3);
        job3.setId(job3Id.toString());
        job3.setAppId(app3.getId());
        job3.setName(app3.getName());
        job3.setUser(app3.getUser());
        job3.setTotalMapTasks(1);
        job3.setTotalReduceTasks(1);
        job3.setStartTime(app3.getStartTime());
        job3.setFinishTime(app3.getFinishTime());
        db.store(DataEntityType.JOB, job3);

    }

    @Test
    public void testFindById() throws Exception {
        String appId = ApplicationId.newInstance(clusterTimestamp, 1).toString();
        AppProfile app = db.findById(DataEntityType.APP, appId);
        assertEquals(appId, app.getId());
        assertEquals(JOB_NAME_ROOT + " 1", app.getName());
        assertEquals(FIRST_USER, app.getUser());
        assertEquals(Long.valueOf(clusterTimestamp - 5 * DURATION_UNIT), app.getStartTime());
        assertEquals(clusterTimestamp, app.getFinishTime());
    }

    @Test
    public void testListIds() throws Exception {
        List<String> returnedAppIds = db.listIds(DataEntityType.APP, Collections.singletonMap("user", (Object) SECOND_USER));
        String appId2 = ApplicationId.newInstance(clusterTimestamp, 2).toString();
        String appId3 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        assertArrayEquals(new String[]{appId2, appId3}, returnedAppIds.toArray());
    }

    @Test
    public void testFindAll() throws Exception {
        List<JobProfile> jobs = db.find(DataEntityType.JOB, Collections.<String, Object>emptyMap());
        assertEquals(3, jobs.size());
    }

    @Test
    public void testFindSelected() throws Exception {
        Map<String, Object> properties = new HashMap<>(2);
        properties.put("finishTime", clusterTimestamp - DURATION_UNIT);
        properties.put("totalMapTasks", 10);
        List<JobProfile> jobs = db.find(DataEntityType.JOB, properties);
        assertEquals(1, jobs.size());
        JobId job2Id = new JobIdPBImpl();
        job2Id.setAppId(ApplicationId.newInstance(clusterTimestamp, 2));
        job2Id.setId(2);
        assertEquals(job2Id.toString(), jobs.get(0).getId());
    }

    @Test
    public void testFindLimit() throws Exception {
        List<AppProfile> apps = db.find(DataEntityType.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)));
        assertEquals(2, apps.size());
        apps = db.find(DataEntityType.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)), 0, 1);
        assertEquals(1, apps.size());
        ApplicationId app2Id = ApplicationId.newInstance(clusterTimestamp, 2);
        assertEquals(app2Id.toString(), apps.get(0).getId());
    }

    @Test
    public void testFindOffset() throws Exception {
        List<AppProfile> apps = db.find(DataEntityType.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)));
        assertEquals(2, apps.size());
        apps = db.find(DataEntityType.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)), 1, 0);
        assertEquals(1, apps.size());
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        assertEquals(app3Id.toString(), apps.get(0).getId());
    }

    @Test
    public void testFindOffsetAndLimit() throws Exception {
        List<AppProfile> apps = db.find(DataEntityType.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)));
        assertEquals(2, apps.size());
        apps = db.find(DataEntityType.APP, Collections.singletonMap("finishTime",
                (Object) (clusterTimestamp - DURATION_UNIT)), -1, 2);
        assertEquals(1, apps.size());
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        assertEquals(app3Id.toString(), apps.get(0).getId());
    }

    @Test(expected = POSUMException.class)
    public void testStoreFailsForDuplicate() throws Exception {
        AppProfile app3 = Records.newRecord(AppProfile.class);
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        app3.setId(app3Id.toString());
        app3.setName("Modified Name");
        app3.setQueue("Now it has a queue");
        db.store(DataEntityType.APP, app3);
    }

    @Test
    public void testUpdateOrStore() throws Exception {
        AppProfile app3 = Records.newRecord(AppProfile.class);
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        app3.setId(app3Id.toString());
        String modifiedName = "Modified Name", queueName = "NotNullQueue";
        app3.setName(modifiedName);
        app3.setQueue(queueName);
        assertTrue(db.updateOrStore(DataEntityType.APP, app3));
        List<AppProfile> returnedApps = db.find(DataEntityType.APP,
                Collections.singletonMap("id", (Object) app3.getId()));
        assertEquals(1, returnedApps.size());
        AppProfile returned = returnedApps.get(0);
        assertEquals(modifiedName, returned.getName());
        assertEquals(queueName, returned.getQueue());
        assertEquals("", returned.getUser());
        assertEquals(new Long(0), returned.getStartTime());
        assertEquals(new Long(0), returned.getFinishTime());

    }


    @Test
    public void testDeleteById() throws Exception {
        db.delete(DataEntityType.APP, ApplicationId.newInstance(clusterTimestamp, 2).toString());
        List<String> returnedAppIds = db.listIds(DataEntityType.APP, Collections.singletonMap("user", (Object) SECOND_USER));
        String appId3 = ApplicationId.newInstance(clusterTimestamp, 3).toString();
        assertArrayEquals(new String[]{appId3}, returnedAppIds.toArray());

    }

    @Test
    public void testDeleteByParams() throws Exception {
        List<String> returnedJobIds = db.listIds(DataEntityType.JOB, Collections.<String, Object>emptyMap());
        assertEquals(3, returnedJobIds.size());
        String appId1 = ApplicationId.newInstance(clusterTimestamp, 1).toString();
        returnedJobIds = db.listIds(DataEntityType.JOB, Collections.singletonMap("appId", (Object) appId1));
        assertEquals(1, returnedJobIds.size());
        db.delete(DataEntityType.JOB, Collections.singletonMap("appId", (Object) appId1));
        returnedJobIds = db.listIds(DataEntityType.JOB, Collections.<String, Object>emptyMap());
        assertEquals(2, returnedJobIds.size());
        returnedJobIds = db.listIds(DataEntityType.JOB, Collections.singletonMap("appId", (Object) appId1));
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
        List<String> returnedJobIds = db.listIds(DataEntityType.JOB, Collections.singletonMap("appId", (Object) appId2));
        assertEquals(1, returnedJobIds.size());
        String jobId = returnedJobIds.get(0);
        String key = "SOME_FLEX_KEY", value = "6";
        db.saveFlexFields(jobId, Collections.singletonMap(key, value), false);
        JobProfile job = db.findById(DataEntityType.JOB, jobId);
        assertEquals(1, job.getFlexFields().size());
        assertEquals(value, job.getFlexField(key));
    }

    @Test
    public void testDataImport() throws Exception {
        dataStore.importData("/Users/ane/Desktop/importtest");
        List<String> ids = db.listIds(DataEntityType.JOB_HISTORY, Collections.<String, Object>emptyMap());
        assertEquals(2, ids.size());
        assertArrayEquals(new String[]{"job_1468525450527_0001", "job_1468525450527_0003"}, ids.toArray());
    }
}
