package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.AppProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.database.client.DataStoreClient;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.database.store.DataStoreImpl;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by ane on 3/21/16.
 */
public class TestDataStoreClient {

    @Test
    public void checkOneObject() {
        Configuration conf = TestUtils.getConf();
        DataStoreClient dataStore = new DataStoreClient();
        dataStore.init(conf);
        dataStore.start();
        DataStore myStore = new DataStoreImpl(conf);

        String appId = "testApp";
        myStore.delete(DataEntityType.APP, appId);
        AppProfile app = Records.newRecord(AppProfile.class);
        app.setId(appId);
        app.setStartTime(System.currentTimeMillis());
        app.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(app);
        myStore.updateOrStore(DataEntityType.APP, app);

        AppProfile otherApp = dataStore.findById(DataEntityType.APP, appId);
        otherApp.setName("Official Test App");
        myStore.updateOrStore(DataEntityType.APP, otherApp);
        System.out.println(otherApp);
        assertTrue(app.getId().equals(otherApp.getId()));

        String jobId = "testApp_job1";
        myStore.delete(DataEntityType.JOB, jobId);
        JobProfile job = Records.newRecord(JobProfile.class);
        job.setId(jobId);
        job.setAppId(appId);
        job.setStartTime(System.currentTimeMillis());
        job.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(job);
        myStore.updateOrStore(DataEntityType.JOB, job);

        JobProfile otherJob = dataStore.findById(DataEntityType.JOB, jobId);
        otherJob.setName("Official Test Job");
        myStore.updateOrStore(DataEntityType.JOB, otherJob);
        System.out.println(otherJob);
        assertTrue(job.getId().equals(otherJob.getId()));


        String taskId = "testApp_job1_task1";
        myStore.delete(DataEntityType.TASK, taskId);
        TaskProfile task = Records.newRecord(TaskProfile.class);
        task.setId(taskId);
        task.setAppId(appId);
        task.setJobId(jobId);
        task.setStartTime(System.currentTimeMillis());
        task.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(task);
        myStore.updateOrStore(DataEntityType.TASK, task);

        TaskProfile otherTask = dataStore.findById(DataEntityType.TASK, taskId);
        otherTask.setType("REDUCE");
        myStore.updateOrStore(DataEntityType.TASK, otherTask);
        System.out.println(otherTask);
        assertTrue(task.getId().equals(otherTask.getId()));

        myStore.delete(DataEntityType.TASK, taskId);
        myStore.delete(DataEntityType.JOB, jobId);
        myStore.delete(DataEntityType.APP, appId);
    }

    @Test
    public void checkMuliObject() {
        Configuration conf = TestUtils.getConf();
        DataStoreClient dataStore = new DataStoreClient();
        dataStore.init(conf);
        dataStore.start();
        DataStore myStore = new DataStoreImpl(conf);

        String appId = "testHistoryApp";
        myStore.delete(DataEntityType.HISTORY, "originalId", appId);
        AppProfile app = Records.newRecord(AppProfile.class);
        app.setId(appId);
        app.setStartTime(System.currentTimeMillis());
        app.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(app);
        HistoryProfile appHistory = new HistoryProfilePBImpl(DataEntityType.APP, (AppProfilePBImpl)app);
        String historyId = myStore.store(DataEntityType.HISTORY, appHistory);

        Map<String, Object> properties = new HashMap<>();
        properties.put("originalId", appId);
        List<HistoryProfile> profilesById = dataStore.find(DataEntityType.HISTORY, properties);
        System.out.println(profilesById);
        assertTrue(profilesById.size() == 1);
        HistoryProfile otherHistory = profilesById.get(0);
        assertEquals(appId, otherHistory.getOriginalId());
        assertEquals(appHistory.getTimestamp(), otherHistory.getTimestamp());

        myStore.delete(DataEntityType.HISTORY, historyId);
    }
}
