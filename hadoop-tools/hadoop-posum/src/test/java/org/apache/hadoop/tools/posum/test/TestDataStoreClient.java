package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.database.client.DataMasterClient;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by ane on 3/21/16.
 */
public class TestDataStoreClient {

    private static final DataEntityDB db = DataEntityDB.getMain();

    @Test
    public void checkOneObject() {
        Configuration conf = TestUtils.getConf();
        DataMasterClient dataStore = new DataMasterClient(null);
        dataStore.init(conf);
        dataStore.start();
        DataStore myStore = new DataStore(conf);

        String appId = "testApp";
        myStore.delete(db, DataEntityType.APP, appId);
        AppProfile app = Records.newRecord(AppProfile.class);
        app.setId(appId);
        app.setStartTime(System.currentTimeMillis());
        app.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(app);
        myStore.updateOrStore(db, DataEntityType.APP, app);

        AppProfile otherApp = dataStore.findById(db, DataEntityType.APP, appId);
        otherApp.setName("Official Test App");
        myStore.updateOrStore(db, DataEntityType.APP, otherApp);
        System.out.println(otherApp);
        assertTrue(app.getId().equals(otherApp.getId()));

        String jobId = "testApp_job1";
        myStore.delete(db, DataEntityType.JOB, jobId);
        JobProfile job = Records.newRecord(JobProfile.class);
        job.setId(jobId);
        job.setAppId(appId);
        job.setStartTime(System.currentTimeMillis());
        job.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(job);
        myStore.updateOrStore(db, DataEntityType.JOB, job);

        JobProfile otherJob = dataStore.findById(db, DataEntityType.JOB, jobId);
        otherJob.setName("Official Test Job");
        myStore.updateOrStore(db, DataEntityType.JOB, otherJob);
        System.out.println(otherJob);
        assertTrue(job.getId().equals(otherJob.getId()));


        String taskId = "testApp_job1_task1";
        myStore.delete(db, DataEntityType.TASK, taskId);
        TaskProfile task = Records.newRecord(TaskProfile.class);
        task.setId(taskId);
        task.setAppId(appId);
        task.setJobId(jobId);
        task.setStartTime(System.currentTimeMillis());
        task.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(task);
        myStore.updateOrStore(db, DataEntityType.TASK, task);

        TaskProfile otherTask = dataStore.findById(db, DataEntityType.TASK, taskId);
        otherTask.setType("REDUCE");
        myStore.updateOrStore(db, DataEntityType.TASK, otherTask);
        System.out.println(otherTask);
        assertTrue(task.getId().equals(otherTask.getId()));

        myStore.delete(db, DataEntityType.TASK, taskId);
        myStore.delete(db, DataEntityType.JOB, jobId);
        myStore.delete(db, DataEntityType.APP, appId);
    }

    @Test
    public void checkMuliObject() {
        Configuration conf = TestUtils.getConf();
        DataMasterClient dataStore = new DataMasterClient(null);
        dataStore.init(conf);
        dataStore.start();
        DataStore myStore = new DataStore(conf);

        String appId = "testHistoryApp";
        myStore.delete(db, DataEntityType.HISTORY, "originalId", appId);
        AppProfile app = Records.newRecord(AppProfile.class);
        app.setId(appId);
        app.setStartTime(System.currentTimeMillis());
        app.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(app);
        HistoryProfile appHistory = new HistoryProfilePBImpl(DataEntityType.APP, app);
        String historyId = myStore.store(db, DataEntityType.HISTORY, appHistory);

        Map<String, Object> properties = new HashMap<>();
        properties.put("originalId", appId);
        List<HistoryProfile> profilesById = dataStore.find(db, DataEntityType.HISTORY, properties);
        System.out.println(profilesById);
        assertTrue(profilesById.size() == 1);
        HistoryProfile otherHistory = profilesById.get(0);
        assertEquals(appId, otherHistory.getOriginalId());
        assertEquals(appHistory.getTimestamp(), otherHistory.getTimestamp());

        myStore.delete(db, DataEntityType.HISTORY, historyId);
    }
}
