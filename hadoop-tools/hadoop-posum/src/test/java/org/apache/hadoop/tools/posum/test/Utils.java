package org.apache.hadoop.tools.posum.test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.data.mock.data.HistorySnapshotStoreImpl;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class Utils {
    public static final Long DURATION_UNIT = 60000L; // 1 minute
    public static final String JOB_NAME_ROOT = "Dummy Job";
    public static final String QUEUE = "default";
    public static final String USER1 = "dummy";
    public static final String USER2 = "geek";
    public static final String TEST_TMP_DIR = "testTmpDir";
    public static final String WORKLOAD_DIR = "test_workload";
    public static final String API_RESPONSES_DIR = "test_api_responses";

    public static final Long CLUSTER_TIMESTAMP = 1483890116284L;
    public static final ApplicationId APP1_ID = ApplicationId.newInstance(CLUSTER_TIMESTAMP, 1);
    public static final ApplicationId APP2_ID = ApplicationId.newInstance(CLUSTER_TIMESTAMP, 2);
    public static final ApplicationId APP3_ID = ApplicationId.newInstance(CLUSTER_TIMESTAMP, 3);
    public static final JobId JOB1_ID = new JobIdPBImpl();
    public static final JobId JOB2_ID = new JobIdPBImpl();
    public static final JobId JOB3_ID = new JobIdPBImpl();
    public static final AppProfile APP1 = Records.newRecord(AppProfile.class);
    public static final AppProfile APP2 = Records.newRecord(AppProfile.class);
    public static final AppProfile APP3 = Records.newRecord(AppProfile.class);
    public static final JobProfile JOB1 = Records.newRecord(JobProfile.class);
    public static final JobProfile JOB2 = Records.newRecord(JobProfile.class);
    public static final JobProfile JOB3 = Records.newRecord(JobProfile.class);

    public static final TaskProfile TASK11 = Records.newRecord(TaskProfile.class);
    public static final TaskProfile TASK12 = Records.newRecord(TaskProfile.class);
    public static final TaskId TASK11_ID = new TaskIdPBImpl();
    public static final TaskId TASK12_ID = new TaskIdPBImpl();

    public static final String RACK1 = "firstRack";
    public static final String RACK2 = "secondRack";
    public static final String NODE1 = "firstNode";
    public static final String NODE2 = "secondNode";

    static {
        APP1.setId(APP1_ID.toString());
        APP1.setName(JOB_NAME_ROOT + " 1");
        APP1.setUser(USER1);
        APP1.setQueue(QUEUE);
        APP1.setStartTime(CLUSTER_TIMESTAMP);
        APP1.setFinishTime(CLUSTER_TIMESTAMP + 5 * DURATION_UNIT);

        JOB1_ID.setAppId(APP1_ID);
        JOB1_ID.setId(1);
        JOB1.setId(JOB1_ID.toString());
        JOB1.setAppId(APP1.getId());
        JOB1.setName(APP1.getName());
        JOB1.setUser(APP1.getUser());
        JOB1.setQueue(QUEUE);
        JOB1.setTotalMapTasks(1);
        JOB1.setTotalReduceTasks(1);
        JOB1.setStartTime(APP1.getStartTime());
        JOB1.setFinishTime(APP1.getFinishTime());

        TASK11_ID.setJobId(JOB1_ID);
        TASK11_ID.setId(1);
        TASK11.setId(TASK11_ID.toString());
        TASK11.setStartTime(JOB1.getStartTime());
        TASK11.setFinishTime(TASK11.getStartTime() + DURATION_UNIT * 3);
        TASK11.setHttpAddress(NODE1);

        TASK12_ID.setJobId(JOB1_ID);
        TASK12_ID.setId(2);
        TASK12.setId(TASK12_ID.toString());
        TASK12.setStartTime(TASK11.getFinishTime());
        TASK12.setFinishTime(JOB1.getFinishTime());
        TASK12.setHttpAddress(NODE1);

        APP2.setId(APP2_ID.toString());
        APP2.setName(JOB_NAME_ROOT + " 2");
        APP2.setUser(USER2);
        APP2.setQueue(QUEUE);
        APP2.setStartTime(CLUSTER_TIMESTAMP + DURATION_UNIT);
        APP2.setFinishTime(CLUSTER_TIMESTAMP + 4 * DURATION_UNIT);

        JOB2_ID.setAppId(APP2_ID);
        JOB2_ID.setId(2);
        JOB2.setId(JOB2_ID.toString());
        JOB2.setAppId(APP2.getId());
        JOB2.setName(APP2.getName());
        JOB2.setUser(APP2.getUser());
        JOB2.setQueue(QUEUE);
        JOB2.setTotalMapTasks(2);
        JOB2.setTotalReduceTasks(0);
        JOB2.setStartTime(APP2.getStartTime());
        JOB2.setFinishTime(APP2.getFinishTime());

        APP3.setId(APP3_ID.toString());
        APP3.setName(JOB_NAME_ROOT + " 3");
        APP3.setUser(USER2);
        APP3.setQueue(QUEUE);
        APP3.setStartTime(CLUSTER_TIMESTAMP + 2 * DURATION_UNIT);
        APP3.setFinishTime(CLUSTER_TIMESTAMP + 4 * DURATION_UNIT);

        JOB3_ID.setAppId(APP3_ID);
        JOB3_ID.setId(3);
        JOB3.setId(JOB3_ID.toString());
        JOB3.setAppId(APP3.getId());
        JOB3.setName(APP3.getName());
        JOB3.setUser(APP3.getUser());
        JOB3.setQueue(QUEUE);
        JOB3.setTotalMapTasks(3);
        JOB3.setTotalReduceTasks(2);
        JOB3.setStartTime(APP3.getStartTime());
        JOB3.setFinishTime(APP3.getFinishTime());
    }

    public static void loadThreeDefaultAppsAndJobs(Database db) {
        StoreCall storeCall = StoreCall.newInstance(DataEntityCollection.APP, APP1);
        db.executeDatabaseCall(storeCall);

        storeCall.setEntityCollection(DataEntityCollection.JOB);
        storeCall.setEntity(JOB1);
        db.executeDatabaseCall(storeCall);

        storeCall.setEntityCollection(DataEntityCollection.APP);
        storeCall.setEntity(APP2);
        db.executeDatabaseCall(storeCall);

        storeCall.setEntityCollection(DataEntityCollection.JOB);
        storeCall.setEntity(JOB2);
        db.executeDatabaseCall(storeCall);

        storeCall.setEntityCollection(DataEntityCollection.APP);
        storeCall.setEntity(APP3);
        db.executeDatabaseCall(storeCall);

        storeCall.setEntityCollection(DataEntityCollection.JOB);
        storeCall.setEntity(JOB3);
        db.executeDatabaseCall(storeCall);
    }

    private static String getMongoScriptCall() {
        String scriptLocation = Utils.class.getClassLoader().getResource("run-mongo.sh").getFile();
        return "/bin/bash " + scriptLocation;
    }

    public static void runMongoDB() throws IOException, InterruptedException {
        runProcess(getMongoScriptCall());
    }

    public static void stopMongoDB() throws IOException, InterruptedException {
        runProcess(getMongoScriptCall() + " --stop");
    }

    public static void runProcess(String command) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(command);
        process.waitFor();
        if (process.exitValue() != 0) {
            System.out.println("Error stopping Mongo database:");
            String s;
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((s = reader.readLine()) != null)
                System.out.println(s);
            reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((s = reader.readLine()) != null)
                System.out.println(s);
            throw new RuntimeException("Could not stop MongoDB");
        }
    }

    public static HistorySnapshotStoreImpl mockDefaultWorkload() {
        URL workloadUrl = Utils.class.getClassLoader().getResource(WORKLOAD_DIR);
        if (workloadUrl == null)
            throw new RuntimeException("Default test workload folder was not found");
        return new HistorySnapshotStoreImpl(workloadUrl.getPath());
    }

    public static String getApiJson(String resource) throws Exception {
        URL apiUrl = Utils.class.getClassLoader().getResource(API_RESPONSES_DIR + File.separator + resource);
        if (apiUrl == null)
            throw new RuntimeException("Default test api folder was not found");
        return FileUtils.readFileToString(new File(apiUrl.getPath()));
    }
}
