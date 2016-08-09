package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.database.master.DataMasterContext;
import org.apache.hadoop.tools.posum.database.monitor.HadoopAPIClient;
import org.apache.hadoop.tools.posum.database.monitor.HadoopMonitor;
import org.apache.hadoop.tools.posum.database.store.DataStoreImpl;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 4/20/16.
 */
public class CustomTests {
//    @Test
    public void checkRegistration() {
        System.out.println(NetUtils.createSocketAddr("0.0.0.0", 7000));
    }

//    @Test
    public void testMongoRunner() throws IOException, InterruptedException {
        String scriptLocation = getClass().getClassLoader().getResource("run-mongo.sh").getFile();
        Process process = Runtime.getRuntime().exec("/bin/bash " + scriptLocation);
        process.waitFor();
        if (process.exitValue() != 0) {
            System.out.println("Error running Mongo database:");
            String s;
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((s = reader.readLine()) != null)
                System.out.println(s);
            reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((s = reader.readLine()) != null)
                System.out.println(s);
            throw new RuntimeException("Could not run MongoDB");
        }
    }

//    @Test
    public void testMongoStopper() throws IOException, InterruptedException {
        String scriptLocation = getClass().getClassLoader().getResource("run-mongo.sh").getFile();
        Process process = Runtime.getRuntime().exec("/bin/bash " + scriptLocation + " --stop");
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

//    @Test
    public void testHadoopAPIClient() {
        HadoopAPIClient apiClient = new HadoopAPIClient(PosumConfiguration.newInstance());
        List<AppProfile> apps = apiClient.getAppsInfo();
        System.out.println(apps);
        for (AppProfile app : apps) {
            if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
                System.out.println("For job: " + app.getId());
                Map<String, String> requested = new HashMap<>(2);
                requested.put("mapreduce.input.fileinputformat.inputdir", "inputdir");
                requested.put("mapreduce.input.fileinputformat.numinputfiles", "numinputfiles");
                Map<String, String> properties = apiClient.getJobConfProperties(app.getId(), app.getId(), requested);
                System.out.println("Input dir is: " + properties.get("inputdir"));
                System.out.println("Input files are: " + properties.get("numinputfiles"));
            }
        }
    }

//    @Test
    public void checkDatabaseFeeding() {
        Configuration conf = PosumConfiguration.newInstance();
        DataStoreImpl dataStore = new DataStoreImpl(conf);
        DataMasterContext context = new DataMasterContext();
        context.setDataStore(dataStore);
        HadoopMonitor monitor = new HadoopMonitor(context);
        monitor.start();
        try {
            Thread.sleep(100000);
            monitor.stop();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
