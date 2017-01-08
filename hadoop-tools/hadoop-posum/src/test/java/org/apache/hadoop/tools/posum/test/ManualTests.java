package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.data.core.DataStoreImpl;
import org.apache.hadoop.tools.posum.data.master.DataMasterContext;
import org.apache.hadoop.tools.posum.data.monitor.cluster.ClusterMonitor;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ManualTests {
    @Test
    @Ignore
    public void checkRegistration() {
        System.out.println(NetUtils.createSocketAddr("0.0.0.0", 7000));
    }

    @Test
    @Ignore
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

    @Test
    @Ignore
    public void checkDatabaseFeeding() {
        Configuration conf = PosumConfiguration.newInstance();
        DataStoreImpl dataStore = new DataStoreImpl(conf);
        DataMasterContext context = new DataMasterContext();
        context.setDataStore(dataStore);
        ClusterMonitor monitor = new ClusterMonitor(context);
        monitor.start();
        try {
            Thread.sleep(100000);
            monitor.stop();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test(){
        System.out.println(System.currentTimeMillis());
    }
}
