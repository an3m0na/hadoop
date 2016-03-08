package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.database.DataStore;
import org.apache.hadoop.tools.posum.database.DataStoreImpl;
import org.apache.hadoop.tools.posum.monitor.SystemInfoCollector;
import org.apache.hadoop.tools.posum.monitor.SystemMonitor;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by ane on 3/3/16.
 */
public class TestSystemMonitor {

    @Test
    public void checkDatabaseFeeding() {
        Configuration conf = TestUtils.getConf();
        DataStore dataStore = new DataStoreImpl(conf);
        SystemMonitor monitor = new SystemMonitor(dataStore);
        monitor.start();
        try {
            Thread.sleep(100000);
            monitor.exit();
            monitor.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void checkSubmittedJobInfo() throws IOException {
        Configuration conf = TestUtils.getConf();
        SystemInfoCollector collector = new SystemInfoCollector(conf);
        System.out.println(collector.getSubmittedJobInfo("application_1457441040516_0003"));
    }
}
