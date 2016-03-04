package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.database.DataStore;
import org.apache.hadoop.tools.posum.database.DataStoreImpl;
import org.apache.hadoop.tools.posum.monitor.SystemMonitor;
import org.junit.Test;

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
}
