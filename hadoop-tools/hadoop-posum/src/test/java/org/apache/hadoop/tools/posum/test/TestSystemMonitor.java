package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.database.master.DataMasterContext;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.database.monitor.HadoopMonitor;
import org.junit.Test;

import javax.xml.crypto.Data;

/**
 * Created by ane on 3/3/16.
 */
public class TestSystemMonitor {

    @Test
    public void checkDatabaseFeeding() {
        Configuration conf = TestUtils.getConf();
        DataStore dataStore = new DataStore(conf);
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
