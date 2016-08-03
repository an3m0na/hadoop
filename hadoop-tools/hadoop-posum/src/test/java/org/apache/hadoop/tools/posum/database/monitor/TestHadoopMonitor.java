package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.database.master.DataMasterContext;
import org.apache.hadoop.tools.posum.database.store.DataStoreImpl;
import org.junit.Test;

/**
 * Created by ane on 3/3/16.
 */
public class TestHadoopMonitor {

    @Test
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
