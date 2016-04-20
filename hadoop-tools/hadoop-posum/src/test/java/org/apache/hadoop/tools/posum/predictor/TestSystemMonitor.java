package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.database.master.DataMasterContext;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;
import org.apache.hadoop.tools.posum.database.store.DataStoreImpl;
import org.apache.hadoop.tools.posum.database.monitor.HadoopMonitor;
import org.junit.Test;

/**
 * Created by ane on 3/3/16.
 */
public class TestSystemMonitor {

    @Test
    public void checkDatabaseFeeding() {
        Configuration conf = TestUtils.getConf();
        DataStoreInterface dataStoreInterface = new DataStoreImpl(conf);
        DataMasterContext context = new DataMasterContext();
        context.setDataStoreInterface(dataStoreInterface);
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
