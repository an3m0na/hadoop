package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.data.core.DataStoreImpl;
import org.apache.hadoop.tools.posum.data.master.DataMasterContext;
import org.apache.hadoop.tools.posum.data.monitor.cluster.ClusterMonitor;
import org.junit.Ignore;
import org.junit.Test;

public class ManualTests {
    @Test
    @Ignore
    public void checkRegistration() {
        System.out.println(NetUtils.createSocketAddr("0.0.0.0", 7000));
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
}
