package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.GeneralLooper;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.master.DataMasterContext;


/**
 * Created by ane on 2/4/16.
 */
public class HadoopMonitor extends GeneralLooper<HadoopMonitor> {


    private ClusterInfoCollector collector;
    private final DataMasterContext context;

    public HadoopMonitor(DataMasterContext context) {
        super(HadoopMonitor.class);
        this.context = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        setSleepInterval(conf.getLong(POSUMConfiguration.MONITOR_HEARTBEAT_MS,
                POSUMConfiguration.MONITOR_HEARTBEAT_MS_DEFAULT));
        this.collector = new ClusterInfoCollector(conf, context.getDataStore());
    }

    @Override
    protected void doAction() {
        collector.collect();
    }
}
