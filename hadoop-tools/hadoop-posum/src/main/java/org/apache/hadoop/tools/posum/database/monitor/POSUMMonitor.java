package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.GeneralLooper;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.apache.hadoop.tools.posum.database.master.DataMasterContext;

import java.util.Map;

/**
 * Created by ane on 2/4/16.
 */
public class POSUMMonitor extends GeneralLooper<POSUMMonitor> {


    private POSUMInfoCollector collector;
    private final DataMasterContext context;

    public POSUMMonitor(DataMasterContext context) {
        super(POSUMMonitor.class);
        this.context = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        setSleepInterval(conf.getLong(POSUMConfiguration.POSUM_MONITOR_HEARTBEAT_MS,
                POSUMConfiguration.POSUM_MONITOR_HEARTBEAT_MS_DEFAULT));
        this.collector = new POSUMInfoCollector(conf, context.getDataStore());

    }

    @Override
    protected void doAction() {
        collector.collect();
    }

}
