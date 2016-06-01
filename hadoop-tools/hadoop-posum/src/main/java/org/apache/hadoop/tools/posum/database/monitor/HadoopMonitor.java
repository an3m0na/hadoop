package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.GeneralLooper;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.database.master.DataMasterContext;


/**
 * Created by ane on 2/4/16.
 */
public class HadoopMonitor extends GeneralLooper<HadoopMonitor> {
    private static Log logger = LogFactory.getLog(HadoopMonitor.class);

    private ClusterInfoCollector collector;
    private final DataMasterContext context;

    public HadoopMonitor(DataMasterContext context) {
        super(HadoopMonitor.class);
        this.context = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        setSleepInterval(conf.getLong(POSUMConfiguration.CLUSTER_MONITOR_HEARTBEAT_MS,
                POSUMConfiguration.CLUSTER_MONITOR_HEARTBEAT_MS_DEFAULT));
        this.collector = new ClusterInfoCollector(conf, context.getDataStore());
    }

    @Override
    protected void doAction() {
        logger.debug("Monitoring Hadoop...");
        if (!RestClient.TrackingUI.isUpdated()) {
            RestClient.TrackingUI.tryUpdate(context.getCommService().getSystemAddresses());
        }
        collector.collect();
    }
}
