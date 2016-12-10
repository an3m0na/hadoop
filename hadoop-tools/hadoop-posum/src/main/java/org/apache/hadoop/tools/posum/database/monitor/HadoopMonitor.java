package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.GeneralLooper;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.database.master.DataMasterContext;


public class HadoopMonitor extends GeneralLooper<HadoopMonitor> {
    private static Log logger = LogFactory.getLog(HadoopMonitor.class);

    private final DataMasterContext context;

    public HadoopMonitor(DataMasterContext context) {
        super(HadoopMonitor.class);
        this.context = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        setSleepInterval(conf.getLong(PosumConfiguration.CLUSTER_MONITOR_HEARTBEAT_MS,
                PosumConfiguration.CLUSTER_MONITOR_HEARTBEAT_MS_DEFAULT));
    }

    @Override
    protected void doAction() {
        RestClient.TrackingUI.checkUpdated(context.getCommService().getSystemAddresses());
        context.getClusterInfo().refresh();
    }
}
