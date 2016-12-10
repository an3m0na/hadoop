package org.apache.hadoop.tools.posum.data.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.GeneralLooper;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.data.master.DataMasterContext;

public class PosumMonitor extends GeneralLooper<PosumMonitor> {


    private final DataMasterContext context;

    public PosumMonitor(DataMasterContext context) {
        super(PosumMonitor.class);
        this.context = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        setSleepInterval(conf.getLong(PosumConfiguration.POSUM_MONITOR_HEARTBEAT_MS,
                PosumConfiguration.POSUM_MONITOR_HEARTBEAT_MS_DEFAULT));

    }

    @Override
    protected void doAction() {
        RestClient.TrackingUI.checkUpdated(context.getCommService().getSystemAddresses());
        context.getPosumInfo().refresh();
    }

}
