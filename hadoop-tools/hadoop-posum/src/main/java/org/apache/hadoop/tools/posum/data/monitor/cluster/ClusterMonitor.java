package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.GeneralLooper;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.data.master.DataMasterContext;


public class ClusterMonitor extends GeneralLooper<ClusterMonitor> {
  private static Log logger = LogFactory.getLog(ClusterMonitor.class);

  private final DataMasterContext context;

  public ClusterMonitor(DataMasterContext context) {
    super(ClusterMonitor.class);
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
