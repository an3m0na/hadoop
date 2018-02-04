package org.apache.hadoop.tools.posum.data.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.communication.CommUtils.PosumProcess;
import org.apache.hadoop.tools.posum.common.util.communication.RestClient;
import org.apache.hadoop.tools.posum.common.util.communication.RestClient.TrackingUI;

public class PosumAPIClient {

  private static Log logger = LogFactory.getLog(PosumAPIClient.class);

  private final RestClient restClient;
  private final Configuration conf;

  public PosumAPIClient(Configuration conf) {
    restClient = new RestClient();
    this.conf = conf;
  }

  public String getSystemMetrics(PosumProcess process){
    return restClient.getInfo(String.class, TrackingUI.valueOf(process.name()), "system");
  }

  public String getClusterMetrics(){
    return restClient.getInfo(String.class, TrackingUI.PS, "cluster");
  }
}
