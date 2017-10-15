package org.apache.hadoop.tools.posum.data.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.communication.RestClient;

public class PosumAPIClient {

  private static Log logger = LogFactory.getLog(PosumAPIClient.class);

  private final RestClient restClient;
  private final Configuration conf;

  public PosumAPIClient(Configuration conf) {
    restClient = new RestClient();
    this.conf = conf;
  }

}
