package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.RestClient;

/**
 * Created by ane on 3/7/16.
 */
public class POSUMAPIClient {

    private static Log logger = LogFactory.getLog(POSUMAPIClient.class);

    private final RestClient restClient;
    private final Configuration conf;

    public POSUMAPIClient(Configuration conf) {
        restClient = new RestClient();
        this.conf = conf;
    }

}
