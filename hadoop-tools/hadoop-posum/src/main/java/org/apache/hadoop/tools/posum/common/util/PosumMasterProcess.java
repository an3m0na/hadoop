package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.service.Service;

/**
 * Created by ane on 8/2/16.
 */
public interface PosumMasterProcess extends Service {
    String getConnectAddress();
}
