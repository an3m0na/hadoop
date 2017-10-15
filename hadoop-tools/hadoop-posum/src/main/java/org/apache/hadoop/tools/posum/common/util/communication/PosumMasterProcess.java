package org.apache.hadoop.tools.posum.common.util.communication;

import org.apache.hadoop.service.Service;

public interface PosumMasterProcess extends Service {
  String getConnectAddress();
}
