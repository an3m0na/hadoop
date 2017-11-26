package org.apache.hadoop.tools.posum.common.records.request;

import org.apache.hadoop.tools.posum.common.util.communication.CommUtils;
import org.apache.hadoop.yarn.util.Records;

public abstract class RegistrationRequest {
  public static RegistrationRequest newInstance(CommUtils.PosumProcess process, String connectAddress) {
    RegistrationRequest payload = Records.newRecord(RegistrationRequest.class);
    payload.setProcess(process);
    payload.setConnectAddress(connectAddress);
    return payload;
  }

  public abstract String getConnectAddress();

  public abstract void setConnectAddress(String connectAddress);

  public abstract CommUtils.PosumProcess getProcess();

  public abstract void setProcess(CommUtils.PosumProcess process);


}
