package org.apache.hadoop.tools.posum.common.records.request;

import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.util.Records;

public abstract class RegistrationRequest {
  public static RegistrationRequest newInstance(Utils.PosumProcess process, String connectAddress) {
    RegistrationRequest payload = Records.newRecord(RegistrationRequest.class);
    payload.setProcess(process);
    payload.setConnectAddress(connectAddress);
    return payload;
  }

  public abstract String getConnectAddress();

  public abstract void setConnectAddress(String connectAddress);

  public abstract Utils.PosumProcess getProcess();

  public abstract void setProcess(Utils.PosumProcess process);


}
