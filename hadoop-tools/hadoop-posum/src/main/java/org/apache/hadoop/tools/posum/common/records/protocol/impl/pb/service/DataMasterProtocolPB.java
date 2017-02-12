package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import org.apache.hadoop.ipc.ProtocolInfo;


@ProtocolInfo(protocolName = "org.apache.hadoop.tools.posum.DataMasterProtocolPB",
  protocolVersion = 1)
public interface DataMasterProtocolPB extends
  org.apache.hadoop.yarn.proto.DataMasterProtocol.DataMasterProtocolService.BlockingInterface {

}