package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import org.apache.hadoop.ipc.ProtocolInfo;


@ProtocolInfo(protocolName = "org.apache.hadoop.tools.posum.OrchestratorMasterProtocolPB",
  protocolVersion = 1)
public interface OrchestratorMasterProtocolPB extends
  org.apache.hadoop.yarn.proto.OrchestratorMasterProtocol.OrchestratorMasterProtocolService.BlockingInterface {

}