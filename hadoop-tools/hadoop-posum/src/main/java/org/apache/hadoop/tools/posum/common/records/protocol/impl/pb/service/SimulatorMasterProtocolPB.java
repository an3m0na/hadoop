package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import org.apache.hadoop.ipc.ProtocolInfo;


@ProtocolInfo(protocolName = "org.apache.hadoop.tools.posum.SimulatorMasterProtocolPB",
  protocolVersion = 1)
public interface SimulatorMasterProtocolPB extends
  org.apache.hadoop.yarn.proto.SimulatorMasterProtocol.SimulatorMasterProtocolService.BlockingInterface {

}