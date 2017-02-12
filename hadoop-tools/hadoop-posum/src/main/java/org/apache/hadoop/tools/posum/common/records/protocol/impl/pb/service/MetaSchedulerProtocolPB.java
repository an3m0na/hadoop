package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import org.apache.hadoop.ipc.ProtocolInfo;


@ProtocolInfo(protocolName = "org.apache.hadoop.tools.posum.MetaSchedulerProtocolPB",
  protocolVersion = 1)
public interface MetaSchedulerProtocolPB extends
  org.apache.hadoop.yarn.proto.MetaSchedulerProtocol.MetaSchedulerProtocolService.BlockingInterface {

}