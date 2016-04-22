package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import org.apache.hadoop.ipc.ProtocolInfo;

/**
 * Created by ane on 3/20/16.
 */

@ProtocolInfo(protocolName = "org.apache.hadoop.tools.posum.SimulatorProtocolPB",
        protocolVersion = 1)
public interface SimulatorProtocolPB extends
        org.apache.hadoop.yarn.proto.SimulatorProtocol.SimulatorProtocolService.BlockingInterface {

}