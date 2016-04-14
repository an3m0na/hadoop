package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import org.apache.hadoop.ipc.ProtocolInfo;

/**
 * Created by ane on 3/20/16.
 */

@ProtocolInfo(protocolName = "org.apache.hadoop.tools.posum.POSUMMasterProtocolPB",
        protocolVersion = 1)
public interface POSUMMasterProtocolPB extends
        org.apache.hadoop.yarn.proto.POSUMMasterProtocol.POSUMMasterProtocolService.BlockingInterface {

}