package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import org.apache.hadoop.ipc.ProtocolInfo;

/**
 * Created by ane on 3/20/16.
 */

@ProtocolInfo(protocolName = "org.apache.hadoop.tools.posum.DataMasterProtocolPB",
        protocolVersion = 1)
public interface DataMasterProtocolPB extends
        org.apache.hadoop.yarn.proto.DataMasterProtocol.DataMasterProtocolService.BlockingInterface {

}