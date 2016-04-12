package org.apache.hadoop.tools.posum.common.records.reponse.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueUserAclsInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.impl.pb.QueueInfoPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;

import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public abstract class QueueUserACLInfoResponsePBImpl extends SimpleResponsePBImpl<List<QueueUserACLInfo>> {
    @Override
    public ByteString payloadToBytes(List<QueueUserACLInfo> payload) {
        GetQueueUserAclsInfoResponse payloadImpl = GetQueueUserAclsInfoResponse.newInstance(payload);
        return ((GetQueueUserAclsInfoResponsePBImpl) payloadImpl).getProto().toByteString();
    }

    @Override
    public List<QueueUserACLInfo> bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        YarnServiceProtos.GetQueueUserAclsInfoResponseProto proto =
                YarnServiceProtos.GetQueueUserAclsInfoResponseProto.parseFrom(data);
        return new GetQueueUserAclsInfoResponsePBImpl(proto).getUserAclsInfoList();
    }
}
