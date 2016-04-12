package org.apache.hadoop.tools.posum.common.records.reponse.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.impl.pb.QueueInfoPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;

/**
 * Created by ane on 3/20/16.
 */
public abstract class QueueInfoResponsePBImpl extends SimpleResponsePBImpl<QueueInfo> {
    @Override
    public ByteString payloadToBytes(QueueInfo payload) {
        return ((QueueInfoPBImpl) payload).getProto().toByteString();
    }

    @Override
    public QueueInfo bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        YarnProtos.QueueInfoProto proto = YarnProtos.QueueInfoProto.parseFrom(data);
        return new QueueInfoPBImpl(proto);
    }
}
