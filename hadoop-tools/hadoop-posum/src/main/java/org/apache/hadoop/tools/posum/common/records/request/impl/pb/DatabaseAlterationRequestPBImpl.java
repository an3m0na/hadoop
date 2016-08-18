package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.payload.DatabaseAlterationPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.DatabaseAlterationPayloadPBImpl;
import org.apache.hadoop.yarn.proto.PosumProtos;

/**
 * Created by ane on 3/20/16.
 */
public class DatabaseAlterationRequestPBImpl extends SimpleRequestPBImpl<DatabaseAlterationPayload> {

    public DatabaseAlterationRequestPBImpl() {
        super();
    }

    public DatabaseAlterationRequestPBImpl(PosumProtos.SimpleRequestProto proto) {
        super(proto);
    }

    @Override
    public ByteString payloadToBytes(DatabaseAlterationPayload payload) {
        return ((DatabaseAlterationPayloadPBImpl) payload).getProto().toByteString();
    }

    @Override
    public DatabaseAlterationPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
        DatabaseAlterationPayloadPBImpl ret = new DatabaseAlterationPayloadPBImpl();
        ret.populateFromProtoBytes(data);
        return ret;
    }
}
