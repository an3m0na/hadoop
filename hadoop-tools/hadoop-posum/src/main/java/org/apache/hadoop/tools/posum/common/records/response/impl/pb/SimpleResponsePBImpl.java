package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.PayloadType;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleResponseProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleResponseProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class SimpleResponsePBImpl<T extends Payload> extends SimpleResponse<T> {
    private SimpleResponseProto proto = SimpleResponseProto.getDefaultInstance();
    private SimpleResponseProto.Builder builder = null;
    private boolean viaProto = false;

    private T payload;

    public SimpleResponsePBImpl() {
        builder = SimpleResponseProto.newBuilder();
    }

    public SimpleResponsePBImpl(SimpleResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SimpleResponseProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    @Override
    public int hashCode() {
        return getProto().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other.getClass().isAssignableFrom(this.getClass())) {
            return this.getProto().equals(this.getClass().cast(other).getProto());
        }
        return false;
    }

    @Override
    public String toString() {
        return TextFormat.shortDebugString(getProto());
    }

    private void mergeLocalToBuilder() {
        maybeInitBuilder();
        if (this.payload != null)
            builder.setPayload(((PayloadPB) payload).getProtoBytes());
    }

    private void mergeLocalToProto() {
        if (viaProto)
            maybeInitBuilder();
        mergeLocalToBuilder();
        proto = builder.build();
        viaProto = true;
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = SimpleResponseProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getText() {
        SimpleResponseProtoOrBuilder p = viaProto ? proto : builder;
        return p.getText();
    }

    @Override
    public void setText(String text) {
        maybeInitBuilder();
        if (text != null)
            builder.setText(text);
    }

    @Override
    public String getException() {
        SimpleResponseProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasException())
            return null;
        return p.getException();
    }

    @Override
    public void setException(String exception) {
        maybeInitBuilder();
        if (exception != null)
            builder.setException(exception);
    }

    @Override
    public boolean getSuccessful() {
        SimpleResponseProtoOrBuilder p = viaProto ? proto : builder;
        return p.getSuccessful();
    }

    @Override
    public void setSuccessful(boolean successful) {
        maybeInitBuilder();
        builder.setSuccessful(successful);
    }

    @Override
    public PayloadType getType() {
        SimpleResponseProtoOrBuilder p = viaProto ? proto : builder;
        return PayloadType.valueOf(p.getType().name().substring("PLD_".length()));
    }

    @Override
    public void setType(PayloadType payloadType) {
        maybeInitBuilder();
        if (payloadType != null)
            builder.setType(PosumProtos.PayloadTypeProto.valueOf("PLD_" + payloadType.name()));
    }

    @Override
    public T getPayload() {
        if (payload == null) {
            SimpleResponseProtoOrBuilder p = viaProto ? proto : builder;
            if (p.hasPayload())
                try {
                    payload = (T) getType().getImplClass().newInstance();
                    ((PayloadPB) payload).populateFromProtoBytes(p.getPayload());
                    return payload;
                } catch (InstantiationException | IllegalAccessException | InvalidProtocolBufferException e) {
                    throw new PosumException("Could not read message payload", e);
                }
        }
        return payload;
    }

    @Override
    public void setPayload(T payload) {
        this.payload = payload;
    }
}
