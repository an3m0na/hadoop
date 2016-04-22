package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.impl.pb.SerializedExceptionPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleResponseProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleResponseProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SimpleResponsePBImpl<T> extends SimpleResponse<T> {
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
            builder.setPayload(payloadToBytes(payload));
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
    public Throwable getException() {
        SimpleResponseProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasException())
            return null;
        return new SerializedExceptionPBImpl(p.getException()).deSerialize();
    }

    @Override
    public void setException(Throwable exception) {
        maybeInitBuilder();
        if (exception != null)
            builder.setException(((SerializedExceptionPBImpl) SerializedException.newInstance(exception)).getProto());
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
    public Type getType() {
        SimpleResponseProtoOrBuilder p = viaProto ? proto : builder;
        return Type.fromProto(p.getType());
    }

    @Override
    public void setType(Type type) {
        maybeInitBuilder();
        if (type != null)
            builder.setType(type.toProto());
    }

    @Override
    public T getPayload() {
        if (this.payload == null) {
            SimpleResponseProtoOrBuilder p = viaProto ? proto : builder;
            if (p.hasPayload())
                try {
                    this.payload = bytesToPayload(p.getPayload());
                } catch (InvalidProtocolBufferException e) {
                    throw new POSUMException("Could not read message payload", e);
                }
        }
        return payload;
    }

    @Override
    public void setPayload(T payload) {
        this.payload = payload;
    }

    public abstract ByteString payloadToBytes(T payload);

    public abstract T bytesToPayload(ByteString data) throws InvalidProtocolBufferException;
}
