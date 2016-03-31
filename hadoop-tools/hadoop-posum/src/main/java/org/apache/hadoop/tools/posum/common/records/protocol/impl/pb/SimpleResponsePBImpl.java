package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.protocol.SimpleResponse;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleResponseProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleResponseProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class SimpleResponsePBImpl extends SimpleResponse {
    private SimpleResponseProto proto = SimpleResponseProto.getDefaultInstance();
    private SimpleResponseProto.Builder builder = null;
    private boolean viaProto = false;

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
        builder.setText(text);
    }

    @Override
    public String getDetails() {
        SimpleResponseProtoOrBuilder p = viaProto ? proto : builder;
        return p.getDetails();
    }

    @Override
    public void setDetails(String details) {
        maybeInitBuilder();
        builder.setDetails(details);
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
}
