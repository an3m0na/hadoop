package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.protocol.HandleEventResponse;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleEventResponseProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleEventResponseProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class HandleEventResponsePBImpl extends HandleEventResponse {
    private HandleEventResponseProto proto = HandleEventResponseProto.getDefaultInstance();
    private HandleEventResponseProto.Builder builder = null;
    private boolean viaProto = false;

    public HandleEventResponsePBImpl() {
        builder = HandleEventResponseProto.newBuilder();
    }

    public HandleEventResponsePBImpl(HandleEventResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public HandleEventResponseProto getProto() {
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
            builder = HandleEventResponseProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getText() {
        HandleEventResponseProtoOrBuilder p = viaProto ? proto : builder;
        return p.getText();
    }

    @Override
    public void setText(String text) {
        maybeInitBuilder();
        builder.setText(text);
    }

    @Override
    public String getDetails() {
        HandleEventResponseProtoOrBuilder p = viaProto ? proto : builder;
        return p.getDetails();
    }

    @Override
    public void setDetails(String details) {
        maybeInitBuilder();
        builder.setDetails(details);
    }

    @Override
    public boolean getSuccessful() {
        HandleEventResponseProtoOrBuilder p = viaProto ? proto : builder;
        return p.getSuccessful();
    }

    @Override
    public void setSuccessful(boolean successful) {
        maybeInitBuilder();
        builder.setSuccessful(successful);
    }
}
