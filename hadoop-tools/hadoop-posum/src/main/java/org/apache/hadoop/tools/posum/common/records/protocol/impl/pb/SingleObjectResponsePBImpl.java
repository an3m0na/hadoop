package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleObjectResponse;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleObjectResponseProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleObjectResponseProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class SingleObjectResponsePBImpl extends SingleObjectResponse {
    SingleObjectResponseProto proto = SingleObjectResponseProto.getDefaultInstance();
    SingleObjectResponseProto.Builder builder = null;
    boolean viaProto = false;

    String response;

    public SingleObjectResponsePBImpl() {
        builder = SingleObjectResponseProto.newBuilder();
    }

    public SingleObjectResponsePBImpl(SingleObjectResponseProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SingleObjectResponseProto getProto() {
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
            builder = SingleObjectResponseProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Object getResponse() {
        SingleObjectResponseProtoOrBuilder p = viaProto ? proto : builder;
        return p.getResponse();
    }

    @Override
    public void setResponse(String response) {
        maybeInitBuilder();
        builder.setResponse(response);
    }
}
