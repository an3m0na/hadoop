package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DataEntityDBPBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.POSUMProtos.JobForAppCallProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.JobForAppCallProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class JobForAppCallPBImpl extends JobForAppCall implements PayloadPB {
    private JobForAppCallProto proto = JobForAppCallProto.getDefaultInstance();
    private JobForAppCallProto.Builder builder = null;
    private boolean viaProto = false;

    public JobForAppCallPBImpl() {
        builder = JobForAppCallProto.newBuilder();
    }

    public JobForAppCallPBImpl(JobForAppCallProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public JobForAppCallProto getProto() {
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
            builder = JobForAppCallProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getAppId() {
        JobForAppCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAppId();
    }

    @Override
    public void setAppId(String id) {
        maybeInitBuilder();
        builder.setAppId(id);
    }

    @Override
    public String getUser() {
        JobForAppCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.getUser();
    }

    @Override
    public void setUser(String user) {
        maybeInitBuilder();
        builder.setUser(user);
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        this.proto = JobForAppCallProto.parseFrom(data);
        viaProto = true;
    }

}