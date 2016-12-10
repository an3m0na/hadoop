package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.SaveJobFlexFieldsCall;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.StringStringMapPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.SaveJobFlexFieldsCallProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SaveJobFlexFieldsCallProtoOrBuilder;

import java.util.Map;

public class SaveJobFlexFieldsCallPBImpl extends SaveJobFlexFieldsCall implements PayloadPB {
    private SaveJobFlexFieldsCallProto proto = SaveJobFlexFieldsCallProto.getDefaultInstance();
    private SaveJobFlexFieldsCallProto.Builder builder = null;
    private boolean viaProto = false;

    private Map<String, String> newFields;

    public SaveJobFlexFieldsCallPBImpl() {
        builder = SaveJobFlexFieldsCallProto.newBuilder();
    }

    public SaveJobFlexFieldsCallPBImpl(SaveJobFlexFieldsCallProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SaveJobFlexFieldsCallProto getProto() {
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
        if (newFields != null) {
            StringStringMapPayloadPBImpl mapPayloadPB = new StringStringMapPayloadPBImpl();
            mapPayloadPB.setEntries(newFields);
            builder.setNewFields(mapPayloadPB.getProto());
        }
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
            builder = SaveJobFlexFieldsCallProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getJobId() {
        SaveJobFlexFieldsCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.getJobId();
    }

    @Override
    public void setJobId(String jobId) {
        maybeInitBuilder();
        builder.setJobId(jobId);
    }

    @Override
    public Map<String, String> getNewFields() {
        if (newFields == null) {
            SaveJobFlexFieldsCallProtoOrBuilder p = viaProto ? proto : builder;
            newFields = new StringStringMapPayloadPBImpl(p.getNewFields()).getEntries();
        }
        return newFields;
    }

    @Override
    public void setNewFields(Map<String, String> newFields) {
        this.newFields = newFields;
    }

    @Override
    public boolean getForHistory() {
        SaveJobFlexFieldsCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.getForHistory();
    }

    @Override
    public void setForHistory(boolean forHistory) {
        maybeInitBuilder();
        builder.setForHistory(forHistory);
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        this.proto = SaveJobFlexFieldsCallProto.parseFrom(data);
        viaProto = true;
    }

}