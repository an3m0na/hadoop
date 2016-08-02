package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.StringListPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.POSUMProtos.StringListPayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.StringListPayloadProtoOrBuilder;

import java.util.*;

/**
 * Created by ane on 3/20/16.
 */
public class StringListPayloadPBImpl extends StringListPayload implements PayloadPB {
    private StringListPayloadProto proto = StringListPayloadProto.getDefaultInstance();
    private StringListPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    public StringListPayloadPBImpl() {
        builder = StringListPayloadProto.newBuilder();
    }

    public StringListPayloadPBImpl(StringListPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public StringListPayloadProto getProto() {
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
            builder = StringListPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public void addEntry(String value) {
        getEntries().add(value);
    }

    @Override
    public List<String> getEntries() {
        StringListPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getEntriesList();
    }

    @Override
    public void setEntries(List<String> entries) {
        maybeInitBuilder();
        builder.clearEntries();
        builder.addAllEntries(entries);
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = StringListPayloadProto.parseFrom(data);
        viaProto = true;
    }

}