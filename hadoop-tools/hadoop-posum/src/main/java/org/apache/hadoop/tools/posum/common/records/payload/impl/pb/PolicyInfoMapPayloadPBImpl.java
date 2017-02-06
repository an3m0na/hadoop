package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.PolicyInfoMapPayload;
import org.apache.hadoop.tools.posum.common.records.payload.PolicyInfoPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.PolicyInfoPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.PolicyInfoMapPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.PolicyInfoMapPayloadProtoOrBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PolicyInfoMapPayloadPBImpl extends PolicyInfoMapPayload implements PayloadPB {
    private PolicyInfoMapPayloadProto proto = PolicyInfoMapPayloadProto.getDefaultInstance();
    private PolicyInfoMapPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    private Map<String, PolicyInfoPayload> entries;

    public PolicyInfoMapPayloadPBImpl() {
        builder = PolicyInfoMapPayloadProto.newBuilder();
    }

    public PolicyInfoMapPayloadPBImpl(PolicyInfoMapPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    @JsonIgnore
    public PolicyInfoMapPayloadProto getProto() {
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
        builder.clearEntries();
        if (entries == null)
            return;
        Iterable<PolicyInfoPayloadProto> iterable =
                new Iterable<PolicyInfoPayloadProto>() {

                    @Override
                    public Iterator<PolicyInfoPayloadProto> iterator() {
                        return new Iterator<PolicyInfoPayloadProto>() {

                            Iterator<Map.Entry<String, PolicyInfoPayload>> entryIterator = entries.entrySet().iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public PolicyInfoPayloadProto next() {
                                Map.Entry<String, PolicyInfoPayload> entry = entryIterator.next();
                                PolicyInfoPayloadPBImpl ret = ((PolicyInfoPayloadPBImpl) entry.getValue());
                                ret.setPolicyName(entry.getKey());
                                return ret.getProto();
                            }

                            @Override
                            public boolean hasNext() {
                                return entryIterator.hasNext();
                            }
                        };
                    }
                };
        builder.addAllEntries(iterable);
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
            builder = PolicyInfoMapPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Map<String, PolicyInfoPayload> getEntries() {
        if (entries == null) {
            PolicyInfoMapPayloadProtoOrBuilder p = viaProto ? proto : builder;
            entries = new HashMap<>(p.getEntriesCount());
            for (PolicyInfoPayloadProto entryProto : p.getEntriesList()) {
                if (entryProto != null && entryProto.hasPolicyName()) {
                    entries.put(entryProto.getPolicyName(), new PolicyInfoPayloadPBImpl(entryProto));
                }
            }
        }
        return entries;
    }

    @Override
    public void setEntries(Map<String, PolicyInfoPayload> entries) {
        maybeInitBuilder();
        this.entries = entries;
    }

    @Override
    @JsonIgnore
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = PolicyInfoMapPayloadProto.parseFrom(data);
        viaProto = true;
    }

}