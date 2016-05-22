package org.apache.hadoop.tools.posum.common.records.field.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.field.StringStringMapPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos.StringStringMapPayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.StringStringMapPayloadProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public class StringStringMapPayloadPBImpl extends StringStringMapPayload {
    private StringStringMapPayloadProto proto = StringStringMapPayloadProto.getDefaultInstance();
    private StringStringMapPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    private Map<String, String> entries;

    public StringStringMapPayloadPBImpl() {
        builder = StringStringMapPayloadProto.newBuilder();
    }

    public StringStringMapPayloadPBImpl(StringStringMapPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public StringStringMapPayloadProto getProto() {
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
        Iterable<YarnProtos.StringStringMapProto> iterable =
                new Iterable<YarnProtos.StringStringMapProto>() {

                    @Override
                    public Iterator<YarnProtos.StringStringMapProto> iterator() {
                        return new Iterator<YarnProtos.StringStringMapProto>() {

                            Iterator<Map.Entry<String, String>> entryIterator = entries.entrySet().iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public YarnProtos.StringStringMapProto next() {
                                Map.Entry<String, String> mapEntry = entryIterator.next();
                                YarnProtos.StringStringMapProto.Builder builder = YarnProtos.StringStringMapProto.newBuilder()
                                        .setKey(mapEntry.getKey());
                                if (mapEntry.getValue() != null)
                                    builder.setValue(mapEntry.getValue());
                                return builder.build();
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
            builder = StringStringMapPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Map<String, String> getEntries() {
        if (entries == null) {
            StringStringMapPayloadProtoOrBuilder p = viaProto ? proto : builder;
            entries = new HashMap<>(p.getEntriesCount());
            for (YarnProtos.StringStringMapProto entryProto : p.getEntriesList()) {
                if (entryProto != null && entryProto.hasValue()) {
                    entries.put(entryProto.getKey(), entryProto.getValue());
                }
            }
        }
        return entries;
    }

    @Override
    public void setEntries(Map<String, String> entries) {
        this.entries = entries;
    }


}