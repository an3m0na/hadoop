package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.PropertyMapPayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.PropertyMapPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SimplePropertyPayloadProto;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PropertyMapPayloadPBImpl extends PropertyMapPayload implements PayloadPB {

  private PropertyMapPayloadProto proto = PropertyMapPayloadProto.getDefaultInstance();
  private PropertyMapPayloadProto.Builder builder = null;
  private boolean viaProto = false;

  private Map<String, Object> entries;

  public PropertyMapPayloadPBImpl() {
    builder = PropertyMapPayloadProto.newBuilder();
  }

  public PropertyMapPayloadPBImpl(PropertyMapPayloadProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public PropertyMapPayloadProto getProto() {
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
    if (entries == null)
      return;
    builder.clearEntries();
    Iterable<SimplePropertyPayloadProto> iterable =
      new Iterable<SimplePropertyPayloadProto>() {

        @Override
        public Iterator<SimplePropertyPayloadProto> iterator() {
          return new Iterator<SimplePropertyPayloadProto>() {

            Iterator<Map.Entry<String, Object>> entryIterator = entries.entrySet().iterator();

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public SimplePropertyPayloadProto next() {
              Map.Entry<String, Object> mapEntry = entryIterator.next();
              SimplePropertyPayload property = SimplePropertyPayload.newInstance(mapEntry.getKey(), mapEntry.getValue());
              return ((SimplePropertyPayloadPBImpl) property).getProto();
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
      builder = PropertyMapPayloadProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void addEntry(String key, Object value) {
    maybeInitBuilder();
    getEntries().put(key, value);
  }

  @Override
  public Map<String, Object> getEntries() {
    if (entries == null) {
      PosumProtos.PropertyMapPayloadProtoOrBuilder p = viaProto ? proto : builder;
      entries = new HashMap<>(p.getEntriesCount());
      for (SimplePropertyPayloadProto entryProto : p.getEntriesList()) {
        if (entryProto != null && entryProto.hasValue()) {
          entries.put(entryProto.getName(), entryProto.getValue());
        }
      }
    }
    return entries;
  }

  @Override
  public void setEntries(Map<String, Object> entries) {
    maybeInitBuilder();
    if (entries == null) {
      builder.clearEntries();
    }
    this.entries = entries;
  }

  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = PropertyMapPayloadProto.parseFrom(data);
    viaProto = true;
  }
}
