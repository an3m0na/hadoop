package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.PropertyGroupsPayload;
import org.apache.hadoop.tools.posum.common.records.payload.PropertyMapPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.PropertyGroupProto;
import org.apache.hadoop.yarn.proto.PosumProtos.PropertyGroupsPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.PropertyGroupsPayloadProtoOrBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PropertyGroupsPayloadPBImpl extends PropertyGroupsPayload implements PayloadPB {

  private PropertyGroupsPayloadProto proto = PropertyGroupsPayloadProto.getDefaultInstance();
  private PropertyGroupsPayloadProto.Builder builder = null;
  private boolean viaProto = false;

  private Map<String, Map<String, Object>> groups;

  public PropertyGroupsPayloadPBImpl() {
    builder = PropertyGroupsPayloadProto.newBuilder();
  }

  public PropertyGroupsPayloadPBImpl(PropertyGroupsPayloadProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public PropertyGroupsPayloadProto getProto() {
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
    if (groups == null)
      return;
    builder.clearGroups();
    Iterable<PropertyGroupProto> iterable =
      new Iterable<PropertyGroupProto>() {

        @Override
        public Iterator<PropertyGroupProto> iterator() {
          return new Iterator<PropertyGroupProto>() {

            Iterator<Map.Entry<String, Map<String, Object>>> entryIterator = groups.entrySet().iterator();

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public PropertyGroupProto next() {
              Map.Entry<String, Map<String, Object>> mapEntry = entryIterator.next();
              PropertyMapPayload value = PropertyMapPayload.newInstance(mapEntry.getValue());
              return PropertyGroupProto.newBuilder().setKey(mapEntry.getKey()).setValue(((PropertyMapPayloadPBImpl) value).getProto()).build();
            }

            @Override
            public boolean hasNext() {
              return entryIterator.hasNext();
            }
          };
        }
      };
    builder.addAllGroups(iterable);
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
      builder = PropertyGroupsPayloadProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void addGroup(String key, Map<String, Object> value) {
    maybeInitBuilder();
    getGroups().put(key, value);
  }

  @Override
  public Map<String, Map<String, Object>> getGroups() {
    if (groups == null) {
      PropertyGroupsPayloadProtoOrBuilder p = viaProto ? proto : builder;
      groups = new HashMap<>(p.getGroupsCount());
      for (PropertyGroupProto entryProto : p.getGroupsList()) {
        if (entryProto != null) {
          groups.put(entryProto.getKey(), new PropertyMapPayloadPBImpl(entryProto.getValue()).getEntries());
        }
      }
    }
    return groups;
  }

  @Override
  public void setGroups(Map<String, Map<String, Object>> groups) {
    maybeInitBuilder();
    if (groups == null) {
      builder.clearGroups();
    }
    this.groups = groups;
  }

  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = PropertyGroupsPayloadProto.parseFrom(data);
    viaProto = true;
  }
}
