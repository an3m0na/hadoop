package org.apache.hadoop.tools.posum.common.records.call.query.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.query.PropertyRangeQuery;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SimplePropertyPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.PropertyRangeQueryProto;
import org.apache.hadoop.yarn.proto.PosumProtos.PropertyRangeQueryProtoOrBuilder;
import org.apache.hadoop.yarn.proto.PosumProtos.SimplePropertyPayloadProto;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PropertyRangeQueryPBImpl extends PropertyRangeQuery implements PayloadPB {
  private PropertyRangeQueryProto proto = PropertyRangeQueryProto.getDefaultInstance();
  private PropertyRangeQueryProto.Builder builder = null;
  private boolean viaProto = false;

  private List<Object> values;

  public PropertyRangeQueryPBImpl() {
    builder = PropertyRangeQueryProto.newBuilder();
  }

  public PropertyRangeQueryPBImpl(PropertyRangeQueryProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public PropertyRangeQueryProto getProto() {
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
    if (values == null)
      return;
    builder.clearValues();
    final Iterable<SimplePropertyPayloadProto> iterable =
      new Iterable<SimplePropertyPayloadProto>() {
        @Override
        public Iterator<SimplePropertyPayloadProto> iterator() {
          return new Iterator<SimplePropertyPayloadProto>() {

            Iterator<?> iterator = values.iterator();

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public SimplePropertyPayloadProto next() {
              SimplePropertyPayload propertyValue =
                SimplePropertyPayload.newInstance("", iterator.next());
              return ((SimplePropertyPayloadPBImpl) propertyValue).getProto();
            }

            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }
          };
        }
      };
    builder.addAllValues(iterable);
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
      builder = PropertyRangeQueryProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getPropertyName() {
    PropertyRangeQueryProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasPropertyName())
      return null;
    return p.getPropertyName();
  }

  @Override
  public void setPropertyName(String propertyName) {
    maybeInitBuilder();
    if (propertyName == null) {
      builder.clearPropertyName();
      return;
    }
    builder.setPropertyName(propertyName);
  }

  @Override
  public Type getType() {
    PropertyRangeQueryProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasType())
      return null;
    return Type.valueOf(p.getType().name().substring("RQRY_".length()));
  }

  @Override
  public void setType(Type type) {
    maybeInitBuilder();
    if (type == null) {
      builder.clearType();
      return;
    }
    builder.setType(PropertyRangeQueryProto.TypeProto.valueOf("RQRY_" + type.name()));
  }

  @Override
  public <T> List<T> getValues() {
    if (values == null) {
      PropertyRangeQueryProtoOrBuilder p = viaProto ? proto : builder;
      values = new ArrayList<>(p.getValuesCount());
      for (SimplePropertyPayloadProto propertyProto : p.getValuesList()) {
        values.add(new SimplePropertyPayloadPBImpl(propertyProto).getValue());
      }
    }
    return (List<T>) values;
  }

  @Override
  public void setValues(List<?> values) {
    maybeInitBuilder();
    if(values == null){
      builder.clearValues();
      this.values = null;
      return;
    }
    this.values = new ArrayList<>(values);
  }

  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = PropertyRangeQueryProto.parseFrom(data);
    viaProto = true;
  }
}
