package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.CounterGroupInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.CounterInfoPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.CounterGroupInfoPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.CounterGroupInfoPayloadProtoOrBuilder;
import org.apache.hadoop.yarn.proto.PosumProtos.CounterInfoPayloadProto;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class CounterGroupInfoPayloadPBImpl extends CounterGroupInfoPayload implements PayloadPB {
  private CounterGroupInfoPayloadProto proto = CounterGroupInfoPayloadProto.getDefaultInstance();
  private CounterGroupInfoPayloadProto.Builder builder = null;
  private boolean viaProto = false;

  private List<CounterInfoPayload> counter;

  public CounterGroupInfoPayloadPBImpl() {
    builder = CounterGroupInfoPayloadProto.newBuilder();
  }

  public CounterGroupInfoPayloadPBImpl(CounterGroupInfoPayloadProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public CounterGroupInfoPayloadProto getProto() {
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
    if (this.counter != null) {
      builder.clearCounters();
      Iterable<CounterInfoPayloadProto> iterable =
        new Iterable<CounterInfoPayloadProto>() {

          @Override
          public Iterator<CounterInfoPayloadProto> iterator() {
            return new Iterator<CounterInfoPayloadProto>() {

              Iterator<CounterInfoPayload> entryIterator = counter.iterator();

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public CounterInfoPayloadProto next() {
                return ((CounterInfoPayloadPBImpl) entryIterator.next()).getProto();
              }

              @Override
              public boolean hasNext() {
                return entryIterator.hasNext();
              }
            };
          }
        };
      builder.addAllCounters(iterable);
    }
    builder.build();
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
      builder = CounterGroupInfoPayloadProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<CounterInfoPayload> getCounter() {
    if (counter == null) {
      CounterGroupInfoPayloadProtoOrBuilder p = viaProto ? proto : builder;
      counter = new ArrayList<>(p.getCountersCount());
      for (CounterInfoPayloadProto counterProto : p.getCountersList()) {
        if (counterProto != null) {
          counter.add(new CounterInfoPayloadPBImpl(counterProto));
        }
      }
    }
    return counter;
  }

  @Override
  public void setCounter(List<CounterInfoPayload> counter) {
    maybeInitBuilder();
    if(counter == null){
      builder.clearCounters();
    }
    this.counter = counter;
  }

  public String getCounterGroupName() {
    CounterGroupInfoPayloadProtoOrBuilder p = viaProto ? proto : builder;
    return p.getName();
  }

  public void setCounterGroupName(String name) {
    maybeInitBuilder();
    builder.setName(name);
  }

  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = CounterGroupInfoPayloadProto.parseFrom(data);
    viaProto = true;
  }

}