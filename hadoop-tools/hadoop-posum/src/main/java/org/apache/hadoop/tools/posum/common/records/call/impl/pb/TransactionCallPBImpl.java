package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.ThreePhaseDatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.TransactionCallProto;
import org.apache.hadoop.yarn.proto.PosumProtos.TransactionCallProtoOrBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TransactionCallPBImpl extends TransactionCall implements PayloadPB {
  private TransactionCallProto proto = TransactionCallProto.getDefaultInstance();
  private TransactionCallProto.Builder builder = null;
  private boolean viaProto = false;

  private List<ThreePhaseDatabaseCall> calls;

  public TransactionCallPBImpl() {
    builder = TransactionCallProto.newBuilder();
  }

  public TransactionCallPBImpl(TransactionCallProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public TransactionCallProto getProto() {
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
    builder.clearCalls();
    if (calls == null)
      return;
    final Iterable<PosumProtos.DatabaseCallProto> iterable =
      new Iterable<PosumProtos.DatabaseCallProto>() {

        @Override
        public Iterator<PosumProtos.DatabaseCallProto> iterator() {
          return new Iterator<PosumProtos.DatabaseCallProto>() {

            Iterator<ThreePhaseDatabaseCall> iterator = calls.iterator();

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public PosumProtos.DatabaseCallProto next() {
              return new DatabaseCallWrapperPBImpl(iterator.next()).getProto();
            }

            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }
          };
        }
      };
    builder.addAllCalls(iterable);
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
      builder = TransactionCallProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<ThreePhaseDatabaseCall> getCallList() {
    if (this.calls == null) {
      TransactionCallProtoOrBuilder p = viaProto ? proto : builder;
      this.calls = new ArrayList<>(p.getCallsCount());
      for (PosumProtos.DatabaseCallProto callProto : p.getCallsList()) {
        if (callProto != null) {
          calls.add((ThreePhaseDatabaseCall) new DatabaseCallWrapperPBImpl(callProto).getCall());
        }
      }
    }
    return this.calls;
  }

  @Override
  public void setCallList(List<? extends ThreePhaseDatabaseCall> callList) {
    maybeInitBuilder();
    this.calls = callList == null ? null : new ArrayList<>(callList);
  }

  @Override
  public TransactionCall addCall(ThreePhaseDatabaseCall call) {
    maybeInitBuilder();
    getCallList().add(call);
    return this;
  }

  @Override
  public TransactionCall addAllCalls(List<? extends ThreePhaseDatabaseCall> calls) {
    maybeInitBuilder();
    getCallList().addAll(calls);
    return this;
  }

  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    this.proto = TransactionCallProto.parseFrom(data);
    viaProto = true;
  }
}