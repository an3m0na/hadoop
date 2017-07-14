package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SimulationResultPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.HandleSimResultRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.HandleSimResultRequestProtoOrBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HandleSimResultRequestPBImpl extends HandleSimResultRequest {

  private HandleSimResultRequestProto proto = HandleSimResultRequestProto.getDefaultInstance();
  private HandleSimResultRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private List<SimulationResultPayload> results;

  public HandleSimResultRequestPBImpl() {
    builder = HandleSimResultRequestProto.newBuilder();
  }

  public HandleSimResultRequestPBImpl(HandleSimResultRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public HandleSimResultRequestProto getProto() {
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
    builder.clearResults();
    if (results == null)
      return;
    final Iterable<PosumProtos.SimulationResultPayloadProto> iterable =
      new Iterable<PosumProtos.SimulationResultPayloadProto>() {

        @Override
        public Iterator<PosumProtos.SimulationResultPayloadProto> iterator() {
          return new Iterator<PosumProtos.SimulationResultPayloadProto>() {

            Iterator<SimulationResultPayload> iterator = results.iterator();

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public PosumProtos.SimulationResultPayloadProto next() {
              SimulationResultPayload result = iterator.next();
              return ((SimulationResultPayloadPBImpl) result).getProto();
            }

            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }
          };
        }
      };
    builder.addAllResults(iterable);
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
      builder = HandleSimResultRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<SimulationResultPayload> getResults() {
    if (this.results == null) {
      HandleSimResultRequestProtoOrBuilder p = viaProto ? proto : builder;
      this.results = new ArrayList<>(p.getResultsCount());
      for (PosumProtos.SimulationResultPayloadProto simProto : p.getResultsList()) {
        SimulationResultPayload result = new SimulationResultPayloadPBImpl(simProto);
        results.add(result);
      }
    }
    return this.results;
  }

  @Override
  public void addResult(SimulationResultPayload result) {
    getResults().add(result);
  }

  @Override
  public void setResults(List<SimulationResultPayload> results) {
    if (results == null)
      return;
    this.results = new ArrayList<>(results);
  }
}
