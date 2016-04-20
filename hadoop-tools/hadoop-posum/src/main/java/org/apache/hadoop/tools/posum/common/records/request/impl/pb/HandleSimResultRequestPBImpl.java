package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.field.SimulationResult;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.SimulationResultPBImpl;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleSimResultRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleSimResultRequestProtoOrBuilder;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by ane on 3/20/16.
 */
public class HandleSimResultRequestPBImpl extends HandleSimResultRequest {

    private HandleSimResultRequestProto proto = HandleSimResultRequestProto.getDefaultInstance();
    private HandleSimResultRequestProto.Builder builder = null;
    private boolean viaProto = false;

    ConcurrentSkipListSet<SimulationResult> results;

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
        final Iterable<POSUMProtos.SimulationResultProto> iterable =
                new Iterable<POSUMProtos.SimulationResultProto>() {

                    @Override
                    public Iterator<POSUMProtos.SimulationResultProto> iterator() {
                        return new Iterator<POSUMProtos.SimulationResultProto>() {

                            Iterator<SimulationResult> iterator = results.iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public POSUMProtos.SimulationResultProto next() {
                                SimulationResult result = iterator.next();
                                return ((SimulationResultPBImpl) result).getProto();
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
    public ConcurrentSkipListSet<SimulationResult> getResults() {
        if (this.results == null) {
            HandleSimResultRequestProtoOrBuilder p = viaProto ? proto : builder;
            this.results = new ConcurrentSkipListSet<>();
            for (POSUMProtos.SimulationResultProto simProto : p.getResultsList()) {
                SimulationResult result = new SimulationResultPBImpl(simProto);
                results.add(result);
            }
        }
        return this.results;
    }

    @Override
    public void addResult(SimulationResult result) {
        if (this.results == null) {
            HandleSimResultRequestProtoOrBuilder p = viaProto ? proto : builder;
            this.results = new ConcurrentSkipListSet<>();
            for (POSUMProtos.SimulationResultProto simProto : p.getResultsList()) {
                SimulationResult simResult = new SimulationResultPBImpl(simProto);
                results.add(simResult);
            }
        }
        results.add(result);
    }

    @Override
    public void setResults(List<SimulationResult> results) {
        if (results == null)
            return;
        this.results = new ConcurrentSkipListSet<>();
        this.results.addAll(results);
    }
}
