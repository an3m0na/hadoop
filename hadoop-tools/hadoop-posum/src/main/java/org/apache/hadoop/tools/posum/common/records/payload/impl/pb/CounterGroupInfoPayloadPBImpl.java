package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.CounterGroupInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.CounterInfoPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos.CounterGroupInfoPayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.CounterInfoPayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.CounterGroupInfoPayloadProtoOrBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by ane on 3/20/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class CounterGroupInfoPayloadPBImpl extends CounterGroupInfoPayload {
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

    @JsonIgnore
    @org.codehaus.jackson.annotate.JsonIgnore
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

}