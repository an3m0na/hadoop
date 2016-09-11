package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.payload.CounterGroupInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.CounterGroupInfoPayloadPBImpl;
import org.apache.hadoop.yarn.proto.PosumProtos.CounterGroupInfoPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.CountersProxyProto;
import org.apache.hadoop.yarn.proto.PosumProtos.CountersProxyProtoOrBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ane on 3/21/16.
 */
public class CountersProxyPBImpl extends GeneralDataEntityPBImpl<CountersProxy, CountersProxyProto, CountersProxyProto.Builder>
        implements CountersProxy {

    @Override
    void initBuilder() {
        builder = viaProto ? CountersProxyProto.newBuilder(proto) : CountersProxyProto.newBuilder();
    }

    private List<CounterGroupInfoPayload> counterGroup;

    public CountersProxyPBImpl() {
    }

    public CountersProxyPBImpl(CountersProxyProto proto) {
        super(proto);
    }

    @Override
    void buildProto() {
        maybeInitBuilder();
        if(counterGroup != null) {
            Iterable<CounterGroupInfoPayloadProto> iterable =
                    new Iterable<CounterGroupInfoPayloadProto>() {
                        @Override
                        public Iterator<CounterGroupInfoPayloadProto> iterator() {
                            return new Iterator<CounterGroupInfoPayloadProto>() {

                                Iterator<CounterGroupInfoPayload> entryIterator = counterGroup.iterator();

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public CounterGroupInfoPayloadProto next() {
                                    return ((CounterGroupInfoPayloadPBImpl) entryIterator.next()).getProto();
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
        proto = builder.build();
    }

    @Override
    public CountersProxy parseToEntity(ByteString data) throws InvalidProtocolBufferException {
        this.proto = CountersProxyProto.parseFrom(data);
        viaProto = true;
        return this;
    }

    @Override
    public String getId() {
        CountersProxyProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasId())
            return null;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        if (id == null) {
            builder.clearId();
            return;
        }
        builder.setId(id);
    }

    @Override
    public CountersProxy copy() {
        return new CountersProxyPBImpl(getProto());
    }

    @Override
    public List<CounterGroupInfoPayload> getCounterGroup() {
        if (counterGroup == null) {
            CountersProxyProtoOrBuilder p = viaProto ? proto : builder;
            counterGroup = new ArrayList<>(p.getGroupsCount());
            for (CounterGroupInfoPayloadProto counterProto : p.getGroupsList()) {
                counterGroup.add(new CounterGroupInfoPayloadPBImpl(counterProto));
            }
        }
        return counterGroup;
    }

    @Override
    public void setCounterGroup(List<CounterGroupInfoPayload> counterGroups) {
        this.counterGroup = counterGroups;
    }

    @Override
    public void setTaskCounterGroup(List<CounterGroupInfoPayload> counterGroups) {
        this.counterGroup = counterGroups;
    }
}
