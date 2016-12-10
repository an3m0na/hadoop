package org.apache.hadoop.tools.posum.common.records.call.query.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.query.CompositionQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.SimplePropertyPayloadPBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseQueryProto;
import org.apache.hadoop.yarn.proto.PosumProtos.CompositionQueryProto;
import org.apache.hadoop.yarn.proto.PosumProtos.CompositionQueryProtoOrBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CompositionQueryPBImpl extends CompositionQuery implements PayloadPB {
    private CompositionQueryProto proto = CompositionQueryProto.getDefaultInstance();
    private CompositionQueryProto.Builder builder = null;
    private boolean viaProto = false;

    private List<DatabaseQuery> queries;

    public CompositionQueryPBImpl() {
        builder = CompositionQueryProto.newBuilder();
    }

    public CompositionQueryPBImpl(CompositionQueryProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public CompositionQueryProto getProto() {
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
        builder.clearQueries();
        if (queries == null)
            return;
        final Iterable<DatabaseQueryProto> iterable =
                new Iterable<DatabaseQueryProto>() {
                    @Override
                    public Iterator<DatabaseQueryProto> iterator() {
                        return new Iterator<DatabaseQueryProto>() {

                            Iterator<DatabaseQuery> iterator = queries.iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public DatabaseQueryProto next() {
                                return new DatabaseQueryWrapperPBImpl(iterator.next()).getProto();
                            }

                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }
                        };
                    }
                };
        builder.addAllQueries(iterable);
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
            builder = CompositionQueryProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public Type getType() {
        CompositionQueryProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasType())
            return null;
        return Type.valueOf(p.getType().name().substring("CQRY_".length()));
    }

    @Override
    public void setType(Type type) {
        maybeInitBuilder();
        if (type == null) {
            builder.clearType();
            return;
        }
        builder.setType(CompositionQueryProto.TypeProto.valueOf("CQRY_" + type.name()));
    }

    @Override
    public List<DatabaseQuery> getQueries() {
        if (queries == null) {
            CompositionQueryProtoOrBuilder p = viaProto ? proto : builder;
            queries = new ArrayList<>(p.getQueriesCount());
            for (DatabaseQueryProto queryProto : p.getQueriesList()) {
                queries.add(new DatabaseQueryWrapperPBImpl(queryProto).getQuery());
            }
        }
        return queries;
    }

    @Override
    public void setQueries(List<DatabaseQuery> queries) {
        maybeInitBuilder();
        this.queries = queries;
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = CompositionQueryProto.parseFrom(data);
        viaProto = true;
    }
}
