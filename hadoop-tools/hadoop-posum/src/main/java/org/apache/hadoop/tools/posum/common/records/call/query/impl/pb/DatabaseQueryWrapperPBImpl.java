package org.apache.hadoop.tools.posum.common.records.call.query.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQueryType;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseQueryProto;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseQueryProtoOrBuilder;

public class DatabaseQueryWrapperPBImpl implements PayloadPB {
    private DatabaseQueryProto proto = DatabaseQueryProto.getDefaultInstance();
    private DatabaseQueryProto.Builder builder = null;
    private boolean viaProto = false;

    private DatabaseQuery query;

    public DatabaseQueryWrapperPBImpl() {
        builder = DatabaseQueryProto.newBuilder();
    }

    public DatabaseQueryWrapperPBImpl(DatabaseQuery query) {
        this();
        setQuery(query);
    }


    public DatabaseQueryWrapperPBImpl(DatabaseQueryProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public DatabaseQueryProto getProto() {
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
        if (query != null) {
            DatabaseQueryType type = DatabaseQueryType.fromMappedClass(query.getClass());
            if (type == null)
                throw new PosumException("DatabaseCall class not recognized " + query.getClass());
            builder.setType(DatabaseQueryProto.TypeProto.valueOf("QRY_" + type.name()));
            PayloadPB serializableCall = (PayloadPB) query;
            builder.setQuery(serializableCall.getProtoBytes());
        }
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
            builder = DatabaseQueryProto.newBuilder(proto);
        }
        viaProto = false;
    }

    public DatabaseQuery getQuery() {
        if (query == null) {
            DatabaseQueryProtoOrBuilder p = viaProto ? proto : builder;
            if (p.hasQuery()) {
                DatabaseQueryType type = DatabaseQueryType.valueOf(p.getType().name().substring("QRY_".length()));
                Class<? extends DatabaseQuery> queryClass = type.getMappedClass();
                try {
                    query = queryClass.newInstance();
                    ((PayloadPB) query).populateFromProtoBytes(p.getQuery());
                } catch (Exception e) {
                    throw new PosumException("Could not read call from byte string " + p.getQuery() + " as " + queryClass, e);
                }
            }
        }
        return query;
    }

    public void setQuery(DatabaseQuery query) {
        maybeInitBuilder();
        this.query = query;
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = DatabaseQueryProto.parseFrom(data);
        viaProto = true;
    }
}
