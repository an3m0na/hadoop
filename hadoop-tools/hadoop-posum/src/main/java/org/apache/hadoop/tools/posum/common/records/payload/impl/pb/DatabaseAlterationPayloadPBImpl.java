package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DataEntityDBPBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.GeneralDataEntityPBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.DatabaseAlterationPayload;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseAlterationPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseAlterationPayloadProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class DatabaseAlterationPayloadPBImpl extends DatabaseAlterationPayload implements PayloadPB {
    private DatabaseAlterationPayloadProto proto = DatabaseAlterationPayloadProto.getDefaultInstance();
    private DatabaseAlterationPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    public DatabaseAlterationPayloadPBImpl() {
        builder = DatabaseAlterationPayloadProto.newBuilder();
    }

    public DatabaseAlterationPayloadPBImpl(DatabaseAlterationPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public DatabaseAlterationPayloadProto getProto() {
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
            builder = DatabaseAlterationPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityDB getSourceDB() {
        DatabaseAlterationPayloadProtoOrBuilder p = viaProto ? proto : builder;
        if(!p.hasSource())
            return null;
        return new DataEntityDBPBImpl(p.getSource());
    }

    @Override
    public void setSourceDB(DataEntityDB db) {
        maybeInitBuilder();
        if (db == null) {
            builder.clearSource();
            return;
        }
        builder.setSource(((DataEntityDBPBImpl) db).getProto());
    }

    @Override
    public DataEntityDB getDestinationDB() {
        DatabaseAlterationPayloadProtoOrBuilder p = viaProto ? proto : builder;
        if(!p.hasDestination())
            return null;
        return new DataEntityDBPBImpl(p.getDestination());
    }

    @Override
    public void setDestinationDB(DataEntityDB db) {
        maybeInitBuilder();
        if (db == null) {
            builder.clearDestination();
            return;
        }
        builder.setDestination(((DataEntityDBPBImpl) db).getProto());
    }


    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = DatabaseAlterationPayloadProto.parseFrom(data);
        viaProto = true;
    }
}
