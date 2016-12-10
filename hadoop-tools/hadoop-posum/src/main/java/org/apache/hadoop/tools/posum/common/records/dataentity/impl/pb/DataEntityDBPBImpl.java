package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.yarn.proto.PosumProtos.EntityDBProto;
import org.apache.hadoop.yarn.proto.PosumProtos.EntityDBProtoOrBuilder;

public class DataEntityDBPBImpl extends DataEntityDB {

    private EntityDBProto proto = EntityDBProto.getDefaultInstance();
    private EntityDBProto.Builder builder = null;
    private boolean viaProto = false;


    public DataEntityDBPBImpl() {
        builder = EntityDBProto.newBuilder();
    }

    public DataEntityDBPBImpl(EntityDBProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public EntityDBProto getProto() {
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
            builder = EntityDBProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityDB.Type getType() {
        EntityDBProtoOrBuilder p = viaProto ? proto : builder;
        return DataEntityDB.Type.valueOf(p.getType().name().substring("DB_".length()));
    }

    @Override
    public void setType(DataEntityDB.Type type) {
        maybeInitBuilder();
        builder.setType(EntityDBProto.EntityDBTypeProto.valueOf("DB_" + type.name()));
    }

    @Override
    public String getView() {
        EntityDBProtoOrBuilder p = viaProto ? proto : builder;
        return p.hasView()? p.getView() : null;
    }

    @Override
    public void setView(String view) {
        maybeInitBuilder();
        if (view != null)
            builder.setView(view);
    }
}
