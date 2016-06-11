package org.apache.hadoop.tools.posum.common.records.field.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DataEntityDBPBImpl;
import org.apache.hadoop.tools.posum.common.records.field.JobForAppPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos.JobForAppPayloadProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.JobForAppPayloadProtoOrBuilder;

/**
 * Created by ane on 3/20/16.
 */
public class JobForAppPayloadPBImpl extends JobForAppPayload {
    private JobForAppPayloadProto proto = JobForAppPayloadProto.getDefaultInstance();
    private JobForAppPayloadProto.Builder builder = null;
    private boolean viaProto = false;

    public JobForAppPayloadPBImpl() {
        builder = JobForAppPayloadProto.newBuilder();
    }

    public JobForAppPayloadPBImpl(JobForAppPayloadProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public JobForAppPayloadProto getProto() {
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
            builder = JobForAppPayloadProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityDB getEntityDB() {
        JobForAppPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return new DataEntityDBPBImpl(p.getEntityDB());
    }

    @Override
    public void setEntityDB(DataEntityDB db) {
        maybeInitBuilder();
        builder.setEntityDB(((DataEntityDBPBImpl) db).getProto());
    }

    @Override
    public String getUser() {
        JobForAppPayloadProtoOrBuilder p = viaProto ? proto : builder;
        if (p.hasUser())
            return p.getUser();
        return null;
    }

    @Override
    public void setUser(String user) {
        if (user == null)
            return;
        maybeInitBuilder();
        builder.setUser(user);
    }

    @Override
    public String getAppId() {
        JobForAppPayloadProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAppId();
    }

    @Override
    public void setAppId(String id) {
        maybeInitBuilder();
        builder.setAppId(id);
    }

}